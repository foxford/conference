use std::result::Result as StdResult;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use svc_agent::mqtt::{
    IncomingEvent, IncomingEventProperties, IncomingRequest, IncomingRequestProperties,
    IncomingResponse, IncomingResponseProperties,
};

use crate::app::context::Context;
use crate::app::error::Error as AppError;
pub(self) use crate::app::message_handler::MessageStream;
use crate::app::message_handler::{
    EventEnvelopeHandler, RequestEnvelopeHandler, ResponseEnvelopeHandler,
};
use crate::backend::janus;

///////////////////////////////////////////////////////////////////////////////

pub(crate) type Result = StdResult<MessageStream, AppError>;

#[async_trait]
pub(crate) trait RequestHandler {
    type Payload: Send + DeserializeOwned;
    const ERROR_TITLE: &'static str;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result;
}

macro_rules! request_routes {
    ($($m: pat => $h: ty),*) => {
        pub(crate) async fn route_request<C: Context>(
            context: &mut C,
            request: &IncomingRequest<String>,
            _topic: &str,
        ) -> Option<MessageStream> {
            match request.properties().method() {
                $(
                    $m => Some(<$h>::handle_envelope::<C>(context, request).await),
                )*
                _ => None,
            }
        }
    }
}

// Request routes configuration: method => RequestHandler
request_routes!(
    "agent.list" => agent::ListHandler,
    "agent_reader_config.read" => agent_reader_config::ReadHandler,
    "agent_reader_config.update" => agent_reader_config::UpdateHandler,
    "agent_writer_config.read" => agent_writer_config::ReadHandler,
    "agent_writer_config.update" => agent_writer_config::UpdateHandler,
    "message.broadcast" => message::BroadcastHandler,
    "message.unicast" => message::UnicastHandler,
    "room.create" => room::CreateHandler,
    "room.delete" => room::DeleteHandler,
    "room.enter" => room::EnterHandler,
    "room.leave" => room::LeaveHandler,
    "room.read" => room::ReadHandler,
    "room.update" => room::UpdateHandler,
    "rtc.connect" => rtc::ConnectHandler,
    "rtc.create" => rtc::CreateHandler,
    "rtc.list" => rtc::ListHandler,
    "rtc.read" => rtc::ReadHandler,
    "rtc_signal.create" => rtc_signal::CreateHandler,
    "rtc_stream.list" => rtc_stream::ListHandler,
    "system.vacuum" => system::VacuumHandler
);

///////////////////////////////////////////////////////////////////////////////

use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum CorrelationData {
    SubscriptionCreate(subscription::CorrelationDataPayload),
    SubscriptionDelete(subscription::CorrelationDataPayload),
    MessageUnicast(message::CorrelationDataPayload),
}

#[async_trait]
pub(crate) trait ResponseHandler {
    type Payload: Send + DeserializeOwned;
    type CorrelationData: Sync;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        respp: &IncomingResponseProperties,
        corr_data: &Self::CorrelationData,
    ) -> Result;
}

macro_rules! response_routes {
    ($($c: tt => $h: ty),*) => {
        #[allow(unused_variables)]
        pub(crate) async fn route_response<C: Context>(
            context: &mut C,
            response: &IncomingResponse<String>,
            corr_data: &str,
            topic: &str,
        ) -> MessageStream {
            // TODO: Refactor janus response handler to use common pattern.
            if topic == context.janus_topics().responses_topic() {
                janus::handle_response::<C>(context, response).await
            } else {
                let corr_data = match CorrelationData::parse(corr_data) {
                    Ok(corr_data) => corr_data,
                    Err(err) => {
                        warn!(
                            context.logger(),
                            "Failed to parse response correlation data '{}': {}", corr_data, err
                        );
                        return Box::new(async_std::stream::empty()) as MessageStream;
                    }
                };
                match corr_data {
                    $(
                        CorrelationData::$c(cd) => <$h>::handle_envelope::<C>(context, response, &cd).await,
                    )*
                }
            }
        }
    }
}

response_routes!(
    SubscriptionCreate => subscription::CreateResponseHandler,
    SubscriptionDelete => subscription::DeleteResponseHandler,
    MessageUnicast => message::UnicastResponseHandler
);

///////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub(crate) trait EventHandler {
    type Payload: Send + DeserializeOwned;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
    ) -> Result;
}

macro_rules! event_routes {
    ($($l: pat => $h: ty),*) => {
        #[allow(unused_variables)]
        pub(crate) async fn route_event<C: Context>(
            context: &mut C,
            event: &IncomingEvent<String>,
            topic: &str,
        ) -> Option<MessageStream> {
            if topic == context.janus_topics().events_topic() {
                Some(janus::handle_event::<C>(context, event).await)
            } else if topic == context.janus_topics().status_events_topic() {
                Some(janus::handle_status_event::<C>(context, event).await)
            } else {
                match event.properties().label() {
                    $(
                        Some($l) => Some(<$h>::handle_envelope::<C>(context, event).await),
                    )*
                    _ => None,
                }
            }
        }
    }
}

// Event routes configuration: label => EventHandler
event_routes!(
    "metric.pull" => metric::PullHandler,
    "subscription.delete" => subscription::DeleteEventHandler
);

///////////////////////////////////////////////////////////////////////////////

mod agent;
mod agent_reader_config;
mod agent_writer_config;
pub(crate) mod helpers;
mod message;
mod metric;
mod room;
pub(crate) mod rtc;
pub(crate) mod rtc_signal;
pub(crate) mod rtc_stream;
mod subscription;
pub(crate) mod system;

pub(self) mod prelude {
    pub(super) use super::{helpers, EventHandler, RequestHandler, ResponseHandler, Result};
    pub(super) use crate::app::endpoint::CorrelationData;
    pub(super) use crate::app::error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind};
}
