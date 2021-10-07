use std::result::Result as StdResult;

pub(self) use crate::app::message_handler::MessageStream;
use crate::{
    app::{
        context::Context,
        error::Error as AppError,
        message_handler::{EventEnvelopeHandler, RequestEnvelopeHandler, ResponseEnvelopeHandler},
    },
    backend::janus,
};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use svc_agent::mqtt::{
    IncomingEvent, IncomingEventProperties, IncomingRequest,
    IncomingResponse, IncomingResponseProperties,
};
use tracing::warn;

///////////////////////////////////////////////////////////////////////////////

pub type RequestResult = StdResult<Response, AppError>;
pub type MqttResult = StdResult<MessageStream, AppError>;

#[async_trait]
pub trait RequestHandler {
    type Payload: Send + DeserializeOwned;
    const ERROR_TITLE: &'static str;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult;
}

macro_rules! request_routes {
    ($($m: pat => $h: ty),*) => {
        pub async fn route_request<C: Context>(
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
    "room.close" => room::CloseHandler,
    "room.create" => room::CreateHandler,
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
    "system.vacuum" => system::VacuumHandler,
    "writer_config_snapshot.read" => writer_config_snapshot::ReadHandler
);

///////////////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use super::service_utils::{RequestParams, Response};

#[derive(Debug, Deserialize, Serialize)]
pub enum CorrelationData {
    SubscriptionCreate(subscription::CorrelationDataPayload),
    SubscriptionDelete(subscription::CorrelationDataPayload),
    MessageUnicast(message::CorrelationDataPayload),
}

#[async_trait]
pub trait ResponseHandler {
    type Payload: Send + DeserializeOwned;
    type CorrelationData: Sync;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        respp: &IncomingResponseProperties,
        corr_data: &Self::CorrelationData,
    ) -> MqttResult;
}

macro_rules! response_routes {
    ($($c: tt => $h: ty),*) => {
        #[allow(unused_variables)]
        pub async fn route_response<C: Context>(
            context: &mut C,
            response: &IncomingResponse<String>,
            corr_data: &str,
            topic: &str,
        ) -> MessageStream {
            let corr_data = match CorrelationData::parse(corr_data) {
                Ok(corr_data) => corr_data,
                Err(err) => {
                    warn!(
                        ?err,
                        %corr_data,
                        "Failed to parse response correlation data"
                    );
                    return Box::new(futures::stream::empty()) as MessageStream;
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

response_routes!(
    SubscriptionCreate => subscription::CreateResponseHandler,
    SubscriptionDelete => subscription::DeleteResponseHandler,
    MessageUnicast => message::UnicastResponseHandler
);

///////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait EventHandler {
    type Payload: Send + DeserializeOwned;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
    ) -> MqttResult;
}

macro_rules! event_routes {
    ($($l: pat => $h: ty),*) => {
        #[allow(unused_variables)]
        pub async fn route_event<C: Context>(
            context: &mut C,
            event: &IncomingEvent<String>,
            topic: &str,
        ) -> Option<MessageStream> {
            if topic == context.janus_topics().status_events_topic() {
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

pub(crate) struct PullHandler;

#[derive(Debug, Deserialize)]
pub(crate) struct PullPayload {
    duration: Option<u64>,
}

#[async_trait]
impl EventHandler for PullHandler {
    type Payload = PullPayload;

    async fn handle<C: Context>(
        _context: &mut C,
        _payload: Self::Payload,
        _evp: &IncomingEventProperties,
    ) -> MqttResult {
        Ok(Box::new(futures::stream::empty()))
    }
}
// Event routes configuration: label => EventHandler
event_routes!(
    "subscription.delete" => subscription::DeleteEventHandler,
    "metric.pull" => PullHandler,
    "system.close_orphaned_rooms" => system::OrphanedRoomCloseHandler
);

///////////////////////////////////////////////////////////////////////////////

pub mod agent;
pub mod agent_reader_config;
pub mod agent_writer_config;
pub mod helpers;
pub mod message;
pub mod room;
pub mod rtc;
pub mod rtc_signal;
pub mod rtc_stream;
pub mod subscription;
pub mod system;
pub mod writer_config_snapshot;

pub(self) mod prelude {
    pub(super) use super::{helpers, EventHandler, RequestHandler, RequestResult, ResponseHandler};
    pub(super) use crate::app::{
        endpoint::CorrelationData,
        error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind},
    };
}
