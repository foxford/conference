use std::result::Result as StdResult;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use svc_agent::mqtt::{
    IncomingEvent, IncomingEventProperties, IncomingRequest, IncomingRequestProperties,
    IncomingResponse, IncomingResponseProperties,
};
use svc_error::Error as SvcError;

use crate::app::context::Context;
use crate::app::janus;
pub(self) use crate::app::message_handler::MessageStream;
use crate::app::message_handler::{
    EventEnvelopeHandler, RequestEnvelopeHandler, ResponseEnvelopeHandler,
};

///////////////////////////////////////////////////////////////////////////////

pub(crate) type Result = StdResult<MessageStream, SvcError>;

#[async_trait]
pub(crate) trait RequestHandler {
    type Payload: Send + DeserializeOwned;
    const ERROR_TITLE: &'static str;

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result;
}

macro_rules! request_routes {
    ($($m: pat => $h: ty),*) => {
        pub(crate) async fn route_request<C: Context>(
            context: &C,
            request: &IncomingRequest<String>,
            _topic: &str,
            start_timestamp: DateTime<Utc>,
        ) -> Option<MessageStream> {
            match request.properties().method() {
                $(
                    $m => Some(
                        <$h>::handle_envelope::<C>(context, request, start_timestamp).await
                    ),
                )*
                _ => None,
            }
        }
    }
}

// Request routes configuration: method => RequestHandler
request_routes!(
    "agent.list" => agent::ListHandler,
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

#[async_trait]
pub(crate) trait ResponseHandler {
    type Payload: Send + DeserializeOwned;

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        respp: &IncomingResponseProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result;
}

pub(crate) async fn route_response<C: Context>(
    context: &C,
    resp: &IncomingResponse<String>,
    topic: &str,
    start_timestamp: DateTime<Utc>,
) -> Option<MessageStream> {
    if topic == context.janus_topics().responses_topic() {
        Some(janus::handle_response::<C>(context, resp, start_timestamp).await)
    } else {
        Some(message::CallbackHandler::handle_envelope::<C>(context, resp, start_timestamp).await)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub(crate) trait EventHandler {
    type Payload: Send + DeserializeOwned;

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result;
}

macro_rules! event_routes {
    ($($l: pat => $h: ty),*) => {
        #[allow(unused_variables)]
        pub(crate) async fn route_event<C: Context>(
            context: &C,
            event: &IncomingEvent<String>,
            topic: &str,
            start_timestamp: DateTime<Utc>,
        ) -> Option<MessageStream> {
            if topic == context.janus_topics().events_topic() {
                Some(janus::handle_event::<C>(context, event, start_timestamp).await)
            } else if topic == context.janus_topics().status_events_topic() {
                Some(janus::handle_status_event::<C>(context, event, start_timestamp).await)
            } else {
                match event.properties().label() {
                    $(
                        Some($l) => Some(
                            <$h>::handle_envelope::<C>(context, event, start_timestamp).await
                        ),
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
    "subscription.delete" => subscription::DeleteHandler,
    "subscription.create" => subscription::CreateHandler
);

///////////////////////////////////////////////////////////////////////////////

mod agent;
mod message;
mod metric;
mod room;
pub(crate) mod rtc;
pub(crate) mod rtc_signal;
pub(crate) mod rtc_stream;
pub(self) mod shared;
mod subscription;
pub(crate) mod system;

pub(self) mod prelude {
    pub(super) use super::{shared, EventHandler, RequestHandler, ResponseHandler, Result};
    pub(super) use crate::app::message_handler::SvcErrorSugar;
}
