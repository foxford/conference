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
    "rtc_stream.list" => rtc_stream::ListHandler,
    "signal.create" => signal::CreateHandler,
    "signal.trickle" => signal::TrickleHandler,
    "system.vacuum" => system::VacuumHandler
);

///////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub(crate) trait ResponseHandler {
    type Payload: Send + DeserializeOwned;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        respp: &IncomingResponseProperties,
    ) -> Result;
}

pub(crate) async fn route_response<C: Context>(
    context: &mut C,
    resp: &IncomingResponse<String>,
    topic: &str,
) -> Option<MessageStream> {
    if topic == context.janus_topics().responses_topic() {
        Some(janus::handle_response::<C>(context, resp).await)
    } else {
        Some(message::CallbackHandler::handle_envelope::<C>(context, resp).await)
    }
}

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
    "room.notify_opened" => room::NotifyOpenedHandler,
    "subscription.delete" => subscription::DeleteHandler,
    "subscription.create" => subscription::CreateHandler
);

///////////////////////////////////////////////////////////////////////////////

mod agent;
pub(crate) mod helpers;
mod message;
mod metric;
mod room;
pub(crate) mod rtc;
pub(crate) mod rtc_stream;
pub(crate) mod signal;
mod subscription;
pub(crate) mod system;

pub(self) mod prelude {
    pub(super) use super::{helpers, EventHandler, RequestHandler, ResponseHandler, Result};
    pub(super) use crate::app::error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind};
}
