use async_std::prelude::*;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde_json::json;
use svc_agent::mqtt::{IncomingEventProperties, IncomingRequestProperties};

use crate::app::endpoint::{EventHandler, RequestHandler};
use crate::app::error::Error as AppError;
use crate::app::message_handler::MessageStream;

use self::agent::TestAgent;
use self::context::TestContext;
use self::outgoing_envelope::{
    OutgoingEnvelope, OutgoingEnvelopeProperties, OutgoingEventProperties,
    OutgoingRequestProperties, OutgoingResponseProperties,
};

///////////////////////////////////////////////////////////////////////////////

pub(crate) const SVC_AUDIENCE: &'static str = "dev.svc.example.org";
pub(crate) const USR_AUDIENCE: &'static str = "dev.usr.example.org";

pub(crate) async fn handle_request<H: RequestHandler>(
    context: &mut TestContext,
    agent: &TestAgent,
    payload: H::Payload,
) -> Result<Vec<OutgoingEnvelope>, AppError> {
    let agent_id = agent.agent_id().to_string();
    let now = Utc::now().timestamp().to_string();

    let reqp_json = json!({
        "type": "request",
        "correlation_data": "ignore",
        "method": "ignore",
        "agent_id": agent_id,
        "connection_mode": "default",
        "connection_version": "v2",
        "response_topic": format!("agents/{}/api/v2/in/event.{}", agent_id, SVC_AUDIENCE),
        "broker_agent_id": format!("alpha.mqtt-gateway.{}", SVC_AUDIENCE),
        "broker_timestamp": now,
        "broker_processing_timestamp": now,
        "broker_initial_processing_timestamp": now,
        "tracking_id": "16911d40-0b13-11ea-8171-60f81db6d53e.14097484-0c8d-11ea-bb82-60f81db6d53e.147b2994-0c8d-11ea-8933-60f81db6d53e",
        "session_tracking_label": "16cc4294-0b13-11ea-91ae-60f81db6d53e.16ee876e-0b13-11ea-8c32-60f81db6d53e 2565f962-0b13-11ea-9359-60f81db6d53e.25c2b97c-0b13-11ea-9f20-60f81db6d53e",
    });

    let reqp = serde_json::from_value::<IncomingRequestProperties>(reqp_json)
        .expect("Failed to parse reqp");

    let messages = H::handle(context, payload, &reqp).await?;
    Ok(parse_messages(messages).await)
}

pub(crate) async fn handle_event<H: EventHandler>(
    context: &mut TestContext,
    agent: &TestAgent,
    payload: H::Payload,
) -> Result<Vec<OutgoingEnvelope>, AppError> {
    let agent_id = agent.agent_id().to_string();
    let now = Utc::now().timestamp().to_string();

    let evp_json = json!({
        "type": "event",
        "label": "ignore",
        "agent_id": agent_id,
        "connection_mode": "default",
        "connection_version": "v2",
        "broker_agent_id": format!("alpha.mqtt-gateway.{}", SVC_AUDIENCE),
        "broker_timestamp": now,
        "broker_processing_timestamp": now,
        "broker_initial_processing_timestamp": now,
        "tracking_id": "16911d40-0b13-11ea-8171-60f81db6d53e.14097484-0c8d-11ea-bb82-60f81db6d53e.147b2994-0c8d-11ea-8933-60f81db6d53e",
        "session_tracking_label": "16cc4294-0b13-11ea-91ae-60f81db6d53e.16ee876e-0b13-11ea-8c32-60f81db6d53e 2565f962-0b13-11ea-9359-60f81db6d53e.25c2b97c-0b13-11ea-9f20-60f81db6d53e",
    });

    let evp =
        serde_json::from_value::<IncomingEventProperties>(evp_json).expect("Failed to parse evp");

    let messages = H::handle(context, payload, &evp).await?;
    Ok(parse_messages(messages).await)
}

async fn parse_messages(mut messages: MessageStream) -> Vec<OutgoingEnvelope> {
    let mut parsed_messages = vec![];

    while let Some(message) = messages.next().await {
        let dump = message
            .into_dump(TestAgent::new("alpha", "conference", SVC_AUDIENCE).address())
            .expect("Failed to dump outgoing message");

        let mut parsed_message = serde_json::from_str::<OutgoingEnvelope>(dump.payload())
            .expect("Failed to parse dumped message");

        parsed_message.set_topic(dump.topic());
        parsed_messages.push(parsed_message);
    }

    parsed_messages
}

pub(crate) fn find_event<P>(messages: &[OutgoingEnvelope]) -> (P, &OutgoingEventProperties, &str)
where
    P: DeserializeOwned,
{
    for message in messages {
        if let OutgoingEnvelopeProperties::Event(evp) = message.properties() {
            return (message.payload::<P>(), evp, message.topic());
        }
    }

    panic!("Event not found");
}

pub(crate) fn find_event_by_predicate<P, F>(
    messages: &[OutgoingEnvelope],
    f: F,
) -> Option<(P, &OutgoingEventProperties, &str)>
where
    P: DeserializeOwned,
    F: Fn(&OutgoingEventProperties, P, &str) -> bool,
{
    for message in messages {
        if let OutgoingEnvelopeProperties::Event(evp) = message.properties() {
            if f(evp, message.payload::<P>(), message.topic()) {
                return Some((message.payload::<P>(), evp, message.topic()));
            }
        }
    }

    return None;
}

pub(crate) fn find_response<P>(messages: &[OutgoingEnvelope]) -> (P, &OutgoingResponseProperties)
where
    P: DeserializeOwned,
{
    for message in messages {
        if let OutgoingEnvelopeProperties::Response(respp) = message.properties() {
            return (message.payload::<P>(), respp);
        }
    }

    panic!("Response not found");
}

pub(crate) fn find_request<P>(
    messages: &[OutgoingEnvelope],
) -> (P, &OutgoingRequestProperties, &str)
where
    P: DeserializeOwned,
{
    for message in messages {
        if let OutgoingEnvelopeProperties::Request(reqp) = message.properties() {
            return (message.payload::<P>(), reqp, message.topic());
        }
    }

    panic!("Request not found");
}

pub(crate) fn find_request_by_predicate<P, F>(
    messages: &[OutgoingEnvelope],
    f: F,
) -> Option<(P, &OutgoingRequestProperties, &str)>
where
    P: DeserializeOwned,
    F: Fn(&OutgoingRequestProperties, P) -> bool,
{
    for message in messages {
        if let OutgoingEnvelopeProperties::Request(reqp) = message.properties() {
            if f(reqp, message.payload::<P>()) {
                return Some((message.payload::<P>(), reqp, message.topic()));
            }
        }
    }

    None
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) mod prelude {
    #[allow(unused_imports)]
    pub(crate) use crate::app::context::GlobalContext;

    #[allow(unused_imports)]
    pub(crate) use super::{
        agent::TestAgent, authz::TestAuthz, context::TestContext, db::TestDb, factory, find_event,
        find_request, find_response, handle_event, handle_request, shared_helpers, SVC_AUDIENCE,
        USR_AUDIENCE,
    };
}

pub(crate) mod agent;
pub(crate) mod authz;
pub(crate) mod context;
pub(crate) mod db;
pub(crate) mod factory;
pub(crate) mod outgoing_envelope;
pub(crate) mod shared_helpers;
