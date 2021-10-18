use axum::response::IntoResponse;
use chrono::{DateTime, Duration, Utc};
use futures::{stream, Stream};
use http::StatusCode;
use serde::Serialize;
use serde_json::Value;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
        ShortTermTimingProperties,
    },
    Addressable, AgentId, Authenticable,
};

use crate::app::{endpoint::helpers, error::ErrorExt};

use super::error;

// use svc_authn::token::jws_compact::extract::decode_jws_compact_with_config;

pub struct Response {
    notifications: Vec<Box<dyn IntoPublishableMessage + Send + Sync + 'static>>,
    status: StatusCode,
    start_timestamp: DateTime<Utc>,
    authz_time: Option<Duration>,
    payload: Result<Value, serde_json::Error>,
}

impl Response {
    pub fn new(
        status: StatusCode,
        payload: impl Serialize + Send + Sync + 'static,
        start_timestamp: DateTime<Utc>,
        maybe_authz_time: Option<Duration>,
    ) -> Self {
        Self {
            notifications: Vec::new(),
            status,
            start_timestamp,
            authz_time: maybe_authz_time,
            payload: serde_json::to_value(&payload),
        }
    }

    pub fn into_mqtt_messages(
        self,
        reqp: &IncomingRequestProperties,
    ) -> Result<
        Box<
            dyn Stream<Item = Box<dyn IntoPublishableMessage + Send + Sync + 'static>>
                + Send
                + Unpin,
        >,
        error::Error,
    > {
        let mut notifications = self.notifications;
        if self.status != StatusCode::NO_CONTENT {
            let payload = self.payload.error(error::ErrorKind::InvalidPayload)?;
            let response = helpers::build_response(
                self.status,
                payload,
                reqp,
                self.start_timestamp,
                self.authz_time,
            );
            notifications.push(response);
        }
        Ok(Box::new(stream::iter(notifications)))
    }

    pub fn add_notification(
        &mut self,
        label: &'static str,
        path: &str,
        payload: impl Serialize + Send + Sync + 'static,
        start_timestamp: DateTime<Utc>,
    ) {
        let timing = ShortTermTimingProperties::until_now(start_timestamp);
        let props = OutgoingEventProperties::new(label, timing);
        self.notifications
            .push(Box::new(OutgoingEvent::broadcast(payload, props, path)))
    }

    pub fn add_message(
        &mut self,
        message: Box<dyn IntoPublishableMessage + Send + Sync + 'static>,
    ) {
        self.notifications.push(message)
    }
}

impl IntoResponse for Response {
    type Body = axum::body::Body;

    type BodyError = <Self::Body as axum::body::HttpBody>::Error;

    fn into_response(self) -> http::Response<Self::Body> {
        let body = self
            .payload
            .and_then(|payload| Ok(serde_json::to_string(&payload)?))
            .unwrap_or_else(|_| "Bad body".to_string());
        http::Response::builder()
            .status(self.status)
            .extension(self.notifications)
            .body(axum::body::Body::from(body))
            .expect("Must be valid response")
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RequestParams<'a> {
    Http { agent_id: &'a AgentId },
    MqttParams(&'a IncomingRequestProperties),
}

impl<'a> RequestParams<'a> {
    pub fn as_mqtt_params(&self) -> Result<&IncomingRequestProperties, error::Error> {
        match self {
            RequestParams::Http { agent_id: _ } => Err(error::Error::new(
                error::ErrorKind::AccessDenied,
                anyhow::anyhow!("Trying convert http params into mqtt"),
            )),
            RequestParams::MqttParams(p) => Ok(p),
        }
    }
}

impl<'a> Addressable for RequestParams<'a> {
    fn as_agent_id(&self) -> &svc_agent::AgentId {
        match self {
            RequestParams::Http { agent_id } => agent_id,
            RequestParams::MqttParams(reqp) => reqp.as_agent_id(),
        }
    }
}

impl<'a> Authenticable for RequestParams<'a> {
    fn as_account_id(&self) -> &svc_agent::AccountId {
        match self {
            RequestParams::Http { agent_id } => agent_id.as_account_id(),
            RequestParams::MqttParams(reqp) => reqp.as_account_id(),
        }
    }
}
