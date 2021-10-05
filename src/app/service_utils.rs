use chrono::{DateTime, Duration, Utc};
use futures::Stream;
use http::StatusCode;
use serde::Serialize;
use svc_agent::{Addressable, AgentId, Authenticable, mqtt::{IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties, ShortTermTimingProperties}};

// use svc_authn::token::jws_compact::extract::decode_jws_compact_with_config;

pub struct Response {
    notifications: Vec<Box<dyn IntoPublishableMessage + Send>>,
    request_params: RequestParams,
    status: StatusCode,
    start_timestamp: DateTime<Utc>,
    authz_time: Option<Duration>,
    payload: Result<Vec<u8>, serde_json::Error>,
}

impl Response {
    pub fn new(
        status: StatusCode,
        payload: impl Serialize + Send + 'static,
        reqp: RequestParams,
        start_timestamp: DateTime<Utc>,
        maybe_authz_time: Option<Duration>,
    ) -> Self {
        Self {
            notifications: Vec::new(),
            request_params: reqp,
            status,
            start_timestamp,
            authz_time: maybe_authz_time,
            payload: serde_json::to_vec(&payload),
        }
    }

    pub fn add_notification(
        &mut self,
        label: &'static str,
        path: &str,
        payload: impl Serialize + Send + 'static,
        start_timestamp: DateTime<Utc>,
    ) {
        let timing = ShortTermTimingProperties::until_now(start_timestamp);
        let mut props = OutgoingEventProperties::new(label, timing);
        self.notifications
            .push(Box::new(OutgoingEvent::broadcast(payload, props, path)))
    }
}

pub enum RequestParams {
    HttpHeaders { agent_id: AgentId },
    MqttParams(IncomingRequestProperties),
}

impl Addressable for RequestParams {
    fn as_agent_id(&self) -> &svc_agent::AgentId {
        match self {
            RequestParams::HttpHeaders { agent_id } => agent_id,
            RequestParams::MqttParams(reqp) => reqp.as_agent_id(),
        }
    }
}

impl Authenticable for RequestParams {
    fn as_account_id(&self) -> &svc_agent::AccountId {
        match self {
            RequestParams::HttpHeaders { agent_id } => agent_id.as_account_id(),
            RequestParams::MqttParams(reqp) => reqp.as_account_id(),
        }
    }
}

// fn extract_token(headers: &HeaderMap) -> Option<&str> {
//     headers.get("Authorization")?.to_str().ok();
//     let token = token
//         .map(|s| s.replace("Bearer ", ""))
//         .unwrap_or_else(|| "".to_string());

//     let claims = decode_jws_compact_with_config::<String>(&token, &self.config.authn)?.claims;
//     let account = AccountId::new(claims.subject(), claims.audience());

//     Ok(account)
// }
