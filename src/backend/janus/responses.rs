use std::fmt;

use serde_derive::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "janus")]
pub(crate) enum IncomingResponse {
    Error(ErrorResponse),
    Ack(AckResponse),
    Event(EventResponse),
    Success(SuccessResponse),
}

// An error making a request occurred due to invalid session or handle.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum ErrorResponse {
    Handle(HandleErrorResponse),
    Session(SessionErrorResponse),
}

#[derive(Debug, Deserialize)]
pub(crate) struct HandleErrorResponse {
    transaction: String,
    session_id: i64,
    error: ErrorResponseData,
}

impl HandleErrorResponse {
    pub(crate) fn error(&self) -> &ErrorResponseData {
        &self.error
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionErrorResponse {
    transaction: String,
    error: ErrorResponseData,
}

impl SessionErrorResponse {
    pub(crate) fn error(&self) -> &ErrorResponseData {
        &self.error
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ErrorResponseData {
    code: i32,
    reason: String,
}

impl fmt::Display for ErrorResponseData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} ({})", self.reason, self.code)
    }
}

// A request to a plugin handle was received.
#[derive(Debug, Deserialize)]
pub(crate) struct AckResponse {
    transaction: String,
    session_id: i64,
}

impl AckResponse {
    pub(crate) fn transaction(&self) -> &str {
        &self.transaction
    }
}

// A response on a request sent to a plugin handle.
#[derive(Debug, Deserialize)]
pub struct EventResponse {
    transaction: String,
    session_id: i64,
    plugindata: EventResponsePluginData,
    jsep: Option<JsonValue>,
}

impl EventResponse {
    pub(crate) fn transaction(&self) -> &str {
        &self.transaction
    }

    pub(crate) fn jsep(&self) -> Option<&JsonValue> {
        self.jsep.as_ref()
    }

    pub(crate) fn plugin(&self) -> &EventResponsePluginData {
        &self.plugindata
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct EventResponsePluginData {
    data: Option<JsonValue>,
    plugin: String,
}

impl EventResponsePluginData {
    pub(crate) fn data(&self) -> Option<&JsonValue> {
        self.data.as_ref()
    }
}

// A success response on request sent to a plugin handle.
#[derive(Debug, Deserialize)]
pub(crate) struct SuccessResponse {
    transaction: String,
    data: SuccessResponseData,
}

impl SuccessResponse {
    pub(crate) fn transaction(&self) -> &str {
        &self.transaction
    }

    pub(crate) fn data(&self) -> &SuccessResponseData {
        &self.data
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct SuccessResponseData {
    id: i64,
}

impl SuccessResponseData {
    pub(crate) fn id(&self) -> i64 {
        self.id
    }
}
