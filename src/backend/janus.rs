use serde_derive::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct CreateSessionRequest {
    transaction: String,
    janus: String,
}

impl CreateSessionRequest {
    pub(crate) fn new(transaction: &str) -> Self {
        Self {
            transaction: transaction.to_owned(),
            janus: "create".to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct CreateHandleRequest {
    transaction: String,
    session_id: u64,
    plugin: String,
    janus: String,
}

impl CreateHandleRequest {
    pub(crate) fn new(transaction: &str, session_id: u64, plugin: &str) -> Self {
        Self {
            transaction: transaction.to_owned(),
            session_id,
            plugin: plugin.to_owned(),
            janus: "attach".to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "janus")]
pub(crate) enum Response {
    Error(ErrorResponse),
    Success(SuccessResponse),
    Timeout(TimeoutEvent),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum ErrorResponse {
    Handle(HandleErrorResponse),
    Session(SessionErrorResponse),
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionErrorResponse {
    pub(crate) transaction: String,
    pub(crate) error: ErrorResponseData,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HandleErrorResponse {
    pub(crate) transaction: String,
    pub(crate) session_id: u64,
    pub(crate) error: ErrorResponseData,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ErrorResponseData {
    pub(crate) code: u32,
    pub(crate) reason: String,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct SuccessResponse {
    pub(crate) transaction: String,
    pub(crate) data: SuccessResponseData,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SuccessResponseData {
    pub(crate) id: u64,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct TimeoutEvent {
    pub(crate) session_id: u64,
}
