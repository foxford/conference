use crate::db;

use super::{HandleId, SessionId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct CreateHandleRequest {
    pub session_id: SessionId,
    pub opaque_id: db::janus_rtc_stream::Id,
}

#[derive(Debug, Deserialize)]
pub struct CreateHandleResponse {
    pub id: HandleId,
}
