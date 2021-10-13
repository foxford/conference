use crate::db;

use super::HandleId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct CreateHandleRequest {
    #[serde(with = "super::serialize_as_base64")]
    pub opaque_id: Option<OpaqueId>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OpaqueId {
    pub stream_id: db::janus_rtc_stream::Id,
    pub room_id: db::room::Id,
}

#[derive(Debug, Deserialize)]
pub struct CreateHandleResponse {
    pub id: HandleId,
}
