use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{HandleId, SessionId};

#[derive(Debug, Serialize)]
pub struct UploadStreamRequest {
    pub session_id: SessionId,
    pub handle_id: HandleId,
    pub body: UploadStreamRequestBody,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct UploadStreamTransaction {
    pub rtc_id: Uuid,
    pub start_timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct UploadStreamRequestBody {
    method: &'static str,
    id: Uuid,
    backend: String,
    bucket: String,
}

impl UploadStreamRequestBody {
    pub fn new(id: Uuid, backend: &str, bucket: &str) -> Self {
        Self {
            method: "stream.upload",
            id,
            backend: backend.to_owned(),
            bucket: bucket.to_owned(),
        }
    }
}
