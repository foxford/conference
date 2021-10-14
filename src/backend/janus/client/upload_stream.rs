use crate::db;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct UploadStreamRequest {
    pub id: db::rtc::Id,
    pub backend: String,
    pub bucket: String,
}

#[derive(Deserialize)]
pub enum UploadResponse {
    AlreadyRunning {
        id: db::rtc::Id,
    },
    Missing {
        id: db::rtc::Id,
    },
    Done {
        id: db::rtc::Id,
        mjr_dumps_uris: Vec<String>,
    },
}
