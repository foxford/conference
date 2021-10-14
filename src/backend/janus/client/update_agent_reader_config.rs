use serde::Serialize;
use svc_agent::AgentId;

use crate::db;

#[derive(Serialize, Debug)]
pub struct UpdateReaderConfigRequest {
    pub configs: Vec<UpdateReaderConfigItem>,
}

#[derive(Debug, Serialize)]
pub struct UpdateReaderConfigItem {
    pub reader_id: AgentId,
    pub stream_id: db::rtc::Id,
    pub receive_video: bool,
    pub receive_audio: bool,
}
