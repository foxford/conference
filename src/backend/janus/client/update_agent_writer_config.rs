use crate::db;
use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct UpdateWriterConfigRequest {
    pub configs: Vec<UpdateWriterConfigItem>,
}

#[derive(Debug, Serialize)]
pub struct UpdateWriterConfigItem {
    pub stream_id: db::rtc::Id,
    pub send_video: bool,
    pub send_audio: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_remb: Option<u32>,
}
