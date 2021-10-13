use crate::db;
use serde::Serialize;



#[derive(Serialize, Debug)]
pub struct UpdateWriterConfigRequest {
    pub body: UpdateWriterConfigRequestBody,
}

#[derive(Debug, Serialize)]
pub struct UpdateWriterConfigRequestBody {
    method: &'static str,
    configs: Vec<UpdateWriterConfigRequestBodyConfigItem>,
}

impl UpdateWriterConfigRequestBody {
    pub fn new(configs: Vec<UpdateWriterConfigRequestBodyConfigItem>) -> Self {
        Self {
            method: "writer_config.update",
            configs,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct UpdateWriterConfigRequestBodyConfigItem {
    pub stream_id: db::rtc::Id,
    pub send_video: bool,
    pub send_audio: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_remb: Option<u32>,
}
