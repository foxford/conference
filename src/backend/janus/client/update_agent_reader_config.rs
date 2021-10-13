use serde::Serialize;
use svc_agent::AgentId;

use crate::db;



#[derive(Serialize, Debug)]
pub struct UpdateReaderConfigRequest {
    pub body: UpdateReaderConfigRequestBody,
}

#[derive(Debug, Serialize)]
pub struct UpdateReaderConfigRequestBody {
    method: &'static str,
    configs: Vec<UpdateReaderConfigRequestBodyConfigItem>,
}

impl UpdateReaderConfigRequestBody {
    pub fn new(configs: Vec<UpdateReaderConfigRequestBodyConfigItem>) -> Self {
        Self {
            method: "reader_config.update",
            configs,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct UpdateReaderConfigRequestBodyConfigItem {
    pub reader_id: AgentId,
    pub stream_id: db::rtc::Id,
    pub receive_video: bool,
    pub receive_audio: bool,
}
