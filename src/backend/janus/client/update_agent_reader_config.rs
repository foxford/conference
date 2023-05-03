use serde::{Deserialize, Serialize};
use svc_agent::AgentId;

use crate::db;

use super::{HandleId, SessionId};

#[derive(Serialize, Debug)]
pub struct UpdateReaderConfigRequest {
    pub session_id: SessionId,
    pub handle_id: HandleId,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateReaderConfigRequestBodyConfigItem {
    pub reader_id: AgentId,
    pub stream_id: db::rtc::Id,
    pub receive_video: bool,
    pub receive_audio: bool,
}
