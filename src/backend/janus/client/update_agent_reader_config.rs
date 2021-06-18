use serde::Serialize;
use svc_agent::AgentId;
use uuid::Uuid;

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
    pub(crate) fn new(configs: Vec<UpdateReaderConfigRequestBodyConfigItem>) -> Self {
        Self {
            method: "reader_config.update",
            configs,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct UpdateReaderConfigRequestBodyConfigItem {
    pub reader_id: AgentId,
    pub stream_id: Uuid,
    pub receive_video: bool,
    pub receive_audio: bool,
}
