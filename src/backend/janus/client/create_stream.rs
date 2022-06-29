use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use svc_agent::{mqtt::IncomingRequestProperties, AgentId};

use crate::db;

use super::{HandleId, Jsep, SessionId};

#[derive(Serialize, Debug)]
pub struct CreateStreamRequest {
    pub session_id: SessionId,
    pub handle_id: HandleId,
    pub body: CreateStreamRequestBody,
    pub jsep: Jsep,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum CreateStreamTransaction {
    Mqtt {
        reqp: IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    },
    Http {
        id: usize,
    },
}

#[derive(Serialize, Debug)]
pub struct WriterConfig {
    pub send_video: bool,
    pub send_audio: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_remb: Option<i64>,
}

#[derive(Serialize, Debug)]
pub struct ReaderConfig {
    pub reader_id: AgentId,
    pub receive_video: bool,
    pub receive_audio: bool,
}

#[derive(Serialize, Debug)]
pub struct CreateStreamRequestBody {
    method: &'static str,
    id: db::rtc::Id,
    agent_id: AgentId,
    writer_config: Option<WriterConfig>,
    reader_configs: Option<Vec<ReaderConfig>>,
}

impl CreateStreamRequestBody {
    pub fn new(
        id: db::rtc::Id,
        agent_id: AgentId,
        writer_config: Option<WriterConfig>,
        reader_configs: Option<Vec<ReaderConfig>>,
    ) -> Self {
        Self {
            method: "stream.create",
            id,
            agent_id,
            writer_config,
            reader_configs,
        }
    }
}
