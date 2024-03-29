use std::net::IpAddr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use svc_agent::{mqtt::IncomingRequestProperties, AgentId};

use crate::db;

use super::{create_stream::ReaderConfig, HandleId, Jsep, SessionId};

#[derive(Serialize, Debug)]
pub struct ReadStreamRequest {
    pub session_id: SessionId,
    pub handle_id: HandleId,
    pub body: ReadStreamRequestBody,
    pub jsep: Jsep,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum ReadStreamTransaction {
    Mqtt {
        reqp: IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    },
    Http {
        id: usize,
        replica_addr: IpAddr,
    },
}

#[derive(Serialize, Debug)]
pub struct ReadStreamRequestBody {
    method: &'static str,
    id: db::rtc::Id,
    agent_id: AgentId,
    reader_configs: Option<Vec<ReaderConfig>>,
}

impl ReadStreamRequestBody {
    pub fn new(id: db::rtc::Id, agent_id: AgentId, reader_configs: Vec<ReaderConfig>) -> Self {
        Self {
            method: "stream.read",
            id,
            agent_id,
            reader_configs: if reader_configs.is_empty() {
                None
            } else {
                Some(reader_configs)
            },
        }
    }
}

// pub type ReadStreamResponse = EventResponse;
