use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use svc_agent::{mqtt::IncomingRequestProperties, AgentId};

use crate::db;

use super::{create_handle::OpaqueId, HandleId, Jsep, SessionId};

#[derive(Serialize, Debug)]
pub struct ReadStreamRequest {
    pub session_id: SessionId,
    pub handle_id: HandleId,
    pub body: ReadStreamRequestBody,
    pub jsep: Jsep,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ReadStreamTransaction;

#[derive(Serialize, Debug)]
pub struct ReadStreamRequestBody {
    method: &'static str,
    id: db::rtc::Id,
    agent_id: AgentId,
}

impl ReadStreamRequestBody {
    pub fn new(id: db::rtc::Id, agent_id: AgentId) -> Self {
        Self {
            method: "stream.read",
            id,
            agent_id,
        }
    }
}

#[derive(Deserialize)]
pub struct ReadStreamResponse {
    pub session_id: SessionId,
    pub opaque_id: OpaqueId,
    pub jsep: Value,
}
