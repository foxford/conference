use serde::{Deserialize, Serialize};
use serde_json::Value;
use svc_agent::AgentId;

use crate::db;

use super::{HandleId, Jsep};

#[derive(Serialize, Debug)]
pub struct ReadStreamRequest {
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
    pub jsep: Value,
}
