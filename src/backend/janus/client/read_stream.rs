use serde::{Deserialize, Serialize};
use svc_agent::{mqtt::IncomingRequestProperties, AgentId};
use uuid::Uuid;

use super::{HandleId, Jsep, SessionId};

#[derive(Serialize, Debug)]
pub struct ReadStreamRequest {
    pub session_id: SessionId,
    pub handle_id: HandleId,
    pub body: ReadStreamRequestBody,
    pub jsep: Jsep,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ReadStreamTransaction {
    pub reqp: IncomingRequestProperties,
}

#[derive(Serialize, Debug)]
pub struct ReadStreamRequestBody {
    method: &'static str,
    id: Uuid,
    agent_id: AgentId,
}

impl ReadStreamRequestBody {
    pub fn new(id: Uuid, agent_id: AgentId) -> Self {
        Self {
            method: "stream.read",
            id,
            agent_id,
        }
    }
}

// pub type ReadStreamResponse = EventResponse;
