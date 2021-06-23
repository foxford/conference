use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use svc_agent::{mqtt::IncomingRequestProperties, AgentId};
use uuid::Uuid;

use super::{HandleId, Jsep, SessionId};

#[derive(Serialize, Debug)]
pub struct CreateStreamRequest {
    pub session_id: SessionId,
    pub handle_id: HandleId,
    pub body: CreateStreamRequestBody,
    pub jsep: Jsep,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CreateStreamTransaction {
    pub reqp: IncomingRequestProperties,
    pub start_timestamp: DateTime<Utc>,
}

#[derive(Serialize, Debug)]
pub struct CreateStreamRequestBody {
    method: &'static str,
    id: Uuid,
    agent_id: AgentId,
}

impl CreateStreamRequestBody {
    pub fn new(id: Uuid, agent_id: AgentId) -> Self {
        Self {
            method: "stream.create",
            id,
            agent_id,
        }
    }
}
