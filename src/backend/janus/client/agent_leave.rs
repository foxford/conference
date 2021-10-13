use serde::Serialize;
use svc_agent::AgentId;

use super::HandleId;

#[derive(Serialize, Debug)]
pub struct AgentLeaveRequest {
    pub handle_id: HandleId,
    pub body: AgentLeaveRequestBody,
}

#[derive(Serialize, Debug)]
pub struct AgentLeaveRequestBody {
    method: &'static str,
    agent_id: AgentId,
}

impl AgentLeaveRequestBody {
    pub fn new(agent_id: AgentId) -> Self {
        Self {
            method: "agent.leave",
            agent_id,
        }
    }
}
