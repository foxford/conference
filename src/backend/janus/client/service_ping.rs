use serde::Serialize;

use super::{HandleId, SessionId};

#[derive(Debug, Serialize)]
pub struct ServicePingRequest {
    pub session_id: SessionId,
    pub handle_id: HandleId,
    pub body: ServicePingRequestBody,
}

#[derive(Serialize, Debug)]
pub struct ServicePingRequestBody {
    method: &'static str,
}

impl ServicePingRequestBody {
    pub fn new() -> Self {
        Self {
            method: "service.ping",
        }
    }
}
