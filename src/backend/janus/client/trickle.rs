use serde::Serialize;

use super::{HandleId, Jsep, SessionId};

#[derive(Serialize, Debug)]
pub struct TrickleRequest {
    pub session_id: SessionId,
    pub handle_id: HandleId,
    pub candidate: Jsep,
}
