use serde::Serialize;

use super::{HandleId, Jsep};

#[derive(Serialize, Debug)]
pub struct TrickleRequest {
    pub handle_id: HandleId,
    pub candidate: Jsep,
}
