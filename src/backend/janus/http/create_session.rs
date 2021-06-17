use serde::Deserialize;

use super::SessionId;

#[derive(Deserialize, Debug)]
pub struct CreateSessionResponse {
    pub id: SessionId,
}
