use crate::app::janus;
use crate::backend::janus::CreateSessionRequest;
use crate::transport::mqtt::{LocalRequest, Request};
use crate::transport::{AgentId, Authenticable};
use failure::Error;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use crate::app::model::rtc;

pub(crate) type CreateRequest = Request<CreateRequestData>;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateRequestData {
    room_id: Uuid,
}

pub(crate) struct State {
    // TODO: replace with backend agent registery, make private
    pub(crate) backend_agent_id: AgentId,
}

impl State {
    pub(crate) fn create(
        &self,
        input: CreateRequest,
    ) -> Result<LocalRequest<CreateSessionRequest>, Error> {
        // Creating a Real-Time Connection
        let record =
            rtc::InsertQuery::new(&input.payload().room_id, &input.properties().account_id())
                .execute()?;

        // TODO: reuse a Janus Session if it already exists (create only Janus Handler)
        // Building a Create Janus Session request
        let to = self.backend_agent_id.clone();
        let req = janus::create_session_request(record, input, to)?;

        Ok(req)
    }
}
