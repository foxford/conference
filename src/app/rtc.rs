use crate::app::janus;
use crate::backend::janus::CreateSessionRequest;
use crate::transport::mqtt::{
    IncomingRequest, OutgoingRequest, OutgoingResponse, OutgoingResponseStatus,
};
use crate::transport::{AgentId, Authenticable};
use crate::PgPool;
use failure::Error;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use crate::app::model::rtc;

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateRequestData {
    room_id: Uuid,
}

pub(crate) type ReadRequest = IncomingRequest<ReadRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ReadRequestData {
    id: Uuid,
}

pub(crate) struct State {
    db: PgPool,
    // TODO: replace with backend agent registery
    backend_agent_id: AgentId,
}

impl State {
    pub(crate) fn new(db: PgPool, backend_agent_id: AgentId) -> Self {
        Self {
            db,
            backend_agent_id,
        }
    }
}

impl State {
    pub(crate) fn create(
        &self,
        req: CreateRequest,
    ) -> Result<OutgoingRequest<CreateSessionRequest>, Error> {
        // Creating a Real-Time Connection
        let conn = self.db.get()?;
        let record = rtc::InsertQuery::new(&req.payload().room_id).execute(&conn)?;

        // TODO: reuse a Janus Session if it already exists (create only Janus Handler)
        // Building a Create Janus Session request
        let to = self.backend_agent_id.clone();
        let backend_req = janus::create_session_request(record, req, to)?;

        Ok(backend_req)
    }

    pub(crate) fn read(&self, req: &ReadRequest) -> Result<OutgoingResponse<rtc::Record>, Error> {
        let conn = self.db.get()?;
        let record = rtc::FindQuery::new(&req.payload().id).execute(&conn)?;
        let status = OutgoingResponseStatus::Success;
        let resp = req.to_response(record, status);
        Ok(resp)
    }
}
