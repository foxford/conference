use crate::app::janus;
use crate::backend::janus::CreateSessionRequest;
use crate::transport::mqtt::{
    IncomingRequest, OutgoingRequest, OutgoingResponse, OutgoingResponseStatus,
};
use crate::transport::{AgentId, Authenticable};
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
    // TODO: replace with backend agent registery, make private
    pub(crate) backend_agent_id: AgentId,
}

impl State {
    pub(crate) fn create(
        &self,
        req: CreateRequest,
    ) -> Result<OutgoingRequest<CreateSessionRequest>, Error> {
        // Creating a Real-Time Connection
        let data = req.payload();
        let props = req.properties();
        let record = rtc::InsertQuery::new(&data.room_id, &props.account_id()).execute()?;

        // TODO: reuse a Janus Session if it already exists (create only Janus Handler)
        // Building a Create Janus Session request
        let to = self.backend_agent_id.clone();
        let req = janus::create_session_request(record, req, to)?;

        Ok(req)
    }

    pub(crate) fn read(&self, req: &ReadRequest) -> Result<OutgoingResponse<rtc::Record>, Error> {
        let record = rtc::FindQuery::new(&req.payload().id).execute()?;
        let status = OutgoingResponseStatus::Success;
        let resp = req.to_response(record, status);
        Ok(resp)
    }
}
