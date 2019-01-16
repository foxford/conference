use crate::app::janus;
use crate::authn::AgentId;
use crate::db::{rtc, ConnectionPool};
use crate::transport::mqtt::compat::IntoEnvelope;
use crate::transport::mqtt::{
    IncomingRequest, OutgoingResponse, OutgoingResponseStatus, Publishable,
};
use failure::Error;
use serde_derive::Deserialize;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    room_id: Uuid,
}

pub(crate) type CreateResponse = OutgoingResponse<rtc::Record>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) type ReadRequest = IncomingRequest<ReadRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ReadRequestData {
    id: Uuid,
}

pub(crate) type ReadResponse = OutgoingResponse<rtc::Record>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    db: ConnectionPool,
    // TODO: replace with backend agent registry
    backend_agent_id: AgentId,
}

impl State {
    pub(crate) fn new(db: ConnectionPool, backend_agent_id: AgentId) -> Self {
        Self {
            db,
            backend_agent_id,
        }
    }
}

impl State {
    pub(crate) fn create(&self, inreq: &CreateRequest) -> Result<impl Publishable, Error> {
        // Creating a Real-Time Connection
        let conn = self.db.get()?;
        let record = rtc::InsertQuery::new(&inreq.payload().room_id).execute(&conn)?;

        // Building a Create Janus Session request
        let to = self.backend_agent_id.clone();
        let backreq = janus::create_session_request(inreq.properties().clone(), record, to)?;

        backreq.into_envelope()
    }

    pub(crate) fn read(&self, inreq: &ReadRequest) -> Result<impl Publishable, Error> {
        let conn = self.db.get()?;
        let record = rtc::FindQuery::new(&inreq.payload().id).execute(&conn)?;

        let resp: ReadResponse = inreq.to_response(record, &OutgoingResponseStatus::Success);

        let backreq = janus::create_handle_request(tn, session_id, location_id)?;

        resp.into_envelope()
    }
}
