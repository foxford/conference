use crate::app::janus;
use crate::authn::{AgentId, Authenticable};
use crate::db::{janus_handle_shadow, janus_session_shadow, rtc, ConnectionPool};
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

pub(crate) type ReadRequest = IncomingRequest<ReadRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ReadRequestData {
    id: Uuid,
}

pub(crate) type ListRequest = IncomingRequest<ListRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ListRequestData {}

pub(crate) type RecordResponse = OutgoingResponse<rtc::Record>;
pub(crate) type RecordListResponse = OutgoingResponse<Vec<rtc::Record>>;

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
        let backreq =
            janus::create_session_request(inreq.properties().clone(), record.id().clone(), to)?;

        backreq.into_envelope()
    }

    pub(crate) fn read(&self, inreq: &ReadRequest) -> Result<impl Publishable, Error> {
        let id = inreq.payload().id;

        // Looking up for Janus Handle
        let conn = self.db.get()?;
        let maybe_location =
            janus_handle_shadow::FindLocationQuery::new(&inreq.properties().agent_id(), &id)
                .execute(&conn);

        match maybe_location {
            Ok(_) => {
                // Returning Real-Time connection
                let record = rtc::FindQuery::new(&id).execute(&conn)?;
                let resp = inreq.to_response(record, &OutgoingResponseStatus::OK);
                resp.into_envelope()
            }
            Err(_) => {
                // Looking up for Janus Gateway Session
                let session = janus_session_shadow::FindQuery::new(&id).execute(&conn)?;

                // Building a Create Janus Gateway Handle request
                let backreq = janus::create_handle_request(
                    inreq.properties().clone(),
                    id,
                    session.session_id(),
                    session.location_id().clone(),
                )?;

                backreq.into_envelope()
            }
        }
    }

    pub(crate) fn list(&self, inreq: &ListRequest) -> Result<impl Publishable, Error> {
        // Looking up for Real-Time Connections
        let conn = self.db.get()?;
        let records = rtc::ListQuery::new().execute(&conn)?;

        let resp = inreq.to_response(records, &OutgoingResponseStatus::OK);
        resp.into_envelope()
    }
}
