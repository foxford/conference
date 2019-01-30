use std::ops::Bound;

use chrono::{offset::Utc, DateTime};
use failure::Error;
use serde_derive::Deserialize;

use crate::authn::AgentId;
use crate::db::{room, ConnectionPool};
use crate::transport::mqtt::{
    compat::IntoEnvelope, IncomingRequest, OutgoingResponse, OutgoingResponseStatus, Publishable,
};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    audience: String,
}

pub(crate) type ObjectResponse = OutgoingResponse<room::Object>;

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
        // Creating a Room
        let conn = self.db.get()?;

        let time = (
            Bound::Included(&inreq.payload().start),
            Bound::Included(&inreq.payload().end),
        );

        let object = room::InsertQuery::new(time, &inreq.payload().audience).execute(&conn)?;

        let resp = inreq.to_response(object, &OutgoingResponseStatus::OK);
        resp.into_envelope()
    }
}
