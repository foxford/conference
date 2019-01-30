use std::ops::Bound;

use chrono::{DateTime, Utc};
use failure::Error;
use serde_derive::Deserialize;

use crate::db::{room, ConnectionPool};
use crate::transport::mqtt::{
    compat::IntoEnvelope, IncomingRequest, OutgoingResponse, OutgoingResponseStatus, Publishable,
};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
    time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    audience: String,
}

pub(crate) type ObjectResponse = OutgoingResponse<room::Object>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(db: ConnectionPool) -> Self {
        Self { db }
    }
}

impl State {
    pub(crate) fn create(&self, inreq: &CreateRequest) -> Result<impl Publishable, Error> {
        // Creating a Room
        let conn = self.db.get()?;

        let object = room::InsertQuery::new(inreq.payload().time, &inreq.payload().audience)
            .execute(&conn)?;

        let resp = inreq.to_response(object, &OutgoingResponseStatus::OK);
        resp.into_envelope()
    }
}
