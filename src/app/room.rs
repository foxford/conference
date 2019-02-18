use chrono::{DateTime, Utc};
use failure::{format_err, Error};
use serde_derive::Deserialize;
use std::ops::Bound;
use svc_agent::mqtt::{
    compat::IntoEnvelope, IncomingRequest, OutgoingResponse, OutgoingResponseStatus, Publishable,
};
use svc_authn::Authenticable;
use uuid::Uuid;

use crate::db::{room, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
    time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    audience: String,
}

pub(crate) type ReadRequest = IncomingRequest<ReadRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ReadRequestData {
    id: Uuid,
}

pub(crate) type DeleteRequest = ReadRequest;

pub(crate) type UpdateRequest = IncomingRequest<room::UpdateQuery>;

pub(crate) type ObjectResponse = OutgoingResponse<room::Object>;
pub(crate) type EmptyResponse = OutgoingResponse<()>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(authz: svc_authz::ClientMap, db: ConnectionPool) -> Self {
        Self { authz, db }
    }
}

impl State {
    pub(crate) fn create(&self, inreq: &CreateRequest) -> Result<impl Publishable, Error> {
        // Authorization: future room's owner has to allow the action
        self.authz.authorize(
            &inreq.payload().audience,
            inreq.properties(),
            vec!["rooms"],
            "create",
        )?;

        // Creating a Room
        let object = {
            let conn = self.db.get()?;
            room::InsertQuery::new(inreq.payload().time, &inreq.payload().audience)
                .execute(&conn)?
        };

        let resp = inreq.to_response(object, OutgoingResponseStatus::OK);
        resp.into_envelope()
    }

    pub(crate) fn read(&self, inreq: &ReadRequest) -> Result<impl Publishable, Error> {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .id(&inreq.payload().id)
                .execute(&conn)?
                .ok_or_else(|| format_err!("room with Id = {} is not found", room_id))?
        };

        self.authz.authorize(
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id],
            "read",
        )?;

        let resp = inreq.to_response(object, OutgoingResponseStatus::OK);
        resp.into_envelope()
    }

    pub(crate) fn delete(&self, inreq: &DeleteRequest) -> Result<impl Publishable, Error> {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .id(&inreq.payload().id)
                .execute(&conn)?
                .ok_or_else(|| format_err!("room with Id = {} is not found", room_id))?
        };

        self.authz.authorize(
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id],
            "delete",
        )?;

        let _ = {
            let conn = self.db.get()?;
            room::DeleteQuery::new(inreq.payload().id).execute(&conn)?
        };

        let resp = inreq.to_response((), OutgoingResponseStatus::OK);
        resp.into_envelope()
    }

    pub(crate) fn update(&self, inreq: &UpdateRequest) -> Result<impl Publishable, Error> {
        let room_id = inreq.payload().id().to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .id(&inreq.payload().id())
                .execute(&conn)?
                .ok_or_else(|| format_err!("room with Id = {} is not found", room_id))?
        };

        self.authz.authorize(
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id],
            "update",
        )?;

        let object = {
            let conn = self.db.get()?;
            inreq.payload().execute(&conn)?
        };

        let resp = inreq.to_response(object, OutgoingResponseStatus::OK);
        resp.into_envelope()
    }
}
