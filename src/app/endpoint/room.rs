use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use std::ops::Bound;
use svc_agent::mqtt::{
    compat::IntoEnvelope, Connection, IncomingRequest, OutgoingRequest, OutgoingRequestProperties,
    OutgoingResponse, Publish, ResponseStatus,
};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::db::{room, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type EnterRequest = IncomingRequest<EnterRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct EnterRequestData {
    id: Uuid,
}

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

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
struct SubscriptionRequest {
    subject: Connection,
    object: Vec<String>,
}

impl SubscriptionRequest {
    fn new(subject: Connection, object: Vec<&str>) -> Self {
        Self {
            subject: subject,
            object: object.iter().map(|&s| s.into()).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub(crate) struct State {
    broker_account_id: svc_agent::AccountId,
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(
        broker_account_id: svc_agent::AccountId,
        authz: svc_authz::ClientMap,
        db: ConnectionPool,
    ) -> Self {
        Self {
            broker_account_id,
            authz,
            db,
        }
    }
}

impl State {
    pub(crate) async fn enter(&self, inreq: EnterRequest) -> Result<impl Publish, SvcError> {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .id(inreq.payload().id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?
        };

        // Authorization: room's owner has to allow the action
        self.authz.authorize(
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id, "events"],
            "subscribe",
        )?;

        let brokerreq = {
            let payload = SubscriptionRequest::new(
                inreq.properties().to_connection(),
                vec!["rooms", &room_id, "events"],
            );
            let props = OutgoingRequestProperties::new(
                "subscription.create",
                inreq.properties().response_topic(),
                inreq.properties().correlation_data(),
            );
            OutgoingRequest::multicast(payload, props, &self.broker_account_id)
        };

        brokerreq.into_envelope().map_err(Into::into)
    }

    pub(crate) async fn create(&self, inreq: CreateRequest) -> Result<impl Publish, SvcError> {
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

        let resp = inreq.to_response(object, ResponseStatus::OK);
        resp.into_envelope().map_err(Into::into)
    }

    pub(crate) async fn read(&self, inreq: ReadRequest) -> Result<impl Publish, SvcError> {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .id(inreq.payload().id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?
        };

        // Authorization: room's owner has to allow the action
        self.authz.authorize(
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id],
            "read",
        )?;

        let resp = inreq.to_response(object, ResponseStatus::OK);
        resp.into_envelope().map_err(Into::into)
    }

    pub(crate) async fn update(&self, inreq: UpdateRequest) -> Result<impl Publish, SvcError> {
        let room_id = inreq.payload().id().to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .id(inreq.payload().id())
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?
        };

        // Authorization: room's owner has to allow the action
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

        let resp = inreq.to_response(object, ResponseStatus::OK);
        resp.into_envelope().map_err(Into::into)
    }

    pub(crate) async fn delete(&self, inreq: DeleteRequest) -> Result<impl Publish, SvcError> {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .id(inreq.payload().id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?
        };

        // Authorization: room's owner has to allow the action
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

        let resp = inreq.to_response(object, ResponseStatus::OK);
        resp.into_envelope().map_err(Into::into)
    }
}
