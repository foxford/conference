use serde_derive::{Deserialize, Serialize};
use svc_agent::mqtt::compat::{IntoEnvelope, OutgoingEnvelope};
use svc_agent::mqtt::{IncomingRequest, OutgoingResponse, ResponseStatus};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::db::{janus_backend, room, rtc, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 25;

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
pub(crate) struct ListRequestData {
    room_id: Uuid,
    offset: Option<i64>,
    limit: Option<i64>,
}

pub(crate) type ConnectRequest = IncomingRequest<ConnectRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ConnectRequestData {
    id: Uuid,
}

#[derive(Debug, Serialize)]
pub(crate) struct ConnectResponseData {
    handle_id: super::rtc_signal::HandleId,
}

impl ConnectResponseData {
    pub(crate) fn new(handle_id: super::rtc_signal::HandleId) -> Self {
        Self { handle_id }
    }
}

pub(crate) type ConnectResponse = OutgoingResponse<ConnectResponseData>;

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
    pub(crate) async fn create(
        &self,
        inreq: CreateRequest,
    ) -> Result<Vec<Box<OutgoingEnvelope>>, SvcError> {
        let room_id = inreq.payload().room_id;

        // Authorization: room's owner has to allow the action
        {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .time(room::upto_now())
                .id(room_id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?;

            let room_id = room.id().to_string();
            self.authz.authorize(
                room.audience(),
                inreq.properties(),
                vec!["rooms", &room_id, "rtcs"],
                "create",
            )?;
        };

        // Creating a Real-Time Connection
        let object = {
            let conn = self.db.get()?;
            rtc::InsertQuery::new(room_id).execute(&conn)?
        };

        inreq
            .to_response(object, ResponseStatus::OK)
            .into_envelope()
            .map(|envelope| vec![Box::new(envelope)])
            .map_err(SvcError::from)
    }

    pub(crate) async fn connect(
        &self,
        inreq: ConnectRequest,
    ) -> Result<Vec<Box<OutgoingEnvelope>>, SvcError> {
        let id = inreq.payload().id;

        // Authorization
        {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .time(room::upto_now())
                .rtc_id(id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("a room for the rtc = '{}' is not found", &id))
                        .build()
                })?;

            if room.backend() != &room::RoomBackend::Janus {
                return Err(SvcError::builder()
                    .status(ResponseStatus::NOT_IMPLEMENTED)
                    .detail(&format!(
                        "'rtc.connect' is not implemented for the backend = '{}'.",
                        room.backend()
                    ))
                    .build());
            }

            let rtc_id = id.to_string();
            let room_id = room.id().to_string();
            self.authz.authorize(
                room.audience(),
                inreq.properties(),
                vec!["rooms", &room_id, "rtcs", &rtc_id],
                "read",
            )?;
        };

        // TODO: implement resource management
        // Picking up first available backend
        let backends = {
            let conn = self.db.get()?;
            janus_backend::ListQuery::new().limit(1).execute(&conn)?
        };
        let backend = backends.first().ok_or_else(|| {
            SvcError::builder()
                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                .detail("no available backends")
                .build()
        })?;

        // Building a Create Janus Gateway Handle request
        let backreq = crate::app::janus::create_rtc_handle_request(
            inreq.properties().clone(),
            Uuid::new_v4(),
            id,
            backend.session_id(),
            backend.id(),
        )
        .map_err(|_| {
            SvcError::builder()
                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                .detail("error creating a backend request")
                .build()
        })?;

        backreq
            .into_envelope()
            .map(|envelope| vec![Box::new(envelope)])
            .map_err(SvcError::from)
    }

    pub(crate) async fn read(
        &self,
        inreq: ReadRequest,
    ) -> Result<Vec<Box<OutgoingEnvelope>>, SvcError> {
        let id = inreq.payload().id;

        // Authorization
        {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .time(room::upto_now())
                .rtc_id(id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("a room for the rtc = '{}' is not found", &id))
                        .build()
                })?;

            let rtc_id = id.to_string();
            let room_id = room.id().to_string();
            self.authz.authorize(
                room.audience(),
                inreq.properties(),
                vec!["rooms", &room_id, "rtcs", &rtc_id],
                "read",
            )?;
        };

        // Returning Real-Time connection
        let object = {
            let conn = self.db.get()?;
            rtc::FindQuery::new()
                .id(id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the rtc = '{}' is not found", &id))
                        .build()
                })?
        };

        inreq
            .to_response(object, ResponseStatus::OK)
            .into_envelope()
            .map(|envelope| vec![Box::new(envelope)])
            .map_err(SvcError::from)
    }

    pub(crate) async fn list(
        &self,
        inreq: ListRequest,
    ) -> Result<Vec<Box<OutgoingEnvelope>>, SvcError> {
        let room_id = inreq.payload().room_id;

        // Authorization: room's owner has to allow the action
        {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .time(room::upto_now())
                .id(room_id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?;

            let room_id = room.id().to_string();
            self.authz.authorize(
                room.audience(),
                inreq.properties(),
                vec!["rooms", &room_id, "rtcs"],
                "list",
            )?;
        };

        // Looking up for Real-Time Connections
        let objects = {
            let conn = self.db.get()?;
            rtc::ListQuery::from((
                Some(room_id),
                inreq.payload().offset,
                Some(std::cmp::min(
                    inreq.payload().limit.unwrap_or_else(|| MAX_LIMIT),
                    MAX_LIMIT,
                )),
            ))
            .execute(&conn)?
        };

        inreq
            .to_response(objects, ResponseStatus::OK)
            .into_envelope()
            .map(|envelope| vec![Box::new(envelope)])
            .map_err(SvcError::from)
    }
}
