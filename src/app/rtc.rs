use failure::{format_err, Error};
use serde_derive::Deserialize;
use uuid::Uuid;

use crate::app::janus;
use crate::db::{janus_session_shadow, location, room, rtc, ConnectionPool};
use crate::transport::util::mqtt::compat::IntoEnvelope;
use crate::transport::util::mqtt::{
    IncomingRequest, OutgoingEvent, OutgoingEventProperties, OutgoingResponse,
    OutgoingResponseStatus, Publishable,
};
use crate::transport::util::AgentId;

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

pub(crate) type ObjectResponse = OutgoingResponse<rtc::Object>;
pub(crate) type ObjectListResponse = OutgoingResponse<Vec<rtc::Object>>;
pub(crate) type ObjectUpdateEvent = OutgoingEvent<rtc::Object>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    authz: authz::ClientMap,
    db: ConnectionPool,
    // TODO: replace with backend agent registry
    backend_agent_id: AgentId,
}

impl State {
    pub(crate) fn new(
        authz: authz::ClientMap,
        db: ConnectionPool,
        backend_agent_id: AgentId,
    ) -> Self {
        Self {
            authz,
            db,
            backend_agent_id,
        }
    }
}

impl State {
    pub(crate) fn create(&self, inreq: &CreateRequest) -> Result<impl Publishable, Error> {
        let room_id = &inreq.payload().room_id;

        // Authorization: room's owner has to allow the action
        {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .id(room_id)
                .execute(&conn)?
                .ok_or_else(|| format_err!("the room = '{}' is not found", &room_id))?;

            let room_id = room.id().to_string();
            self.authz.authorize(
                room.audience(),
                inreq.properties(),
                vec!["rooms", &room_id, "rtcs"],
                "create",
            )?;
        };

        // Creating a Real-Time Connection
        let rtc = {
            let conn = self.db.get()?;
            rtc::InsertQuery::new(room_id).execute(&conn)?
        };

        // Building a Create Janus Gateway Session request
        let to = self.backend_agent_id.clone();
        let backreq =
            janus::create_session_request(inreq.properties().clone(), rtc.id().clone(), to)?;

        backreq.into_envelope()
    }

    pub(crate) fn read(&self, inreq: &ReadRequest) -> Result<impl Publishable, Error> {
        let agent_id = AgentId::from(inreq.properties());
        let id = inreq.payload().id;

        // Authorization: room's owner has to allow the action
        let authorize = |audience: &str, room_id: &Uuid| -> Result<(), Error> {
            let room_id = room_id.to_string();
            let rtc_id = id.to_string();
            self.authz.authorize(
                audience,
                inreq.properties(),
                vec!["rooms", &room_id, "rtcs", &rtc_id],
                "read",
            )
        };

        // Looking up for Janus Gateway Handle
        let maybe_location = {
            let conn = self.db.get()?;
            location::FindQuery::new(&agent_id, &id).execute(&conn)?
        };

        match maybe_location {
            Some(ref loc) => {
                // Authorization
                authorize(loc.audience(), loc.room_id())?;

                // Returning Real-Time connection
                let rtc = {
                    let conn = self.db.get()?;
                    rtc::FindQuery::new(&id)
                        .execute(&conn)?
                        .ok_or_else(|| format_err!("the rtc = '{}' is not found", &id))?
                };
                let resp = inreq.to_response(rtc, &OutgoingResponseStatus::OK);
                resp.into_envelope()
            }
            None => {
                // Authorization
                {
                    let conn = self.db.get()?;
                    let room = room::FindQuery::new()
                        .rtc_id(&id)
                        .execute(&conn)?
                        .ok_or_else(|| format_err!("a room for rtc = '{}' is not found", &id))?;

                    authorize(room.audience(), room.id())?;
                };

                // Looking up for Janus Gateway Session
                let session = {
                    let conn = self.db.get()?;
                    janus_session_shadow::FindQuery::new()
                        .rtc_id(&id)
                        .execute(&conn)?
                        .ok_or_else(|| format_err!("a session for rtc = '{}' is not found", &id))?
                };

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
        let room_id = &inreq.payload().room_id;

        // Authorization: room's owner has to allow the action
        {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .id(room_id)
                .execute(&conn)?
                .ok_or_else(|| format_err!("the room = '{}' is not found", &room_id))?;

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
            rtc::ListQuery::from_options(
                Some(room_id),
                inreq.payload().offset,
                Some(std::cmp::min(
                    inreq.payload().limit.unwrap_or_else(|| MAX_LIMIT),
                    MAX_LIMIT,
                )),
            )
            .execute(&conn)?
        };

        let resp = inreq.to_response(objects, &OutgoingResponseStatus::OK);
        resp.into_envelope()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn update_event(object: rtc::Object) -> ObjectUpdateEvent {
    let uri = format!("rooms/{}/events", object.room_id());
    OutgoingEvent::broadcast(object, OutgoingEventProperties::new("rtc.update"), &uri)
}
