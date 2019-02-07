use crate::app::janus;
use crate::authn::{AgentId, Authenticable};
use crate::db::{janus_handle_shadow, janus_session_shadow, location, room, rtc, ConnectionPool};
use crate::transport::mqtt::compat::IntoEnvelope;
use crate::transport::mqtt::{
    IncomingRequest, OutgoingEvent, OutgoingEventProperties, OutgoingResponse,
    OutgoingResponseStatus, Publishable,
};
use crate::transport::Destination;
use failure::{format_err, Error};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

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

pub(crate) type StoreRequest = IncomingRequest<StoreRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct StoreRequestData {}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct StoreEventData {
    id: Uuid,
    uri: String,
}

pub(crate) type ObjectResponse = OutgoingResponse<rtc::Object>;
pub(crate) type ObjectListResponse = OutgoingResponse<Vec<rtc::Object>>;
pub(crate) type ObjectUpdateEvent = OutgoingEvent<rtc::Object>;
pub(crate) type ObjectStoreEvent = OutgoingEvent<StoreEventData>;

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
        let object = rtc::InsertQuery::new(&inreq.payload().room_id).execute(&conn)?;

        // Building a Create Janus Session request
        let to = self.backend_agent_id.clone();
        let backreq =
            janus::create_session_request(inreq.properties().clone(), object.id().clone(), to)?;

        backreq.into_envelope()
    }

    pub(crate) fn read(&self, inreq: &ReadRequest) -> Result<impl Publishable, Error> {
        let id = inreq.payload().id;

        // Looking up for Janus Handle
        let conn = self.db.get()?;
        let maybe_location =
            location::FindQuery::new(&inreq.properties().agent_id(), &id).execute(&conn)?;

        match maybe_location {
            Some(_) => {
                // Returning Real-Time connection
                let object = rtc::FindQuery::new()
                    .id(&id)
                    .one(&conn)?
                    .ok_or_else(|| format_err!("the rtc = '{}' is not found", &id))?;
                let resp = inreq.to_response(object, &OutgoingResponseStatus::OK);
                resp.into_envelope()
            }
            None => {
                // Looking up for Janus Gateway Session
                let session = janus_session_shadow::FindQuery::new()
                    .rtc_id(&id)
                    .execute(&conn)?
                    .ok_or_else(|| format_err!("a session for rtc = '{}' is not found", &id))?;

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
        let objects = rtc::ListQuery::from_options(
            Some(&inreq.payload().room_id),
            inreq.payload().offset,
            Some(std::cmp::min(
                inreq.payload().limit.unwrap_or_else(|| MAX_LIMIT),
                MAX_LIMIT,
            )),
        )
        .execute(&conn)?;

        let resp = inreq.to_response(objects, &OutgoingResponseStatus::OK);
        resp.into_envelope()
    }

    pub(crate) fn store(&self, inreq: &StoreRequest) -> Result<impl Publishable, Error> {
        use diesel::BelongingToDsl;

        let conn = self.db.get()?;

        let rooms_to_process = room::FindQuery::new().finished(true).many(&conn)?;
        // TODO: why belonging_to is not working?
        // TODO: filter here?
        let rtcs = rtc::Object::belonging_to(&rooms_to_process).load(&conn);
        let rtcs = rtcs.grouped_by(&rooms_to_process);

        let mut backreq = None;

        for (room, rtcs) in rooms_to_process.into_iter().zip(rtcs) {
            for rtc in rtcs.iter().filter(|rtc| !rtc.stored) {
                // TODO: optimize if there will be such a need

                let session = janus_session_shadow::FindQuery::new()
                    .rtc_id(&rtc.id())
                    .execute(&conn)?
                    .ok_or_else(|| {
                        format_err!("a session for rtc = '{}' is not found", &rtc.id())
                    })?;

                let handle = janus_handle_shadow::FindQuery::new()
                    .rtc_id(&rtc.id())
                    .one(&conn)?
                    .ok_or_else(|| {
                        format_err!("a handle for rtc = '{}' is not found", &rtc.id())
                    })?;

                backreq = Some(janus::upload_stream_request(
                    inreq.properties().clone(),
                    session.session_id(),
                    handle.handle_id(),
                    janus::UploadStreamRequestBody::new(
                        *rtc.id(),
                        room.bucket_name(),
                        rtc.record_name(),
                    ),
                    session.location_id().clone(),
                )?);
            }
        }

        backreq
            .ok_or_else(|| format_err!("no rtcs need to be stored"))?
            .into_envelope()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn update_event(object: rtc::Object) -> ObjectUpdateEvent {
    let uri = format!("rooms/{}/events", object.room_id());
    OutgoingEvent::new(
        object,
        OutgoingEventProperties::new("rtc.update"),
        Destination::Broadcast(uri),
    )
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn store_event(rtc: rtc::Object, room: room::Object) -> ObjectStoreEvent {
    let uri = format!("audiences/{}", room.audience());
    let event = StoreEventData {
        id: *rtc.id(),
        uri: format!("s3://{}/{}", room.bucket_name(), rtc.record_name()),
    };

    OutgoingEvent::new(
        event,
        OutgoingEventProperties::new("rtc.store"),
        Destination::Broadcast(uri),
    )
}
