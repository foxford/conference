use std::ops::Bound;

use chrono::{DateTime, Utc};
use failure::{format_err, Error};
use itertools::izip;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use crate::app::janus;
use crate::authz;
use crate::db::{janus_handle_shadow, janus_session_shadow, recording, room, rtc, ConnectionPool};
use crate::transport::mqtt::{
    compat::IntoEnvelope, IncomingRequest, OutgoingEvent, OutgoingEventProperties,
    OutgoingResponse, OutgoingResponseStatus, Publish, Publishable,
};
use crate::transport::Destination;

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
    time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    audience: String,
}

pub(crate) type UploadRequest = IncomingRequest<UploadRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct UploadRequestData {}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UploadEventData {
    id: Uuid,
    uri: String,
}

pub(crate) type ObjectResponse = OutgoingResponse<room::Object>;
pub(crate) type ObjectUploadEvent = OutgoingEvent<UploadEventData>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    authz: authz::ClientMap,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(authz: authz::ClientMap, db: ConnectionPool) -> Self {
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

        let resp = inreq.to_response(object, &OutgoingResponseStatus::OK);
        resp.into_envelope()
    }

    pub(crate) fn upload(&self, inreq: &UploadRequest) -> Result<impl Publish, Error> {
        use diesel::prelude::*;

        let conn = self.db.get()?;

        let rooms = room::ListQuery::new().finished(true).execute(&conn)?;
        let rtcs: Vec<rtc::Object> = rtc::Object::belonging_to(&rooms).load(&conn)?;
        let recordings: Vec<recording::Object> =
            recording::Object::belonging_to(&rtcs).load(&conn)?;
        let sessions: Vec<janus_session_shadow::Object> =
            janus_session_shadow::Object::belonging_to(&rtcs).load(&conn)?;
        let handles: Vec<janus_handle_shadow::Object> =
            janus_handle_shadow::Object::belonging_to(&rtcs).load(&conn)?;

        let recordings = recordings.grouped_by(&rtcs);
        let sessions = sessions.grouped_by(&rtcs);
        let handles = handles.grouped_by(&rtcs);
        let rtcs_and_related = rtcs
            .into_iter()
            .zip(izip!(recordings, sessions, handles))
            .grouped_by(&rooms);

        let mut requests = Vec::new();

        for (room, rtcs_and_related) in rooms.into_iter().zip(rtcs_and_related) {
            for (rtc, (recording, session, handle)) in rtcs_and_related {
                if !recording.is_empty() {
                    continue;
                }

                let session = session.into_iter().next().ok_or_else(|| {
                    format_err!("a session for rtc = '{}' is not found", &rtc.id())
                })?;

                let handle = handle.into_iter().next().ok_or_else(|| {
                    format_err!("a handle for rtc = '{}' is not found", &rtc.id())
                })?;

                let req = janus::upload_stream_request(
                    inreq.properties().clone(),
                    session.session_id(),
                    handle.handle_id(),
                    janus::UploadStreamRequestBody::new(
                        *rtc.id(),
                        &bucket_name(&room),
                        &record_name(&rtc),
                    ),
                    session.location_id().clone(),
                )?;

                requests.push(req.into_envelope()?);
            }
        }

        Ok(requests)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn upload_event(rtc: rtc::Object, room: room::Object) -> ObjectUploadEvent {
    let uri = format!("audiences/{}/events", room.audience());
    let event = UploadEventData {
        id: *rtc.id(),
        uri: format!("s3://{}/{}", bucket_name(&room), record_name(&rtc)),
    };

    OutgoingEvent::new(
        event,
        OutgoingEventProperties::new("rtc.store"),
        Destination::Broadcast(uri),
    )
}

fn bucket_name(room: &room::Object) -> String {
    format!("origin.webinar.{}", room.audience())
}

fn record_name(rtc: &rtc::Object) -> String {
    format!("{}.source.mp4", rtc.id())
}
