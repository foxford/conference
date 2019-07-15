use std::ops::Bound;

use chrono::{DateTime, Utc};
use failure::Error;
use serde_derive::{Deserialize, Serialize};
use svc_agent::mqtt::{
    compat::IntoEnvelope, IncomingRequest, OutgoingEvent, OutgoingEventProperties, Publish,
    ResponseStatus,
};
use svc_authn::AccountId;
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::db::{janus_backend, recording, room, rtc, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type UploadRequest = IncomingRequest<UploadRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct UploadRequestData {}

#[derive(Debug, Serialize)]
pub(crate) struct RoomUploadEventData {
    id: Uuid,
    rtcs: Vec<RtcUploadEventData>,
}

#[derive(Debug, Serialize)]
struct RtcUploadEventData {
    id: Uuid,
    #[serde(serialize_with = "crate::serde::milliseconds_bound_tuples")]
    time: Vec<(Bound<i64>, Bound<i64>)>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    started_at: DateTime<Utc>,
    uri: String,
}

pub(crate) type RoomUploadEvent = OutgoingEvent<RoomUploadEventData>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    me: AccountId,
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(me: AccountId, authz: svc_authz::ClientMap, db: ConnectionPool) -> Self {
        Self { me, authz, db }
    }
}

impl State {
    pub(crate) async fn vacuum(&self, inreq: UploadRequest) -> Result<impl Publish, SvcError> {
        // Authorization: only trusted subjects are allowed to perform operations with the system
        self.authz.authorize(
            self.me.audience(),
            inreq.properties(),
            vec!["system"],
            "update",
        )?;

        // TODO: Update 'finished_without_recordings' in order to return (backend,room,rtc)
        let backends = {
            let conn = self.db.get()?;
            janus_backend::ListQuery::new().execute(&conn)?
        };

        let mut requests = Vec::new();
        for backend in backends {
            // Retrieve all the finished rooms without recordings.
            let rooms = {
                let conn = self.db.get()?;
                room::finished_without_recordings(&conn)?
            };

            for (room, rtc) in rooms.into_iter() {
                let backreq = crate::app::janus::upload_stream_request(
                    backend.session_id(),
                    backend.handle_id(),
                    crate::app::janus::UploadStreamRequestBody::new(
                        rtc.id(),
                        &bucket_name(&room),
                        &record_name(&rtc),
                    ),
                    backend.id(),
                )
                .map_err(|_| {
                    // TODO: Send the error as an event to "app/${APP}/audiences/${AUD}" topic
                    SvcError::builder()
                        .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                        .detail("error creating a backend request")
                        .build()
                })?;
                requests.push(backreq.into_envelope().map_err(SvcError::from)?);
            }
        }

        Ok(requests)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn upload_event<I>(
    room: &room::Object,
    rtcs_and_recordings: I,
) -> Result<RoomUploadEvent, Error>
where
    I: Iterator<Item = (rtc::Object, recording::Object)>,
{
    let mut event_entries = Vec::new();
    for (rtc, recording) in rtcs_and_recordings {
        let entry = RtcUploadEventData {
            id: rtc.id(),
            uri: format!("s3://{}/{}", bucket_name(&room), record_name(&rtc)),
            time: recording.time().to_owned(),
            started_at: recording.started_at().to_owned(),
        };

        event_entries.push(entry);
    }

    let uri = format!("audiences/{}/events", room.audience());
    let event = RoomUploadEventData {
        id: room.id(),
        rtcs: event_entries,
    };

    Ok(OutgoingEvent::broadcast(
        event,
        OutgoingEventProperties::new("room.upload"),
        &uri,
    ))
}

fn bucket_name(room: &room::Object) -> String {
    format!("origin.webinar.{}", room.audience())
}

fn record_name(rtc: &rtc::Object) -> String {
    format!("{}.source.mp4", rtc.id())
}
