use chrono::{DateTime, Utc};
use failure::{format_err, Error};
use serde_derive::{Deserialize, Serialize};
use svc_agent::mqtt::{
    compat::IntoEnvelope, IncomingRequest, OutgoingEvent, OutgoingEventProperties,
    OutgoingResponse, Publish,
};
use svc_agent::Addressable;
use svc_authn::{AccountId, Authenticable};
use uuid::Uuid;

use super::janus;
use crate::db::{janus_backend, recording, room, rtc, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type UploadRequest = IncomingRequest<UploadRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct UploadRequestData {}

#[derive(Debug, Serialize)]
pub(crate) struct UploadEventData {
    rtcs: Vec<UploadEventEntry>,
}

#[derive(Debug, Serialize)]
struct UploadEventEntry {
    id: Uuid,
    time: Vec<(i64, i64)>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    started_at: DateTime<Utc>,
    uri: String,
}

pub(crate) type ObjectResponse = OutgoingResponse<room::Object>;
pub(crate) type ObjectUploadEvent = OutgoingEvent<UploadEventData>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
    id: AccountId,
}

impl State {
    pub(crate) fn new(authz: svc_authz::ClientMap, db: ConnectionPool, id: AccountId) -> Self {
        Self { authz, db, id }
    }
}

impl State {
    pub(crate) fn upload(&self, inreq: &UploadRequest) -> Result<impl Publish, Error> {
        // TODO: Use 'local' authz mode instead
        if *inreq.properties().as_account_id() != self.id {
            return Err(format_err!(
                "Agent {} is not allowed to call system.upload method",
                inreq.properties().as_agent_id()
            ));
        }

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
                let req = janus::upload_stream_request(
                    inreq.properties().clone(),
                    backend.session_id(),
                    backend.handle_id(),
                    janus::UploadStreamRequestBody::new(
                        rtc.id(),
                        &bucket_name(&room),
                        &record_name(&rtc),
                    ),
                    backend.id(),
                )?;

                requests.push(req.into_envelope()?);
            }
        }

        Ok(requests)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn upload_event<I>(
    room: room::Object,
    rtc_and_recordings: I,
) -> Result<ObjectUploadEvent, Error>
where
    I: Iterator<Item = (rtc::Object, Vec<recording::Object>)>,
{
    use std::ops::Bound;

    let (started_at, _finished_at) = room.time();
    let started_at = match started_at {
        Bound::Excluded(started_at) | Bound::Included(started_at) => started_at,
        Bound::Unbounded => {
            return Err(format_err!(
                "unexpected Bound::Unbounded in room's 'started_at' value"
            ));
        }
    };

    let mut event_entries = Vec::new();

    for (rtc, recordings) in rtc_and_recordings {
        let time = recordings
            .into_iter()
            .flat_map(|r| {
                let (_rtc_id, time) = r.into_tuple();
                time
            })
            .map(|(start, end)| {
                let start = match start {
                    Bound::Included(start) | Bound::Excluded(start) => {
                        start.timestamp_millis() - started_at.timestamp_millis()
                    }
                    Bound::Unbounded => 0,
                };

                let end = match end {
                    Bound::Included(end) | Bound::Excluded(end) => {
                        end.timestamp_millis() - started_at.timestamp_millis()
                    }
                    Bound::Unbounded => 0,
                };

                (start, end)
            })
            .collect();

        let entry = UploadEventEntry {
            id: rtc.id(),
            uri: format!("s3://{}/{}", bucket_name(&room), record_name(&rtc)),
            time,
            started_at,
        };

        event_entries.push(entry);
    }

    let uri = format!("audiences/{}/events", room.audience());
    let event = UploadEventData {
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
