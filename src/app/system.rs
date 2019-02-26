use chrono::{DateTime, Utc};
use failure::{format_err, Error};
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        compat::IntoEnvelope, IncomingRequest, OutgoingEvent, OutgoingEventProperties,
        OutgoingResponse, Publish,
    },
    Addressable, AgentId,
};
use svc_authn::{AccountId, Authenticable};
use uuid::Uuid;

use super::janus;
use crate::db::{recording, room, rtc, ConnectionPool};

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
    #[serde(serialize_with = "crate::serde::ts_seconds_option")]
    started_at: Option<DateTime<Utc>>,
    uri: String,
}

pub(crate) type ObjectResponse = OutgoingResponse<room::Object>;
pub(crate) type ObjectUploadEvent = OutgoingEvent<UploadEventData>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
    id: AccountId,
    session_id: Option<i64>,
    handle_id: Option<i64>,
    backend_id: AgentId,
}

impl State {
    pub(crate) fn new(
        authz: svc_authz::ClientMap,
        db: ConnectionPool,
        id: AccountId,
        backend_id: AgentId,
    ) -> Self {
        Self {
            authz,
            db,
            id,
            backend_id,
            session_id: None,
            handle_id: None,
        }
    }

    pub(crate) fn set_session_id(&mut self, session_id: i64) {
        self.session_id = Some(session_id);
    }

    pub(crate) fn set_handle_id(&mut self, handle_id: i64) {
        self.handle_id = Some(handle_id);
    }
}

impl State {
    pub(crate) fn upload(&self, inreq: &UploadRequest) -> Result<impl Publish, Error> {
        let session_id = self
            .session_id
            .ok_or_else(|| format_err!("a system session is not ready yet"))?;

        let handle_id = self
            .handle_id
            .ok_or_else(|| format_err!("a system handle is not ready yet"))?;

        if *inreq.properties().as_account_id() != self.id {
            return Err(format_err!(
                "Agent {} is not allowed to call system.upload method",
                inreq.properties().as_agent_id()
            ));
        }

        let conn = self.db.get()?;

        // Retrieve all the finished rooms without recordings.
        let rooms = room::finished_without_recordings(&conn)?;

        let mut requests = Vec::new();

        for (room, rtc) in rooms.into_iter() {
            let req = janus::upload_stream_request(
                inreq.properties().clone(),
                session_id,
                handle_id,
                janus::UploadStreamRequestBody::new(
                    rtc.id(),
                    &bucket_name(&room),
                    &record_name(&rtc),
                ),
                &self.backend_id,
            )?;

            requests.push(req.into_envelope()?);
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

    let started_at = match room.started_at() {
        Bound::Excluded(started_at) | Bound::Included(started_at) => Some(started_at),
        Bound::Unbounded => {
            return Err(format_err!(
                "unexpected Bound::Unbounded in room's 'started_at' value"
            ));
        }
    };

    let mut event_entries = Vec::new();

    for (rtc, recordings) in rtc_and_recordings {
        let time = match started_at {
            Some(started_at) => recordings
                .into_iter()
                .flat_map(|r| {
                    let (_rtc_id, time) = r.decompose();
                    time
                })
                .map(|(start, end)| {
                    let start = match start {
                        Bound::Included(start) | Bound::Excluded(start) => {
                            start.timestamp() - started_at.timestamp()
                        }
                        Bound::Unbounded => 0,
                    };

                    let end = match end {
                        Bound::Included(end) | Bound::Excluded(end) => {
                            end.timestamp() - started_at.timestamp()
                        }
                        Bound::Unbounded => 0,
                    };

                    (start, end)
                })
                .collect(),
            None => Vec::new(),
        };

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
