use failure::{format_err, Error};
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        compat::IntoEnvelope, IncomingRequest, OutgoingEvent, OutgoingEventProperties,
        OutgoingResponse, Publish,
    },
    Addressable,
};
use svc_authn::{AccountId, Authenticable};
use uuid::Uuid;

use super::janus;
use crate::db::{janus_session_shadow, room, rtc, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

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
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
    id: AccountId,
    session_id: Option<i64>,
    handle_id: Option<i64>,
}

impl State {
    pub(crate) fn new(authz: svc_authz::ClientMap, db: ConnectionPool, id: AccountId) -> Self {
        Self {
            authz,
            db,
            id,
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
        use diesel::prelude::{BelongingToDsl, GroupedBy, RunQueryDsl};

        if *inreq.properties().as_account_id() != self.id {
            return Err(format_err!(
                "Agent {} is not allowed to call system.upload method",
                inreq.properties().as_agent_id()
            ));
        }

        let conn = self.db.get()?;

        let rooms = room::ListQuery::new()
            .finished(true)
            .with_records(false)
            .execute(&conn)?;
        let rtcs: Vec<rtc::Object> = rtc::Object::belonging_to(&rooms).load(&conn)?;
        let sessions: Vec<janus_session_shadow::Object> =
            janus_session_shadow::Object::belonging_to(&rtcs).load(&conn)?;

        let sessions = sessions.grouped_by(&rtcs);
        let rtcs_and_related = rtcs.into_iter().zip(sessions).grouped_by(&rooms);

        let mut requests = Vec::new();

        for (room, rtcs_and_related) in rooms.into_iter().zip(rtcs_and_related) {
            for (rtc, session) in rtcs_and_related {
                let session_id = self
                    .session_id
                    .ok_or_else(|| format_err!("a system session is not ready yet"))?;

                let handle_id = self
                    .handle_id
                    .ok_or_else(|| format_err!("a system handle is not ready yet"))?;

                let session = session
                    .iter()
                    .next()
                    .ok_or_else(|| format_err!("a session for rtc = {} is not found", rtc.id()))?;

                let req = janus::upload_stream_request(
                    inreq.properties().clone(),
                    session_id,
                    handle_id,
                    janus::UploadStreamRequestBody::new(
                        rtc.id(),
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
        id: rtc.id(),
        uri: format!("s3://{}/{}", bucket_name(&room), record_name(&rtc)),
    };

    OutgoingEvent::broadcast(event, OutgoingEventProperties::new("rtc.store"), &uri)
}

fn bucket_name(room: &room::Object) -> String {
    format!("origin.webinar.{}", room.audience())
}

fn record_name(rtc: &rtc::Object) -> String {
    format!("{}.source.mp4", rtc.id())
}
