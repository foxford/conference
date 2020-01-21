use std::ops::Bound;

use chrono::{DateTime, Utc};
use failure::Error;
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        IncomingRequest, IntoPublishableDump, OutgoingEvent, OutgoingEventProperties,
        ResponseStatus, ShortTermTimingProperties, TrackingProperties,
    },
    AgentId,
};
use svc_authn::Authenticable;
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
use crate::db::{janus_backend, recording, room, rtc, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type VacuumRequest = IncomingRequest<VacuumRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct VacuumRequestData {}

#[derive(Debug, Serialize)]
pub(crate) struct RoomUploadEventData {
    id: Uuid,
    rtcs: Vec<RtcUploadEventData>,
}

#[derive(Debug, Serialize)]
struct RtcUploadEventData {
    id: Uuid,
    status: recording::Status,
    #[serde(
        serialize_with = "crate::serde::milliseconds_bound_tuples_option",
        skip_serializing_if = "Option::is_none"
    )]
    time: Option<Vec<(Bound<i64>, Bound<i64>)>>,
    #[serde(
        serialize_with = "crate::serde::ts_milliseconds_option",
        skip_serializing_if = "Option::is_none"
    )]
    started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
}

pub(crate) type RoomUploadEvent = OutgoingEvent<RoomUploadEventData>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    me: AgentId,
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(me: AgentId, authz: svc_authz::ClientMap, db: ConnectionPool) -> Self {
        Self { me, authz, db }
    }
}

impl State {
    pub(crate) async fn vacuum(
        &self,
        inreq: VacuumRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        // Authorization: only trusted subjects are allowed to perform operations with the system
        self.authz
            .authorize(
                self.me.as_account_id().audience(),
                inreq.properties(),
                vec!["system"],
                "update",
            )
            .await
            .map_err(|err| SvcError::from(err))?;

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
                    inreq.properties(),
                    backend.session_id(),
                    backend.handle_id(),
                    crate::app::janus::UploadStreamRequestBody::new(
                        rtc.id(),
                        &bucket_name(&room),
                        &record_name(&rtc),
                    ),
                    backend.id(),
                    &self.me,
                    start_timestamp,
                )
                .map_err(|_| {
                    // TODO: Send the error as an event to "app/${APP}/audiences/${AUD}" topic
                    SvcError::builder()
                        .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                        .detail("error creating a backend request")
                        .build()
                })?;

                requests.push(Box::new(backreq) as Box<dyn IntoPublishableDump>);
            }
        }

        requests.into()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn upload_event<I>(
    room: &room::Object,
    rtcs_and_recordings: I,
    start_timestamp: DateTime<Utc>,
    tracking: &TrackingProperties,
) -> Result<RoomUploadEvent, Error>
where
    I: Iterator<Item = (rtc::Object, recording::Object)>,
{
    let mut event_entries = Vec::new();
    for (rtc, recording) in rtcs_and_recordings {
        let uri = match recording.status() {
            recording::Status::Missing => None,
            recording::Status::Ready => {
                Some(format!("s3://{}/{}", bucket_name(&room), record_name(&rtc)))
            }
        };

        let entry = RtcUploadEventData {
            id: rtc.id(),
            status: recording.status().to_owned(),
            uri,
            time: recording.time().to_owned(),
            started_at: recording.started_at().to_owned(),
        };

        event_entries.push(entry);
    }

    let uri = format!("audiences/{}/events", room.audience());
    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let mut props = OutgoingEventProperties::new("room.upload", timing);
    props.set_tracking(tracking.to_owned());

    let event = RoomUploadEventData {
        id: room.id(),
        rtcs: event_entries,
    };

    Ok(OutgoingEvent::broadcast(event, props, &uri))
}

fn bucket_name(room: &room::Object) -> String {
    format!("origin.webinar.{}", room.audience())
}

fn record_name(rtc: &rtc::Object) -> String {
    format!("{}.source.mp4", rtc.id())
}

#[cfg(test)]
mod test {
    use std::ops::Try;

    use chrono::{Duration, Utc};
    use serde_json::json;
    use svc_authz::ClientMap;

    use crate::backend::janus::JANUS_API_VERSION;
    use crate::db::room;

    use crate::test_helpers::{
        agent::TestAgent,
        authz::TestAuthz,
        db::TestDb,
        factory::{insert_janus_backend, insert_rtc},
        Message, AUDIENCE,
    };

    use super::*;

    fn build_state(authz: ClientMap, db: &TestDb) -> State {
        let agent = TestAgent::new("alpha", "conference", AUDIENCE);
        State::new(
            agent.agent_id().to_owned(),
            authz,
            db.connection_pool().clone(),
        )
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct VacuumJanusRequest {
        janus: String,
        session_id: i64,
        handle_id: i64,
        body: VacuumJanusRequestBody,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct VacuumJanusRequestBody {
        method: String,
        id: Uuid,
        bucket: String,
        object: String,
    }

    #[test]
    fn vacuum_system() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            let (rtcs, backend) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    // Insert an rtc and janus backend.
                    let rtcs = vec![insert_rtc(&conn, AUDIENCE), insert_rtc(&conn, AUDIENCE)];
                    let _other_rtc = insert_rtc(&conn, AUDIENCE);
                    let backend = insert_janus_backend(&conn, AUDIENCE);

                    // Close rooms.
                    let start = Utc::now() - Duration::hours(2);
                    let finish = start + Duration::hours(1);
                    let time = (Bound::Included(start), Bound::Excluded(finish));

                    for rtc in rtcs.iter() {
                        room::UpdateQuery::new(rtc.room_id().to_owned())
                            .set_time(time)
                            .execute(&conn)
                            .unwrap();
                    }

                    (rtcs, backend)
                })
                .unwrap();

            // Allow cron to perform vacuum.
            let agent = TestAgent::new("alpha", "cron", AUDIENCE);
            authz.allow(agent.account_id(), vec!["system"], "update");

            // Make system.vacuum request.
            let state = build_state(authz.into(), &db);
            let payload = json!({});
            let request: VacuumRequest = agent.build_request("system.vacuum", &payload).unwrap();
            let result = state
                .vacuum(request, Utc::now())
                .await
                .into_result()
                .unwrap();
            assert_eq!(result.len(), 2);

            // Assert outgoing Janus stream.upload requests.
            for (publishable, rtc) in result.into_iter().zip(rtcs.iter()) {
                let message = Message::<VacuumJanusRequest>::from_publishable(publishable)
                    .expect("Failed to parse message");

                assert_eq!(message.properties().kind(), "request");

                assert_eq!(
                    message.topic(),
                    format!(
                        "agents/{}/api/{}/in/conference.{}",
                        backend.id(),
                        JANUS_API_VERSION,
                        AUDIENCE,
                    )
                );

                assert_eq!(
                    *message.payload(),
                    VacuumJanusRequest {
                        janus: "message".to_string(),
                        session_id: backend.session_id(),
                        handle_id: backend.handle_id(),
                        body: VacuumJanusRequestBody {
                            method: "stream.upload".to_string(),
                            id: rtc.id(),
                            bucket: format!("origin.webinar.{}", AUDIENCE).to_string(),
                            object: format!("{}.source.mp4", rtc.id()).to_string(),
                        }
                    }
                );
            }
        });
    }

    #[test]
    fn vacuum_system_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Make system.vacuum request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let state = build_state(authz.into(), &db);
            let payload = json!({});
            let request: VacuumRequest = agent.build_request("system.vacuum", &payload).unwrap();
            let result = state.vacuum(request, Utc::now()).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected system.vacuum to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        })
    }
}
