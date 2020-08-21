use std::ops::Bound;

use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::mqtt::{
    IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
    OutgoingMessage, ResponseStatus, ShortTermTimingProperties, TrackingProperties,
};
use svc_authn::Authenticable;
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::janus;
use crate::db;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct RoomUploadEventData {
    id: Uuid,
    rtcs: Vec<RtcUploadEventData>,
}

#[derive(Debug, Serialize)]
struct RtcUploadEventData {
    id: Uuid,
    status: db::recording::Status,
    #[serde(
        serialize_with = "crate::serde::milliseconds_bound_tuples_option",
        skip_serializing_if = "Option::is_none"
    )]
    segments: Option<Vec<(Bound<i64>, Bound<i64>)>>,
    #[serde(
        serialize_with = "crate::serde::ts_milliseconds_option",
        skip_serializing_if = "Option::is_none"
    )]
    started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
}

pub(crate) type RoomUploadEvent = OutgoingMessage<RoomUploadEventData>;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct VacuumRequest {}

pub(crate) struct VacuumHandler;

#[async_trait]
impl RequestHandler for VacuumHandler {
    type Payload = VacuumRequest;
    const ERROR_TITLE: &'static str = "Failed to vacuum system";

    async fn handle<C: Context>(
        context: &C,
        _payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        // Authorization: only trusted subjects are allowed to perform operations with the system
        let audience = context.agent_id().as_account_id().audience();

        context
            .authz()
            .authorize(audience, reqp, vec!["system"], "update")
            .await?;

        let mut requests = Vec::new();
        let conn = context.db().get()?;
        let rooms = db::room::finished_without_recordings(&conn)?;

        for (room, rtc, backend) in rooms.into_iter() {
            db::agent::DeleteQuery::new()
                .room_id(room.id())
                .execute(&conn)?;

            // TODO: Send the error as an event to "app/${APP}/audiences/${AUD}" topic
            let backreq = janus::upload_stream_request(
                reqp,
                backend.session_id(),
                backend.handle_id(),
                janus::UploadStreamRequestBody::new(
                    rtc.id(),
                    &bucket_name(&room),
                    &record_name(&rtc),
                ),
                backend.id(),
                context.agent_id(),
                start_timestamp,
            )
            .map_err(|err| format!("error creating a backend request: {}", err))
            .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

            requests.push(Box::new(backreq) as Box<dyn IntoPublishableMessage + Send>);
        }

        Ok(Box::new(stream::from_iter(requests)))
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn upload_event<I>(
    room: &db::room::Object,
    rtcs_and_recordings: I,
    start_timestamp: DateTime<Utc>,
    tracking: &TrackingProperties,
) -> anyhow::Result<RoomUploadEvent>
where
    I: Iterator<Item = (db::rtc::Object, db::recording::Object)>,
{
    let mut event_entries = Vec::new();
    for (rtc, recording) in rtcs_and_recordings {
        let uri = match recording.status() {
            db::recording::Status::Missing => None,
            db::recording::Status::Ready => {
                Some(format!("s3://{}/{}", bucket_name(&room), record_name(&rtc)))
            }
        };

        let entry = RtcUploadEventData {
            id: rtc.id(),
            status: recording.status().to_owned(),
            uri,
            segments: recording.segments().to_owned(),
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

fn bucket_name(room: &db::room::Object) -> String {
    format!("origin.webinar.{}", room.audience())
}

fn record_name(rtc: &db::rtc::Object) -> String {
    format!("{}.source.webm", rtc.id())
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod vacuum {
        use chrono::{Duration, Utc};
        use diesel::prelude::*;

        use crate::backend::janus::JANUS_API_VERSION;
        use crate::db;
        use crate::test_helpers::outgoing_envelope::OutgoingEnvelopeProperties;
        use crate::test_helpers::prelude::*;

        use super::super::*;

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
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                authz.set_audience(SVC_AUDIENCE);

                let (rtcs, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert an rtc and janus backend.
                        let rtcs = vec![
                            shared_helpers::insert_rtc(&conn),
                            shared_helpers::insert_rtc(&conn),
                        ];
                        let _other_rtc = shared_helpers::insert_rtc(&conn);
                        let backend = shared_helpers::insert_janus_backend(&conn);

                        // Insert active agents and close rooms.
                        let start = Utc::now() - Duration::hours(2);
                        let finish = start + Duration::hours(1);
                        let time = (Bound::Included(start), Bound::Excluded(finish));
                        let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                        for rtc in rtcs.iter() {
                            shared_helpers::insert_agent(&conn, agent.agent_id(), rtc.room_id());

                            db::room::UpdateQuery::new(rtc.room_id().to_owned())
                                .time(time)
                                .execute(&conn)
                                .unwrap();
                        }

                        (rtcs, backend)
                    })
                    .unwrap();

                // Allow cron to perform vacuum.
                let agent = TestAgent::new("alpha", "cron", SVC_AUDIENCE);
                authz.allow(agent.account_id(), vec!["system"], "update");

                // Make system.vacuum request.
                let context = TestContext::new(TestDb::new(), authz);
                let payload = VacuumRequest {};

                let messages = handle_request::<VacuumHandler>(&context, &agent, payload)
                    .await
                    .expect("System vacuum failed");

                let conn = context.db().get().unwrap();

                for (message, rtc) in messages.into_iter().zip(rtcs.iter()) {
                    // Assert outgoing Janus stream.upload requests.
                    match message.properties() {
                        OutgoingEnvelopeProperties::Request(_) => (),
                        _ => panic!("Expected outgoing request"),
                    }

                    assert_eq!(
                        message.topic(),
                        format!(
                            "agents/{}/api/{}/in/conference.{}",
                            backend.id(),
                            JANUS_API_VERSION,
                            SVC_AUDIENCE,
                        )
                    );

                    assert_eq!(
                        message.payload::<VacuumJanusRequest>(),
                        VacuumJanusRequest {
                            janus: "message".to_string(),
                            session_id: backend.session_id(),
                            handle_id: backend.handle_id(),
                            body: VacuumJanusRequestBody {
                                method: "stream.upload".to_string(),
                                id: rtc.id(),
                                bucket: format!("origin.webinar.{}", USR_AUDIENCE).to_string(),
                                object: format!("{}.source.webm", rtc.id()).to_string(),
                            }
                        }
                    );

                    // Assert deleted active agents.
                    let query = crate::schema::agent::table
                        .filter(crate::schema::agent::room_id.eq(rtc.room_id()));

                    assert_eq!(query.execute(&conn).unwrap(), 0);
                }
            });
        }

        #[test]
        fn vacuum_system_unauthorized() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                authz.set_audience(SVC_AUDIENCE);

                // Make system.vacuum request.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(db, authz);
                let payload = VacuumRequest {};

                let err = handle_request::<VacuumHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on system vacuum");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            })
        }
    }
}
