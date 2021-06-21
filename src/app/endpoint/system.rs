use anyhow::anyhow;
use async_std::{stream, task};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{ops::Bound, result::Result as StdResult};
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, OutgoingEvent, OutgoingEventProperties, OutgoingMessage,
        ShortTermTimingProperties,
    },
    AgentId,
};
use svc_authn::Authenticable;
use uuid::Uuid;

use crate::{
    app::{context::Context, endpoint::prelude::*, error::Error as AppError},
    backend::janus::client::upload_stream::{
        UploadStreamRequest, UploadStreamRequestBody, UploadStreamTransaction,
    },
    config::UploadConfig,
    db,
    db::{
        recording::{Object as Recording, Status as RecordingStatus},
        room::Object as Room,
        rtc::SharingPolicy,
    },
};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub struct RoomUploadEventData {
    id: Uuid,
    rtcs: Vec<RtcUploadEventData>,
}

#[derive(Debug, Serialize)]
struct RtcUploadEventData {
    id: Uuid,
    status: RecordingStatus,
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
    created_by: AgentId,
    mjr_dumps_uris: Option<Vec<String>>,
}

pub type RoomUploadEvent = OutgoingMessage<RoomUploadEventData>;

////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize)]
struct ClosedRoomNotification {
    room_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct VacuumRequest {}

pub struct VacuumHandler;

#[async_trait]
impl RequestHandler for VacuumHandler {
    type Payload = VacuumRequest;
    const ERROR_TITLE: &'static str = "Failed to vacuum system";

    async fn handle<C: Context>(
        context: &mut C,
        _payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        // Authorization: only trusted subjects are allowed to perform operations with the system
        let audience = context.agent_id().as_account_id().audience();

        context
            .authz()
            .authorize(audience, reqp, vec!["system"], "update")
            .await?;

        let mut requests = Vec::new();
        let conn = context.get_conn().await?;
        let rooms =
            task::spawn_blocking(move || db::room::finished_with_in_progress_recordings(&conn))
                .await?;

        for (room, recording, backend) in rooms.into_iter() {
            let conn = context.get_conn().await?;
            let room_id = room.id();
            task::spawn_blocking(move || {
                db::agent::DeleteQuery::new()
                    .room_id(room_id)
                    .execute(&conn)
            })
            .await?;

            let config = upload_config(context, &room)?;
            let request = UploadStreamRequest {
                body: UploadStreamRequestBody::new(
                    recording.rtc_id(),
                    &config.backend,
                    &config.bucket,
                    &record_name(&recording, &room),
                ),
                handle_id: backend.handle_id(),
                session_id: backend.session_id(),
            };
            let transaction = UploadStreamTransaction {
                rtc_id: recording.rtc_id(),
            };
            // TODO: Send the error as an event to "app/${APP}/audiences/${AUD}" topic
            context
                .janus_clients()
                .get_or_insert(&backend)
                .error(AppErrorKind::BackendClientCreationFailed)?
                .upload_stream(request, transaction)
                .await
                .error(AppErrorKind::BackendRequestFailed)?;

            // Publish room closed notification
            let closed_notification = helpers::build_notification(
                "room.close",
                &format!("rooms/{}/events", room.id()),
                room,
                reqp,
                context.start_timestamp(),
            );

            requests.push(closed_notification);
        }

        Ok(Box::new(stream::from_iter(requests)))
    }
}

////////////////////////////////////////////////////////////////////////////////

pub fn upload_event<C: Context, I>(
    context: &C,
    room: &db::room::Object,
    recordings: I,
) -> StdResult<RoomUploadEvent, AppError>
where
    I: Iterator<Item = (db::recording::Object, db::rtc::Object)>,
{
    let mut event_entries = Vec::new();

    for (recording, rtc) in recordings {
        let uri = match recording.status() {
            RecordingStatus::InProgress => {
                let err = anyhow!(
                    "Unexpected recording in in_progress status, rtc_id = '{}'",
                    recording.rtc_id(),
                );

                return Err(err).error(AppErrorKind::MessageBuildingFailed)?;
            }
            RecordingStatus::Missing => None,
            RecordingStatus::Ready => Some(format!(
                "s3://{}/{}",
                &upload_config(context, &room)?.bucket,
                record_name(&recording, &room)
            )),
        };

        let entry = RtcUploadEventData {
            id: recording.rtc_id(),
            status: recording.status().to_owned(),
            uri,
            segments: recording.segments().to_owned(),
            started_at: recording.started_at().to_owned(),
            created_by: rtc.created_by().to_owned(),
            mjr_dumps_uris: recording.mjr_dumps_uris().cloned(),
        };

        event_entries.push(entry);
    }

    let uri = format!("audiences/{}/events", room.audience());
    let timing = ShortTermTimingProperties::until_now(context.start_timestamp());
    let props = OutgoingEventProperties::new("room.upload", timing);

    let event = RoomUploadEventData {
        id: room.id(),
        rtcs: event_entries,
    };

    Ok(OutgoingEvent::broadcast(event, props, &uri))
}

fn upload_config<'a, C: Context>(
    context: &'a C,
    room: &Room,
) -> StdResult<&'a UploadConfig, AppError> {
    let configs = &context.config().upload;

    let config = match room.rtc_sharing_policy() {
        SharingPolicy::Shared => &configs.shared,
        SharingPolicy::Owned => &configs.owned,
        SharingPolicy::None => {
            let err = anyhow!("Uploading not available for rooms with 'none' RTC sharing policy");
            return Err(err).error(AppErrorKind::NotImplemented);
        }
    };

    config
        .get(room.audience())
        .ok_or_else(|| anyhow!("Missing upload configuration for the room's audience"))
        .error(AppErrorKind::ConfigKeyMissing)
}

fn record_name(recording: &Recording, room: &Room) -> String {
    let prefix = match room.rtc_sharing_policy() {
        SharingPolicy::Owned => {
            if let Some(classroom_id) = room.classroom_id() {
                format!("{}/", classroom_id)
            } else {
                String::from("")
            }
        }
        _ => String::from(""),
    };

    format!("{}{}.source.webm", prefix, recording.rtc_id())
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod vacuum {
        use svc_agent::mqtt::ResponseStatus;

        use crate::{
            backend::janus::client::{
                events::EventResponse, transactions::Transaction, IncomingEvent,
            },
            test_helpers::{prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[async_std::test]
        async fn vacuum_system() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            authz.set_audience(SVC_AUDIENCE);

            let (rtcs, backend) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    // Insert janus backend and rooms.
                    let backend = shared_helpers::insert_janus_backend(
                        &conn, &janus.url, session_id, handle_id,
                    );

                    let room1 =
                        shared_helpers::insert_closed_room_with_backend_id(&conn, &backend.id());

                    let room2 =
                        shared_helpers::insert_closed_room_with_backend_id(&conn, &backend.id());

                    // Insert rtcs.
                    let rtcs = vec![
                        shared_helpers::insert_rtc_with_room(&conn, &room1),
                        shared_helpers::insert_rtc_with_room(&conn, &room2),
                    ];

                    let _other_rtc = shared_helpers::insert_rtc(&conn);

                    // Insert active agents.
                    let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                    for rtc in rtcs.iter() {
                        shared_helpers::insert_agent(&conn, agent.agent_id(), rtc.room_id());
                        shared_helpers::insert_recording(&conn, rtc);
                    }

                    (
                        rtcs.into_iter().map(|x| x.id()).collect::<Vec<_>>(),
                        backend,
                    )
                })
                .unwrap();

            // Allow cron to perform vacuum.
            let agent = TestAgent::new("alpha", "cron", SVC_AUDIENCE);
            authz.allow(agent.account_id(), vec!["system"], "update");

            // Make system.vacuum request.
            let mut context = TestContext::new(db, authz);
            let (tx, rx) = async_std::channel::unbounded();
            context.with_janus(tx.clone());

            let payload = VacuumRequest {};

            let messages = handle_request::<VacuumHandler>(&mut context, &agent, payload)
                .await
                .expect("System vacuum failed");
            context.janus_clients().remove_client(backend.id());
            let recv_rtcs: Vec<Uuid> = [rx.recv().await.unwrap(), rx.recv().await.unwrap()]
                .iter()
                .map(|resp| match resp {
                    IncomingEvent::Event(EventResponse {
                        transaction: Transaction::UploadStream(UploadStreamTransaction { rtc_id }),
                        ..
                    }) => *rtc_id,
                    _ => panic!("Got wrong event"),
                })
                .collect();

            assert!(tx.is_empty());
            assert!(messages.len() > 0);
            assert_eq!(recv_rtcs, rtcs);
        }

        #[test]
        fn vacuum_system_unauthorized() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                authz.set_audience(SVC_AUDIENCE);

                // Make system.vacuum request.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(db, authz);
                let payload = VacuumRequest {};

                let err = handle_request::<VacuumHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on system vacuum");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            })
        }
    }
}
