use crate::{
    app::{
        context::Context,
        endpoint::prelude::*,
        error::Error as AppError,
        service_utils::{RequestParams, Response},
    },
    authz::AuthzObject,
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
use anyhow::{anyhow, Context as AnyhowContext};
use async_trait::async_trait;
use chrono::Utc;
use futures::stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{ops::Bound, result::Result as StdResult};
use svc_agent::{
    mqtt::{
        IncomingEventProperties, OutgoingEvent, OutgoingEventProperties, OutgoingMessage,
        ResponseStatus, ShortTermTimingProperties,
    },
    AgentId,
};
use svc_authn::Authenticable;

use tracing::error;
use tracing_attributes::instrument;

use super::MqttResult;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub struct RoomUploadEventData {
    id: db::room::Id,
    rtcs: Vec<RtcUploadEventData>,
}

#[derive(Debug, Serialize)]
struct RtcUploadEventData {
    id: db::rtc::Id,
    status: RecordingStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
    created_by: AgentId,
    mjr_dumps_uris: Option<Vec<String>>,
}

pub type RoomUploadEvent = OutgoingMessage<RoomUploadEventData>;

////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize)]
struct ClosedRoomNotification {
    room_id: db::room::Id,
}

#[derive(Debug, Deserialize)]
pub struct VacuumRequest {}

pub struct VacuumHandler;

#[async_trait]
impl RequestHandler for VacuumHandler {
    type Payload = VacuumRequest;
    const ERROR_TITLE: &'static str = "Failed to vacuum system";

    #[instrument(skip(context, _payload, reqp))]
    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        _payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        // Authorization: only trusted subjects are allowed to perform operations with the system
        let audience = context.agent_id().as_account_id().audience();

        context
            .authz()
            .authorize(
                audience.into(),
                reqp,
                AuthzObject::new(&["system"]).into(),
                "update".into(),
            )
            .await?;

        let mut response = Response::new(
            ResponseStatus::NO_CONTENT,
            json!({}),
            context.start_timestamp(),
            None,
        );

        let rooms = crate::util::spawn_blocking({
            let group = context.config().janus_group.clone();

            let conn = context.get_conn().await?;
            move || db::room::finished_with_in_progress_recordings(&conn, group.as_deref())
        })
        .await?;

        for (room, recording, backend) in rooms.into_iter() {
            crate::util::spawn_blocking({
                let room_id = room.id();

                let conn = context.get_conn().await?;
                move || {
                    db::agent::DeleteQuery::new()
                        .room_id(room_id)
                        .execute(&conn)
                }
            })
            .await?;

            let config = upload_config(context, &room)?;
            let request = UploadStreamRequest {
                body: UploadStreamRequestBody::new(
                    recording.rtc_id(),
                    &config.backend,
                    &config.bucket,
                ),
                handle_id: backend.handle_id(),
                session_id: backend.session_id(),
            };
            let transaction = UploadStreamTransaction {
                rtc_id: recording.rtc_id(),
                start_timestamp: context.start_timestamp(),
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
            response.add_notification(
                "room.close",
                &format!("rooms/{}/events", room.id()),
                room,
                context.start_timestamp(),
            );
        }

        Ok(response)
    }
}

#[derive(Debug, Deserialize)]
pub struct OrphanedRoomCloseEvent {}

pub struct OrphanedRoomCloseHandler;

#[async_trait]
impl EventHandler for OrphanedRoomCloseHandler {
    type Payload = OrphanedRoomCloseEvent;

    #[instrument(skip(context, _payload))]
    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        _payload: Self::Payload,
        evp: &IncomingEventProperties,
    ) -> MqttResult {
        let audience = context.agent_id().as_account_id().audience();
        // Authorization: only trusted subjects are allowed to perform operations with the system
        context
            .authz()
            .authorize(
                audience.into(),
                evp,
                AuthzObject::new(&["system"]).into(),
                "update".into(),
            )
            .await?;

        let load_till = Utc::now()
            - chrono::Duration::from_std(context.config().orphaned_room_timeout)
                .expect("Orphaned room timeout misconfigured");

        let timed_out = crate::util::spawn_blocking({
            let conn = context.get_conn().await?;
            move || db::orphaned_room::get_timed_out(load_till, &conn)
        })
        .await?;

        let mut closed_rooms = vec![];
        let mut notifications = vec![];

        {
            // to close this connection right after the loop
            let mut conn = context.get_conn_sqlx().await?;

            for (orphan, room) in timed_out {
                match room {
                    Some(room) if !room.is_closed() => {
                        let r = db::room::UpdateQuery::new(room.id())
                            .time(Some((room.time().0, Bound::Excluded(Utc::now()))))
                            .timed_out()
                            .execute(&mut conn)
                            .await;

                        match r {
                            Ok(room) => {
                                closed_rooms.push(room.id());
                                notifications.push(helpers::build_notification(
                                    "room.close",
                                    &format!("rooms/{}/events", room.id()),
                                    room.clone(),
                                    evp.tracking(),
                                    context.start_timestamp(),
                                ));
                                notifications.push(helpers::build_notification(
                                    "room.close",
                                    &format!("audiences/{}/events", room.audience()),
                                    room,
                                    evp.tracking(),
                                    context.start_timestamp(),
                                ));
                            }
                            Err(err) => {
                                error!(?err, "Closing room failed");
                            }
                        }
                    }

                    _ => {
                        closed_rooms.push(orphan.id);
                    }
                }
            }
        }

        crate::util::spawn_blocking({
            let conn = context.get_conn().await?;
            move || {
                if let Err(err) = db::orphaned_room::remove_rooms(&closed_rooms, &conn) {
                    error!(?err, "Error removing rooms fron orphan table");
                }
            }
        })
        .await;

        Ok(Box::new(stream::iter(notifications)))
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
                &upload_config(context, room)?.bucket,
                record_name(&recording, room)
            )),
        };

        let entry = RtcUploadEventData {
            id: recording.rtc_id(),
            status: recording.status().to_owned(),
            uri,
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
        .context("Missing upload configuration for the room's audience")
        .error(AppErrorKind::ConfigKeyMissing)
}

fn record_name(recording: &Recording, room: &Room) -> String {
    let prefix = match room.rtc_sharing_policy() {
        SharingPolicy::Owned => {
            format!("{}/", room.classroom_id())
        }
        _ => String::from(""),
    };

    format!("{}{}.source.webm", prefix, recording.rtc_id())
}

///////////////////////////////////////////////////////////////////////////////

mod agent_cleanup;
mod agent_connection_cleanup;

pub use agent_cleanup::Handler as AgentCleanupHandler;
pub use agent_connection_cleanup::Handler as AgentConnectionCleanupHandler;

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod orphaned {
        use chrono::Utc;

        use crate::{
            app::endpoint::system::{OrphanedRoomCloseEvent, OrphanedRoomCloseHandler},
            db,
            test_helpers::{
                authz::TestAuthz,
                context::TestContext,
                db::TestDb,
                db_sqlx, handle_event,
                prelude::{GlobalContext, TestAgent},
                shared_helpers,
                test_deps::LocalDeps,
                SVC_AUDIENCE,
            },
        };

        #[tokio::test]
        async fn close_orphaned_rooms() -> anyhow::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let mut authz = TestAuthz::new();
            authz.set_audience(SVC_AUDIENCE);

            let agent = TestAgent::new("alpha", "cron", SVC_AUDIENCE);
            authz.allow(agent.account_id(), vec!["system"], "update");

            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let connection = context.get_conn().await?;
            let opened_room = shared_helpers::insert_room(&connection);
            let opened_room2 = shared_helpers::insert_room(&connection);
            let closed_room = shared_helpers::insert_closed_room(&connection);
            db::orphaned_room::upsert_room(
                opened_room.id(),
                Utc::now() - chrono::Duration::seconds(10),
                &connection,
            )?;
            db::orphaned_room::upsert_room(
                closed_room.id(),
                Utc::now() - chrono::Duration::seconds(10),
                &connection,
            )?;
            db::orphaned_room::upsert_room(
                opened_room2.id(),
                Utc::now() + chrono::Duration::seconds(10),
                &connection,
            )?;

            let messages = handle_event::<OrphanedRoomCloseHandler>(
                &mut context,
                &agent,
                OrphanedRoomCloseEvent {},
            )
            .await
            .expect("System vacuum failed");

            let rooms: Vec<db::room::Object> =
                messages.into_iter().map(|ev| ev.payload()).collect();
            assert_eq!(rooms.len(), 2);
            assert!(rooms[0].timed_out());
            assert_eq!(rooms[0].id(), opened_room.id());
            let orphaned = db::orphaned_room::get_timed_out(
                Utc::now() + chrono::Duration::seconds(20),
                &connection,
            )?;
            assert_eq!(orphaned.len(), 1);
            assert_eq!(orphaned[0].0.id, opened_room2.id());
            Ok(())
        }
    }

    mod vacuum {
        use svc_agent::mqtt::ResponseStatus;

        use crate::{
            backend::janus::client::{
                events::EventResponse,
                transactions::{Transaction, TransactionKind},
                IncomingEvent,
            },
            test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn vacuum_system() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            authz.set_audience(SVC_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            // Insert janus backend and rooms.
            let backend =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            let conn = db.get_conn();
            let room1 = shared_helpers::insert_closed_room_with_backend_id(&conn, backend.id());

            let room2 = shared_helpers::insert_closed_room_with_backend_id(&conn, backend.id());
            // Insert rtcs.
            let rtcs = vec![
                shared_helpers::insert_rtc_with_room(&conn, &room1),
                shared_helpers::insert_rtc_with_room(&conn, &room2),
            ];

            let _other_rtc = shared_helpers::insert_rtc(&conn);

            // Insert active agents.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn_sqlx = db_sqlx.get_conn().await;

            for rtc in rtcs.iter() {
                shared_helpers::insert_agent(
                    &conn,
                    &mut conn_sqlx,
                    agent.agent_id(),
                    rtc.room_id(),
                )
                .await;
                shared_helpers::insert_recording(&conn, rtc);
            }

            let rtcs = rtcs.into_iter().map(|x| x.id()).collect::<Vec<_>>();

            // Allow cron to perform vacuum.
            let agent = TestAgent::new("alpha", "cron", SVC_AUDIENCE);
            authz.allow(agent.account_id(), vec!["system"], "update");

            // Make system.vacuum request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx.clone());
            let payload = VacuumRequest {};

            let messages = handle_request::<VacuumHandler>(&mut context, &agent, payload)
                .await
                .expect("System vacuum failed");
            rx.recv().await.unwrap();
            let recv_rtcs: Vec<db::rtc::Id> = [rx.recv().await.unwrap(), rx.recv().await.unwrap()]
                .iter()
                .map(|resp| match resp {
                    IncomingEvent::Event(EventResponse {
                        transaction:
                            Transaction {
                                kind:
                                    Some(TransactionKind::UploadStream(UploadStreamTransaction {
                                        rtc_id,
                                        start_timestamp: _start_timestamp,
                                    })),
                                ..
                            },
                        ..
                    }) => *rtc_id,
                    _ => panic!("Got wrong event"),
                })
                .collect();
            context.janus_clients().remove_client(&backend);
            assert!(!messages.is_empty());
            assert_eq!(recv_rtcs, rtcs);
        }

        #[tokio::test]
        async fn vacuum_system_unauthorized() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let mut authz = TestAuthz::new();
            authz.set_audience(SVC_AUDIENCE);

            // Make system.vacuum request.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let payload = VacuumRequest {};

            let err = handle_request::<VacuumHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on system vacuum");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }
    }
}
