use std::ops::Bound;
use std::result::Result as StdResult;

use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, OutgoingEvent, OutgoingEventProperties, OutgoingMessage,
        ShortTermTimingProperties,
    },
    AgentId,
};
use svc_authn::Authenticable;
use uuid::Uuid;

use crate::config::UploadConfig;
use crate::db;
use crate::db::recording::{Object as Recording, Status as RecordingStatus};
use crate::db::room::Object as Room;
use crate::db::rtc::SharingPolicy;
use crate::{app::context::Context, backend::janus::client::upload_stream::UploadStreamRequest};
use crate::{
    app::endpoint::prelude::*, backend::janus::client::upload_stream::UploadStreamRequestBody,
};
use crate::{
    app::error::Error as AppError, backend::janus::client::upload_stream::UploadStreamTransaction,
};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct RoomUploadEventData {
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

pub(crate) type RoomUploadEvent = OutgoingMessage<RoomUploadEventData>;

////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize)]
struct ClosedRoomNotification {
    room_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub(crate) struct VacuumRequest {}

pub(crate) struct VacuumHandler;

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
        let conn = context.get_conn()?;
        let rooms = db::room::finished_with_in_progress_recordings(&conn)?;

        for (room, recording, backend) in rooms.into_iter() {
            db::agent::DeleteQuery::new()
                .room_id(room.id())
                .execute(&conn)?;

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

pub(crate) fn upload_event<C: Context, I>(
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

        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn vacuum_system() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                authz.set_audience(SVC_AUDIENCE);

                let (_rtcs, _backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert janus backend and rooms.
                        let backend = shared_helpers::insert_janus_backend(&conn);

                        let room1 = shared_helpers::insert_closed_room_with_backend_id(
                            &conn,
                            &backend.id(),
                        );

                        let room2 = shared_helpers::insert_closed_room_with_backend_id(
                            &conn,
                            &backend.id(),
                        );

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

                        (rtcs, backend)
                    })
                    .unwrap();

                // Allow cron to perform vacuum.
                let agent = TestAgent::new("alpha", "cron", SVC_AUDIENCE);
                authz.allow(agent.account_id(), vec!["system"], "update");

                // Make system.vacuum request.
                let mut context = TestContext::new(db, authz);
                let payload = VacuumRequest {};

                let messages = handle_request::<VacuumHandler>(&mut context, &agent, payload)
                    .await
                    .expect("System vacuum failed");

                assert!(messages.len() > 0);
            })
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
