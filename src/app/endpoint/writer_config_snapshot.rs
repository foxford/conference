use anyhow::anyhow;
use async_trait::async_trait;
use axum::extract::{Extension, Path};
use futures::stream;
use serde::Deserialize;
use svc_agent::{
    mqtt::{IncomingRequestProperties, ResponseStatus},
    Authenticable,
};

use crate::app::{
    context::{AppContext, Context},
    service_utils::{RequestParams, Response},
};
use crate::app::{endpoint::prelude::*, http::AuthExtractor};
use crate::db;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct ReadRequest {
    room_id: db::room::Id,
}

pub async fn read(
    Extension(ctx): Extension<AppContext>,
    AuthExtractor(agent_id): AuthExtractor,
    Path(room_id): Path<db::room::Id>,
) -> RequestResult {
    let request = ReadRequest { room_id };
    ReadHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct ReadHandler;

#[async_trait]
impl RequestHandler for ReadHandler {
    type Payload = ReadRequest;
    const ERROR_TITLE: &'static str = "Failed to read writer config snapshots";

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let conn = context.get_conn().await?;

        let account_id = reqp.as_account_id().to_owned();
        let service_audience = context.agent_id().as_account_id().to_owned();

        let snapshots = crate::util::spawn_blocking(move || {
            let room = helpers::find_room_by_id(
                payload.room_id,
                helpers::RoomTimeRequirement::Any,
                &conn,
            )?;

            if account_id.label() != "dispatcher"
                || account_id.audience() != service_audience.audience()
            {
                return Err(anyhow!(
                    "Agent writer config is available only to dispatcher"
                ))
                .error(AppErrorKind::AccessDenied)?;
            }

            if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
                return Err(anyhow!(
                    "Agent writer config is available only for rooms with owned RTC sharing policy"
                ))
                .error(AppErrorKind::InvalidPayload)?;
            }

            let snapshots =
                db::rtc_writer_config_snapshot::ListWithRtcQuery::new(room.id()).execute(&conn)?;
            Ok::<_, AppError>(snapshots)
        })
        .await?;

        Ok(Response::new(
            ResponseStatus::OK,
            snapshots,
            context.start_timestamp(),
            None,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod read {
        use std::ops::Bound;

        use chrono::Utc;

        use crate::{
            db::rtc::SharingPolicy as RtcSharingPolicy,
            test_helpers::{prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn read() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
            let dispatcher = TestAgent::new("dispatcher-0", "dispatcher", SVC_AUDIENCE);

            // Insert a room with RTCs and agent writer configs.
            let (room, rtc2, rtc3) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((Bound::Included(Utc::now()), Bound::Unbounded))
                        .rtc_sharing_policy(RtcSharingPolicy::Owned)
                        .insert(&conn);

                    shared_helpers::insert_agent(&conn, agent1.agent_id(), room.id());

                    let rtc2 = factory::Rtc::new(room.id())
                        .created_by(agent2.agent_id().to_owned())
                        .insert(&conn);

                    factory::RtcWriterConfigSnaphost::new(&rtc2, Some(true), Some(true))
                        .insert(&conn);

                    let rtc3 = factory::Rtc::new(room.id())
                        .created_by(agent3.agent_id().to_owned())
                        .insert(&conn);

                    factory::RtcWriterConfigSnaphost::new(&rtc3, Some(false), Some(false))
                        .insert(&conn);
                    factory::RtcWriterConfigSnaphost::new(&rtc3, Some(true), None).insert(&conn);

                    (room, rtc2, rtc3)
                })
                .unwrap();

            // Make agent_writer_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = ReadRequest { room_id: room.id() };

            let messages = handle_request::<ReadHandler>(&mut context, &dispatcher, payload)
                .await
                .expect("Agent writer config read failed");

            // Assert response.
            let (vec, respp, _) = find_response::<Vec<crate::db::rtc_writer_config_snapshot::Object>>(
                messages.as_slice(),
            );
            assert_eq!(respp.status(), ResponseStatus::OK);

            assert_eq!(vec[0].rtc_id(), rtc2.id());
            assert_eq!(vec[0].send_video(), Some(true));
            assert_eq!(vec[0].send_audio(), Some(true));

            assert_eq!(vec[1].rtc_id(), rtc3.id());
            assert_eq!(vec[1].send_video(), Some(false));
            assert_eq!(vec[1].send_audio(), Some(false));

            assert_eq!(vec[2].rtc_id(), rtc3.id());
            assert_eq!(vec[2].send_video(), Some(true));
            assert_eq!(vec[2].send_audio(), None);

            Ok(())
        }

        #[tokio::test]
        async fn wrong_rtc_sharing_policy() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
            let dispatcher = TestAgent::new("dispatcher-0", "dispatcher", SVC_AUDIENCE);

            // Insert a room with an agent.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((Bound::Included(Utc::now()), Bound::Unbounded))
                        .rtc_sharing_policy(RtcSharingPolicy::Shared)
                        .insert(&conn);

                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());

                    room
                })
                .unwrap();

            // Make agent_writer_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &dispatcher, payload)
                .await
                .expect_err("Unexpected agent writer config update success");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_payload");
            Ok(())
        }

        #[tokio::test]
        async fn missing_room() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            // Make agent_writer_config.read request.
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = ReadRequest {
                room_id: db::room::Id::random(),
            };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent writer config read success");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
            Ok(())
        }
    }
}
