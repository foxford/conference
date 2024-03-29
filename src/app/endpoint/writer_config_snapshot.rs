use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use axum::extract::{Extension, Path};

use serde::Deserialize;
use svc_agent::{mqtt::ResponseStatus, Authenticable};
use svc_utils::extractors::AgentIdExtractor;

use crate::app::endpoint::prelude::*;
use crate::app::{
    context::{AppContext, Context},
    service_utils::{RequestParams, Response},
};
use crate::db;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct ReadRequest {
    room_id: db::room::Id,
}

pub async fn read(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

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

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let account_id = reqp.as_account_id().to_owned();
        let service_audience = context.agent_id().as_account_id().to_owned();

        let mut conn = context.get_conn().await?;
        let room = helpers::find_room_by_id(
            payload.room_id,
            helpers::RoomTimeRequirement::Any,
            &mut conn,
        )
        .await?;

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

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

        let snapshots = db::rtc_writer_config_snapshot::ListWithRtcQuery::new(room.id())
            .execute(&mut conn)
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
            test_helpers::{db::TestDb, prelude::*},
        };

        use super::super::*;

        #[sqlx::test]
        async fn read(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);

            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
            let dispatcher = TestAgent::new("dispatcher-0", "dispatcher", SVC_AUDIENCE);

            let mut conn = db.get_conn().await;

            // Insert a room with RTCs and agent writer configs.
            let room = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((Bound::Included(Utc::now()), Bound::Unbounded))
                .rtc_sharing_policy(RtcSharingPolicy::Owned)
                .insert(&mut conn)
                .await;

            shared_helpers::insert_agent(&mut conn, agent1.agent_id(), room.id()).await;

            let rtc2 = factory::Rtc::new(room.id())
                .created_by(agent2.agent_id().to_owned())
                .insert(&mut conn)
                .await;

            factory::RtcWriterConfigSnaphost::new(&rtc2, Some(true), Some(true))
                .insert(&mut conn)
                .await;

            let rtc3 = factory::Rtc::new(room.id())
                .created_by(agent3.agent_id().to_owned())
                .insert(&mut conn)
                .await;

            factory::RtcWriterConfigSnaphost::new(&rtc3, Some(false), Some(false))
                .insert(&mut conn)
                .await;
            factory::RtcWriterConfigSnaphost::new(&rtc3, Some(true), None)
                .insert(&mut conn)
                .await;

            // Make agent_writer_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

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

        #[sqlx::test]
        async fn wrong_rtc_sharing_policy(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);

            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
            let dispatcher = TestAgent::new("dispatcher-0", "dispatcher", SVC_AUDIENCE);

            let mut conn = db.get_conn().await;

            // Insert a room with an agent.
            let room = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((Bound::Included(Utc::now()), Bound::Unbounded))
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .insert(&mut conn)
                .await;

            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

            // Make agent_writer_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &dispatcher, payload)
                .await
                .expect_err("Unexpected agent writer config update success");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_payload");
            Ok(())
        }

        #[sqlx::test]
        async fn missing_room(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);

            // Make agent_writer_config.read request.
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new()).await;

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
