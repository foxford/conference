use crate::{
    app::{
        context::{AppContext, GlobalContext},
        endpoint::{helpers, prelude::AppErrorKind, RequestResult},
        error::ErrorExt,
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
        stage::video_group::{save_create_intent, save_delete_intent, save_update_intent},
    },
    authz::AuthzObject,
    db::{self, group_agent::Groups},
};
use anyhow::{anyhow, Context};
use axum::{extract::Path, Extension, Json};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use svc_agent::mqtt::ResponseStatus;
use svc_utils::extractors::AgentIdExtractor;

#[derive(Deserialize)]
pub struct Payload {
    room_id: db::room::Id,
    groups: Groups,
}

pub async fn update(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    Json(groups): Json<Groups>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    Handler::handle(
        ctx,
        Payload { room_id, groups },
        RequestParams::Http {
            agent_id: &agent_id,
        },
        Utc::now(),
    )
    .await
}

pub struct Handler;

impl Handler {
    async fn handle(
        context: Arc<dyn GlobalContext + Send + Sync>,
        payload: Payload,
        reqp: RequestParams<'_>,
        start_timestamp: DateTime<Utc>,
    ) -> RequestResult {
        let Payload { room_id, groups } = payload;

        let room = {
            let mut conn = context.get_conn().await?;
            helpers::find_room_by_id(room_id, helpers::RoomTimeRequirement::NotClosed, &mut conn)
                .await?
        };

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize classrooms.update on the tenant
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "update".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
            return Err(anyhow!(
                "Updating groups is only available for rooms with owned RTC sharing policy"
            ))
            .error(AppErrorKind::InvalidPayload)?;
        }

        let backend_id = room
            .backend_id()
            .cloned()
            .context("backend not found")
            .error(AppErrorKind::BackendNotFound)?;

        let existed_groups = {
            let mut conn = context.get_conn().await?;
            db::group_agent::FindQuery::new(room.id())
                .execute(&mut conn)
                .await?
                .groups()
                .len()
        };

        if existed_groups == 1 {
            save_create_intent(context.clone(), room, backend_id).await?
        } else if existed_groups > 1 && groups.len() == 1 {
            save_delete_intent(context.clone(), room, backend_id).await?
        } else {
            save_update_intent(context.clone(), room, backend_id).await?
        };

        context
            .metrics()
            .request_duration
            .group_update
            .observe_timestamp(start_timestamp);

        Ok(Response::new(
            ResponseStatus::OK,
            json!({}),
            start_timestamp,
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::rtc::SharingPolicy as RtcSharingPolicy;
    use crate::test_helpers::{
        db::TestDb,
        factory,
        prelude::{TestAgent, TestAuthz, TestContext},
        shared_helpers, USR_AUDIENCE,
    };
    use chrono::{Duration, Utc};
    use std::ops::Bound;

    #[sqlx::test]
    async fn missing_room(pool: sqlx::PgPool) -> std::io::Result<()> {
        let db = TestDb::new(pool);
        let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
        let context = TestContext::new(db, TestAuthz::new()).await;

        let payload = Payload {
            room_id: db::room::Id::random(),
            groups: Groups::new(vec![]),
        };

        // Assert error.
        let reqp = RequestParams::Http {
            agent_id: &agent.agent_id(),
        };
        let err = Handler::handle(Arc::new(context), payload, reqp, Utc::now())
            .await
            .err()
            .expect("Unexpected group update success");

        assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
        assert_eq!(err.kind(), "room_not_found");
        Ok(())
    }

    #[sqlx::test]
    async fn closed_room(pool: sqlx::PgPool) -> std::io::Result<()> {
        let db = TestDb::new(pool);
        let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

        let mut conn = db.get_conn().await;

        let room = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((
                Bound::Included(Utc::now() - Duration::hours(2)),
                Bound::Excluded(Utc::now() - Duration::hours(1)),
            ))
            .rtc_sharing_policy(RtcSharingPolicy::Owned)
            .insert(&mut conn)
            .await;

        shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

        let context = TestContext::new(db, TestAuthz::new()).await;

        let payload = Payload {
            room_id: room.id(),
            groups: Groups::new(vec![]),
        };

        // Assert error.
        let reqp = RequestParams::Http {
            agent_id: &agent.agent_id(),
        };
        let err = Handler::handle(Arc::new(context), payload, reqp, Utc::now())
            .await
            .err()
            .expect("Unexpected group update success");

        assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
        assert_eq!(err.kind(), "room_closed");
        Ok(())
    }

    #[sqlx::test]
    async fn wrong_rtc_sharing_policy(pool: sqlx::PgPool) {
        let db = TestDb::new(pool);
        let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);

        let mut conn = db.get_conn().await;
        let room = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((Bound::Included(Utc::now()), Bound::Unbounded))
            .rtc_sharing_policy(RtcSharingPolicy::Shared)
            .insert(&mut conn)
            .await;

        // Allow agent to update the room.
        let mut authz = TestAuthz::new();
        let classroom_id = room.classroom_id().to_string();
        authz.allow(
            agent1.account_id(),
            vec!["classrooms", &classroom_id],
            "update",
        );

        let context = TestContext::new(db, authz).await;
        let payload = Payload {
            room_id: room.id(),
            groups: Groups::new(vec![]),
        };

        // Assert error.
        let reqp = RequestParams::Http {
            agent_id: &agent1.agent_id(),
        };
        let err = Handler::handle(Arc::new(context), payload, reqp, Utc::now())
            .await
            .err()
            .expect("Unexpected group update success");

        assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
        assert_eq!(err.kind(), "invalid_payload");
    }
}
