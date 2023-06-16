use crate::{
    app::{
        context::{AppContext, Context},
        endpoint::{helpers, prelude::AppErrorKind, RequestHandler, RequestResult},
        error::ErrorExt,
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
    },
    authz::AuthzObject,
    db,
};
use anyhow::anyhow;
use async_trait::async_trait;
use axum::{
    extract::{Path, Query},
    Extension,
};
use serde::Deserialize;
use std::sync::Arc;
use svc_agent::{mqtt::ResponseStatus, Addressable};
use svc_utils::extractors::AgentIdExtractor;

#[derive(Debug, Deserialize, Default)]
pub struct WithinGroup {
    within_group: bool,
}

#[derive(Deserialize)]
pub struct Payload {
    room_id: db::room::Id,
    within_group: bool,
}

pub async fn list(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    query: Option<Query<WithinGroup>>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let payload = Payload {
        room_id,
        within_group: query.unwrap_or_default().within_group,
    };

    Handler::handle(
        &mut ctx.start_message(),
        payload,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct Handler;

#[async_trait]
impl RequestHandler for Handler {
    type Payload = Payload;
    const ERROR_TITLE: &'static str = "Failed to get groups";

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let room_id = payload.room_id;
        let agent_id = reqp.as_agent_id().clone();

        let room = {
            let mut conn = context.get_conn().await?;
            helpers::find_room_by_id(room_id, helpers::RoomTimeRequirement::NotClosed, &mut conn)
                .await?
        };

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize classrooms.read on the tenant
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "read".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
            return Err(anyhow!(
                "Getting groups is only available for rooms with owned RTC sharing policy"
            ))
            .error(AppErrorKind::InvalidPayload)?;
        }

        let mut conn = context.get_conn().await?;
        let group_agent = db::group_agent::FindQuery::new(room_id)
            .execute(&mut conn)
            .await?;

        let mut groups = group_agent.groups();
        if payload.within_group {
            groups = groups.filter_by_agent(&agent_id);
        }

        context
            .metrics()
            .request_duration
            .group_list
            .observe_timestamp(context.start_timestamp());

        Ok(Response::new(
            ResponseStatus::OK,
            groups,
            context.start_timestamp(),
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            group_agent::{GroupItem, Groups},
            rtc::SharingPolicy as RtcSharingPolicy,
        },
        test_helpers::{
            db::TestDb,
            factory, find_response, handle_request,
            prelude::{TestAgent, TestAuthz, TestContext},
            shared_helpers, USR_AUDIENCE,
        },
    };
    use chrono::{Duration, Utc};
    use std::ops::Bound;
    use svc_agent::mqtt::ResponseStatus;

    #[tokio::test]
    async fn missing_room() -> std::io::Result<()> {
        let db = TestDb::new().await;
        let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
        let mut context = TestContext::new(db, TestAuthz::new()).await;

        let payload = Payload {
            room_id: db::room::Id::random(),
            within_group: false,
        };

        // Assert error.
        let err = handle_request::<Handler>(&mut context, &agent, payload)
            .await
            .expect_err("Unexpected group list success");

        assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
        assert_eq!(err.kind(), "room_not_found");
        Ok(())
    }

    #[tokio::test]
    async fn closed_room() -> std::io::Result<()> {
        let db = TestDb::new().await;
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

        let mut context = TestContext::new(db, TestAuthz::new()).await;

        let payload = Payload {
            room_id: room.id(),
            within_group: false,
        };

        // Assert error.
        let err = handle_request::<Handler>(&mut context, &agent, payload)
            .await
            .expect_err("Unexpected agent reader config read success");

        assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
        assert_eq!(err.kind(), "room_closed");
        Ok(())
    }

    #[tokio::test]
    async fn wrong_rtc_sharing_policy() {
        let db = TestDb::new().await;
        let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);

        let mut conn = db.get_conn().await;
        let room = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((Bound::Included(Utc::now()), Bound::Unbounded))
            .rtc_sharing_policy(RtcSharingPolicy::Shared)
            .insert(&mut conn)
            .await;

        // Allow agent to read the room.
        let mut authz = TestAuthz::new();
        let classroom_id = room.classroom_id().to_string();
        authz.allow(
            agent1.account_id(),
            vec!["classrooms", &classroom_id],
            "read",
        );

        let mut context = TestContext::new(db, authz).await;
        let payload = Payload {
            room_id: room.id(),
            within_group: false,
        };

        // Assert error.
        let err = handle_request::<Handler>(&mut context, &agent1, payload)
            .await
            .expect_err("Unexpected group list success");

        assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
        assert_eq!(err.kind(), "invalid_payload");
    }

    #[tokio::test]
    async fn list_agents_with_groups() {
        let db = TestDb::new().await;
        let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
        let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);

        let mut conn = db.get_conn().await;

        let room = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((Bound::Included(Utc::now()), Bound::Unbounded))
            .rtc_sharing_policy(RtcSharingPolicy::Owned)
            .insert(&mut conn)
            .await;

        factory::GroupAgent::new(
            room.id(),
            Groups::new(vec![
                GroupItem::new(0, vec![agent1.agent_id().clone()]),
                GroupItem::new(1, vec![agent2.agent_id().clone()]),
            ]),
        )
        .upsert(&mut conn)
        .await;

        // Allow agent to read the room.
        let mut authz = TestAuthz::new();
        let classroom_id = room.classroom_id().to_string();
        authz.allow(
            agent1.account_id(),
            vec!["classrooms", &classroom_id],
            "read",
        );

        let mut context = TestContext::new(db, authz).await;
        let payload = Payload {
            room_id: room.id(),
            within_group: false,
        };

        let messages = handle_request::<Handler>(&mut context, &agent1, payload)
            .await
            .expect("Group list failed");

        // Assert response.
        let (state, respp, _) = find_response::<Groups>(messages.as_slice());
        assert_eq!(respp.status(), ResponseStatus::OK);
        assert_eq!(state.len(), 2);
    }

    #[tokio::test]
    async fn list_agents_within_group() {
        let db = TestDb::new().await;
        let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
        let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);

        let mut conn = db.get_conn().await;

        let room = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((Bound::Included(Utc::now()), Bound::Unbounded))
            .rtc_sharing_policy(RtcSharingPolicy::Owned)
            .insert(&mut conn)
            .await;

        factory::GroupAgent::new(
            room.id(),
            Groups::new(vec![
                GroupItem::new(0, vec![agent1.agent_id().clone()]),
                GroupItem::new(1, vec![agent2.agent_id().clone()]),
            ]),
        )
        .upsert(&mut conn)
        .await;

        // Allow agent to read the room.
        let mut authz = TestAuthz::new();
        let classroom_id = room.classroom_id().to_string();
        authz.allow(
            agent1.account_id(),
            vec!["classrooms", &classroom_id],
            "read",
        );

        let mut context = TestContext::new(db, authz).await;
        let payload = Payload {
            room_id: room.id(),
            within_group: true,
        };

        let messages = handle_request::<Handler>(&mut context, &agent1, payload)
            .await
            .expect("Group list failed");

        let current_state =
            Groups::new(vec![GroupItem::new(0, vec![agent1.agent_id().to_owned()])]);

        // Assert response.
        let (state, respp, _) = find_response::<Groups>(messages.as_slice());
        assert_eq!(respp.status(), ResponseStatus::OK);
        assert_eq!(state, current_state);
    }
}
