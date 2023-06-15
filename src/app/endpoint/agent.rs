use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Extension, Path, Query};

use serde::Deserialize;
use svc_agent::mqtt::ResponseStatus;
use svc_utils::extractors::AgentIdExtractor;

use crate::{
    app::{
        context::{AppContext, Context},
        endpoint::prelude::*,
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
    },
    authz::AuthzObject,
    db,
};

///////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 25;

#[derive(Debug, Deserialize)]
pub struct ListRequest {
    room_id: db::room::Id,
    offset: Option<i64>,
    limit: Option<i64>,
}

#[derive(Deserialize, Clone, Copy)]
pub struct Pagination {
    offset: i64,
    limit: i64,
}

pub async fn list(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    query: Option<Query<Pagination>>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = ListRequest {
        room_id,
        offset: query.map(|x| x.offset),
        limit: query.map(|x| x.limit),
    };
    ListHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct ListHandler;

#[async_trait]
impl RequestHandler for ListHandler {
    type Payload = ListRequest;
    const ERROR_TITLE: &'static str = "Failed to list agents";

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let room = {
            let mut conn = context.get_conn_sqlx().await?;
            helpers::find_room_by_id(
                payload.room_id,
                helpers::RoomTimeRequirement::Open,
                &mut conn,
            )
            .await?
        };

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize agents listing in the room.
        let object = AuthzObject::new(&["classrooms", &room.classroom_id().to_string()]);

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object.into(), "read".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        // Get agents list in the room.
        let mut conn = context.get_conn_sqlx().await?;
        let agents = db::agent::ListQuery::new()
            .room_id(payload.room_id)
            .offset(payload.offset.unwrap_or(0))
            .limit(std::cmp::min(payload.limit.unwrap_or(MAX_LIMIT), MAX_LIMIT))
            .execute(&mut conn)
            .await?;

        context
            .metrics()
            .request_duration
            .agent_list
            .observe_timestamp(context.start_timestamp());

        // Respond with agents list.
        Ok(Response::new(
            ResponseStatus::OK,
            agents,
            context.start_timestamp(),
            Some(authz_time),
        ))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod list {
        use serde::Deserialize;
        use svc_agent::AgentId;

        use crate::test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps};

        use super::super::*;

        ///////////////////////////////////////////////////////////////////////////

        #[derive(Deserialize)]
        struct Agent {
            agent_id: AgentId,
            room_id: db::room::Id,
        }

        #[tokio::test]
        async fn list_agents() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;

            // Create room and put the agent online.
            let room = shared_helpers::insert_room(&mut conn).await;
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

            // Allow agent to list agents in the room.
            let mut authz = TestAuthz::new();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &room.classroom_id().to_string()],
                "read",
            );

            // Make agent.list request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;

            let payload = ListRequest {
                room_id: room.id(),
                offset: None,
                limit: None,
            };

            let messages = handle_request::<ListHandler>(&mut context, &agent, payload)
                .await
                .expect("Agents listing failed");

            // Assert response.
            let (agents, respp, _) = find_response::<Vec<Agent>>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(agents.len(), 1);
            assert_eq!(&agents[0].agent_id, agent.agent_id());
            assert_eq!(agents[0].room_id, room.id());
        }

        #[tokio::test]
        async fn list_agents_not_authorized() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db_sqlx.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = ListRequest {
                room_id: room.id(),
                offset: None,
                limit: None,
            };

            let err = handle_request::<ListHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on agents listing");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }

        #[tokio::test]
        async fn list_agents_closed_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db_sqlx.get_conn().await;
                // Create closed room.
                shared_helpers::insert_closed_room(&mut conn).await
            };

            // Allow agent to list agents in the room.
            let mut authz = TestAuthz::new();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &room.classroom_id().to_string()],
                "read",
            );

            // Make agent.list request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;

            let payload = ListRequest {
                room_id: room.id(),
                offset: None,
                limit: None,
            };

            let err = handle_request::<ListHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on agents listing");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
        }

        #[tokio::test]
        async fn list_agents_missing_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = ListRequest {
                room_id: db::room::Id::random(),
                offset: None,
                limit: None,
            };

            let err = handle_request::<ListHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on agents listing");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }
    }
}
