use crate::app::context::{AppContext, Context};
use crate::app::endpoint::prelude::AppError;
use crate::app::endpoint::{RequestHandler, RequestResult};
use crate::app::service_utils::{RequestParams, Response};
use crate::db;
use async_trait::async_trait;
use axum::extract::Path;
use axum::{Extension, Json};
use diesel::{Connection, Identifiable};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use svc_agent::mqtt::ResponseStatus;
use svc_agent::AgentId;
use svc_utils::extractors::AgentIdExtractor;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct State {
    room_id: db::room::Id,
    groups: Vec<StateItem>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StateItem {
    number: i32,
    agents: Vec<AgentId>,
}

#[derive(Debug, Deserialize)]
pub struct StateGroups(Vec<StateItem>);

pub async fn update(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    Json(groups): Json<StateGroups>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = State {
        room_id,
        groups: groups.0,
    };
    UpdateHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct UpdateHandler;

#[async_trait]
impl RequestHandler for UpdateHandler {
    type Payload = State;
    const ERROR_TITLE: &'static str = "Failed to update groups";

    // Plan to do:
    // 1. Create groups (on conflict - do nothing)
    // 2. Create/update group_agent (on conflict - update group_id)

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        _reqp: RequestParams<'_>,
    ) -> RequestResult {
        let conn = context.get_conn().await?;

        crate::util::spawn_blocking({
            let groups = payload.groups;
            let room_id = payload.room_id;

            move || {
                conn.transaction::<_, AppError, _>(|| {
                    for g in groups {
                        let group = db::group::InsertQuery::new(room_id)
                            .number(g.number)
                            .execute(&conn)?;

                        for agent in g.agents {
                            db::group_agent::InsertQuery::new(*group.id(), &agent)
                                .execute(&conn)?;
                        }
                    }

                    Ok(())
                })?;

                Ok::<_, AppError>(())
            }
        })
        .await?;

        let mut response = Response::new(ResponseStatus::OK, "", context.start_timestamp(), None);

        response.add_notification(
            "group.update",
            &format!("rooms/{}/events", payload.room_id),
            "",
            context.start_timestamp(),
        );

        Ok(response)
    }
}
