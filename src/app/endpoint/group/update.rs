use crate::app::context::{AppContext, Context};
use crate::app::endpoint::group::StateItem;
use crate::app::endpoint::prelude::AppError;
use crate::app::endpoint::{RequestHandler, RequestResult};
use crate::app::service_utils::{RequestParams, Response};
use crate::db;
use async_trait::async_trait;
use axum::extract::Path;
use axum::{Extension, Json};
use diesel::{Connection, Identifiable};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use svc_agent::mqtt::ResponseStatus;
use svc_utils::extractors::AgentIdExtractor;

#[derive(Deserialize)]
pub struct UpdatePayload {
    room_id: db::room::Id,
    groups: Vec<StateItem>,
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

    let request = UpdatePayload {
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
    type Payload = UpdatePayload;
    const ERROR_TITLE: &'static str = "Failed to update groups";

    // TODO: Add tests
    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        _reqp: RequestParams<'_>,
    ) -> RequestResult {
        let conn = context.get_conn().await?;

        crate::util::spawn_blocking({
            let group_agents = payload.groups;
            let room_id = payload.room_id;

            move || {
                conn.transaction::<_, AppError, _>(|| {
                    // Deletes all groups and groups agents in the room
                    db::group::DeleteQuery::new(room_id).execute(&conn)?;

                    // Creates groups
                    let numbers = group_agents.iter().map(|g| g.number).collect::<Vec<i32>>();
                    let groups = db::group::batch_insert(&conn, room_id, numbers)?
                        .iter()
                        .map(|g| (g.number(), *g.id()))
                        .collect::<HashMap<_, _>>();

                    let agents = group_agents.into_iter().fold(Vec::new(), |mut vec, g| {
                        if let Some(group_id) = groups.get(&g.number) {
                            vec.push((*group_id, g.agents));
                        }

                        vec
                    });

                    // Creates group agents
                    db::group_agent::batch_insert(&conn, agents)?;

                    // Update reader_configs
                    // group_reader_config::update(&conn, state);

                    Ok(())
                })?;

                Ok::<_, AppError>(())
            }
        })
        .await?;

        let mut response = Response::new(
            ResponseStatus::OK,
            json!({}),
            context.start_timestamp(),
            None,
        );

        response.add_notification(
            "group.update",
            &format!("rooms/{}/events", payload.room_id),
            json!({}),
            context.start_timestamp(),
        );

        Ok(response)
    }
}
