use crate::app::context::{AppContext, Context};
use crate::app::endpoint::group::StateItem;
use crate::app::endpoint::prelude::{AppError, AppErrorKind};
use crate::app::endpoint::{helpers, RequestHandler, RequestResult};
use crate::app::error::ErrorExt;
use crate::app::group_reader_config;
use crate::app::service_utils::{RequestParams, Response};
use crate::backend::janus::client::update_agent_reader_config::{
    UpdateReaderConfigRequest, UpdateReaderConfigRequestBody,
    UpdateReaderConfigRequestBodyConfigItem,
};
use crate::db;
use anyhow::{anyhow, Context as AnyhowContext};
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

pub async fn update(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    Json(groups): Json<Vec<StateItem>>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = UpdatePayload { room_id, groups };

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
        _: RequestParams<'_>,
    ) -> RequestResult {
        let conn = context.get_conn().await?;

        let (room, configs, maybe_backend) = crate::util::spawn_blocking({
            let group_agents = payload.groups;
            let room = helpers::find_room_by_id(
                payload.room_id,
                helpers::RoomTimeRequirement::NotClosed,
                &conn,
            )?;

            if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
                return Err(anyhow!(
                    "Updating groups is only available for rooms with owned RTC sharing policy"
                ))
                .error(AppErrorKind::InvalidPayload)?;
            }

            move || {
                let configs = conn.transaction(|| {
                    // Deletes all groups and groups agents in the room
                    db::group::DeleteQuery::new(room.id()).execute(&conn)?;

                    // Creates groups
                    let numbers = group_agents.iter().map(|g| g.number).collect::<Vec<i32>>();
                    let groups = db::group::batch_insert(&conn, room.id(), numbers)?
                        .iter()
                        .map(|g| (g.number(), *g.id()))
                        .collect::<HashMap<_, _>>();

                    let agents = group_agents.into_iter().fold(Vec::new(), |mut acc, g| {
                        if let Some(group_id) = groups.get(&g.number) {
                            acc.push((*group_id, g.agents));
                        }

                        acc
                    });

                    // Creates group agents
                    db::group_agent::batch_insert(&conn, agents)?;

                    // Update rtc_reader_configs
                    let configs = group_reader_config::update(&conn, room.id())?;

                    Ok::<_, AppError>(configs)
                })?;

                // Find backend and send updates to it if present.
                let maybe_backend = match room.backend_id() {
                    None => None,
                    Some(backend_id) => db::janus_backend::FindQuery::new()
                        .id(backend_id)
                        .execute(&conn)?,
                };

                Ok::<_, AppError>((room, configs, maybe_backend))
            }
        })
        .await?;

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // TODO: Need refactoring
        if let Some(backend) = maybe_backend {
            let items = configs
                .iter()
                .map(|cfg| UpdateReaderConfigRequestBodyConfigItem {
                    reader_id: cfg.agent_id.to_owned(),
                    stream_id: cfg.rtc_id,
                    receive_video: cfg.availability,
                    receive_audio: cfg.availability,
                })
                .collect();

            let request = UpdateReaderConfigRequest {
                session_id: backend.session_id(),
                handle_id: backend.handle_id(),
                body: UpdateReaderConfigRequestBody::new(items),
            };
            context
                .janus_clients()
                .get_or_insert(&backend)
                .error(AppErrorKind::BackendClientCreationFailed)?
                .reader_update(request)
                .await
                .context("Reader update")
                .error(AppErrorKind::BackendRequestFailed)?
        }

        let mut response = Response::new(
            ResponseStatus::OK,
            json!({}),
            context.start_timestamp(),
            None,
        );

        response.add_notification(
            "group.update",
            &format!("rooms/{}/events", room.id()),
            json!({}),
            context.start_timestamp(),
        );

        Ok(response)
    }
}
