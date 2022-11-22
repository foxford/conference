use crate::app::context::{AppContext, Context};
use crate::app::endpoint::group::State;
use crate::app::endpoint::prelude::{AppError, AppErrorKind};
use crate::app::endpoint::{helpers, RequestHandler, RequestResult};
use crate::app::error::ErrorExt;
use crate::app::service_utils::{RequestParams, Response};
use crate::db;
use anyhow::anyhow;
use async_trait::async_trait;
use axum::extract::{Path, Query};
use axum::Extension;
use serde::Deserialize;
use std::sync::Arc;
use svc_agent::mqtt::ResponseStatus;
use svc_agent::Addressable;
use svc_utils::extractors::AgentIdExtractor;

#[derive(Debug, Deserialize, Default)]
pub struct WithinGroup {
    within_group: bool,
}

#[derive(Deserialize)]
pub struct ListPayload {
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

    let payload = ListPayload {
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
    type Payload = ListPayload;
    const ERROR_TITLE: &'static str = "Failed to get groups";

    // TODO: Add tests for API
    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let conn = context.get_conn().await?;
        let agent_id = reqp.as_agent_id().clone();

        let groups = crate::util::spawn_blocking(move || {
            let room = helpers::find_room_by_id(
                payload.room_id,
                helpers::RoomTimeRequirement::NotClosed,
                &conn,
            )?;

            if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
                return Err(anyhow!(
                    "Getting groups is only available for rooms with owned RTC sharing policy"
                ))
                .error(AppErrorKind::InvalidPayload)?;
            }

            let mut q = db::group_agent::ListWithGroupQuery::new(payload.room_id);
            if payload.within_group {
                q = q.within_group(&agent_id);
            }

            let groups = q.execute(&conn)?;

            Ok::<_, AppError>(groups)
        })
        .await?;

        Ok(Response::new(
            ResponseStatus::OK,
            State::new(&groups),
            context.start_timestamp(),
            None,
        ))
    }
}
