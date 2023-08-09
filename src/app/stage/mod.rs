use crate::{
    app::{
        context::GlobalContext,
        error::Error,
        group_reader_config,
        stage::video_group::{send_mqtt_notification, update_janus_config, ENTITY_TYPE},
        AppError, AppErrorKind,
    },
    backend::janus::client::update_agent_reader_config::UpdateReaderConfigRequestBodyConfigItem,
    client::nats,
    db::{
        self,
        room::{FindQueryable, Object as RoomObject},
    },
};
use anyhow::{anyhow, Context};
use sqlx::Connection;
use std::{convert::TryFrom, str::FromStr, sync::Arc};
use svc_agent::AgentId;
use svc_events::{Event, EventId, EventV1, VideoGroupEventV1};
use svc_nats_client::{
    consumer::{FailureKind, FailureKindExt, HandleMessageFailure},
    Subject,
};
use uuid::Uuid;

use crate::app::error::{ErrorExt, ErrorKind};

pub mod video_group;

pub async fn route_message(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    msg: Arc<svc_nats_client::Message>,
) -> Result<(), HandleMessageFailure<anyhow::Error>> {
    let subject = Subject::from_str(&msg.subject)
        .context("parse nats subject")
        .permanent()?;

    let event = serde_json::from_slice::<Event>(msg.payload.as_ref())
        .context("parse nats payload")
        .permanent()?;

    let classroom_id = subject.classroom_id();
    let room = {
        let mut conn = ctx
            .get_conn()
            .await
            .map_err(anyhow::Error::from)
            .transient()?;

        db::room::FindQuery::by_classroom_id(classroom_id)
            .execute(&mut conn)
            .await
            .context("find room by classroom_id")
            .transient()?
            .ok_or(anyhow!(
                "failed to get room by classroom_id: {}",
                classroom_id
            ))
            .permanent()?
    };

    tracing::info!(?event, class_id = %classroom_id);

    let headers = svc_nats_client::Headers::try_from(msg.headers.clone().unwrap_or_default())
        .context("parse nats headers")
        .permanent()?;
    let agent_id = headers.sender_id();
    let event_id = headers.event_id();

    let r: Result<(), HandleMessageFailure<Error>> = match event {
        Event::V1(EventV1::VideoGroupCreateIntent(e)) => {
            handle_video_group_intent_event(
                ctx.clone(),
                event_id,
                room,
                agent_id.clone(),
                classroom_id,
                e.backend_id.clone(),
                VideoGroupEventV1::Created {
                    created_at: e.created_at,
                },
            )
            .await
        }
        Event::V1(EventV1::VideoGroupDeleteIntent(e)) => {
            handle_video_group_intent_event(
                ctx.clone(),
                event_id,
                room,
                agent_id.clone(),
                classroom_id,
                e.backend_id.clone(),
                VideoGroupEventV1::Deleted {
                    created_at: e.created_at,
                },
            )
            .await
        }
        Event::V1(EventV1::VideoGroupUpdateIntent(e)) => {
            handle_video_group_intent_event(
                ctx.clone(),
                event_id,
                room,
                agent_id.clone(),
                classroom_id,
                e.backend_id.clone(),
                VideoGroupEventV1::Updated {
                    created_at: e.created_at,
                },
            )
            .await
        }
        _ => {
            // ignore
            Ok(())
        }
    };

    FailureKindExt::map_err(r, |e| anyhow!(e))
}

async fn handle_video_group_intent_event(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    event_id: &EventId,
    room: RoomObject,
    agent_id: AgentId,
    classroom_id: Uuid,
    backend_id: AgentId,
    event: VideoGroupEventV1,
) -> Result<(), HandleMessageFailure<Error>> {
    let configs = {
        let mut conn = ctx
            .get_conn()
            .await
            .error(AppErrorKind::DbConnAcquisitionFailed)
            .transient()?;
        let room = room.clone();
        conn.transaction::<_, _, AppError>(|conn| {
            Box::pin(async move {
                let group_agent = db::group_agent::FindQuery::new(room.id())
                    .execute(conn)
                    .await
                    .error(AppErrorKind::DbQueryFailed)?;

                let mut groups = group_agent.groups();
                if !groups.is_agent_exist(&agent_id) {
                    groups = groups.add_to_default_group(&agent_id);
                }

                db::group_agent::UpsertQuery::new(room.id(), &groups)
                    .execute(conn)
                    .await
                    .error(AppErrorKind::DbQueryFailed)?;

                // Update rtc_reader_configs
                let configs = group_reader_config::update(conn, room.id(), groups)
                    .await
                    .error(AppErrorKind::DbQueryFailed)?;

                Ok(configs)
            })
        })
        .await
        .error(AppErrorKind::DbQueryFailed)
        .transient()?
    };

    // Generate configs for janus
    let items = configs
        .into_iter()
        .map(
            |((rtc_id, agent_id), value)| UpdateReaderConfigRequestBodyConfigItem {
                reader_id: agent_id,
                stream_id: rtc_id,
                receive_video: value,
                receive_audio: value,
            },
        )
        .collect::<Vec<_>>();

    update_janus_config(ctx.clone(), backend_id, items)
        .await
        .error(ErrorKind::StageProcessingFailed)
        .transient()?;

    let event_id = &EventId::from((
        ENTITY_TYPE.to_string(),
        "send_notification".to_string(),
        event_id.sequence_id(),
    ));
    let event = Event::from(event);

    nats::publish_event(ctx.clone(), classroom_id, event_id, event)
        .await
        .error(ErrorKind::StageProcessingFailed)
        .transient()?;

    send_mqtt_notification(ctx, room.id)
        .await
        .error(ErrorKind::StageProcessingFailed)
        .transient()?;

    Ok(())
}
