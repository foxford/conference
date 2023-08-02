use crate::{
    app::{
        context::GlobalContext,
        error::Error,
        group_reader_config,
        stage::video_group::{
            VideoGroupSendMqttNotification, VideoGroupSendNatsNotification,
            VideoGroupUpdateJanusConfig,
        },
        AppErrorKind,
    },
    backend::janus::client::update_agent_reader_config::UpdateReaderConfigRequestBodyConfigItem,
    db::{
        self,
        room::{FindQueryable, Object as RoomObject},
    },
};
use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, str::FromStr, sync::Arc};
use svc_agent::AgentId;
use svc_events::{stage::UpdateJanusConfigAndSendNotificationStageV1, Event, EventId, EventV1};
use svc_nats_client::{
    consumer::{FailureKind, FailureKindExt, HandleMessageFailure},
    Subject,
};

use crate::app::error::{ErrorExt, ErrorKind};

pub mod nats_ids;
pub mod video_group;

#[async_trait::async_trait]
pub trait StageHandle
where
    Self: Sized + Clone + Send + Sync + 'static,
{
    type Context;
    type Stage;

    async fn handle(&self, ctx: &Self::Context, id: &EventId)
        -> Result<Option<Self::Stage>, Error>;
}

#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Serialize, Clone)]
#[serde(tag = "name")]
pub enum AppStage {
    VideoGroupUpdateJanusConfig(VideoGroupUpdateJanusConfig),
    VideoGroupSendNatsNotification(VideoGroupSendNatsNotification),
    VideoGroupSendMqttNotification(VideoGroupSendMqttNotification),
}

async fn handle_stage(
    ctx: &Arc<dyn GlobalContext + Send + Sync>,
    stage: &AppStage,
    id: &EventId,
) -> Result<Option<AppStage>, Error> {
    match stage {
        AppStage::VideoGroupUpdateJanusConfig(s) => s.handle(ctx, id).await,
        AppStage::VideoGroupSendNatsNotification(s) => s.handle(ctx, id).await,
        AppStage::VideoGroupSendMqttNotification(s) => s.handle(ctx, id).await,
    }
}

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
        Event::V1(EventV1::UpdateJanusConfigAndSendNotificationStage(e)) => {
            handle_update_janus_config_and_send_notification_stage(
                ctx.clone(),
                &event_id,
                e,
                room,
                agent_id.clone(),
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

async fn handle_update_janus_config_and_send_notification_stage(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    event_id: &EventId,
    e: UpdateJanusConfigAndSendNotificationStageV1,
    room: RoomObject,
    agent_id: AgentId,
) -> Result<(), HandleMessageFailure<Error>> {
    let mut conn = ctx
        .get_conn()
        .await
        .error(AppErrorKind::DbConnAcquisitionFailed)
        .transient()?;

    let group_agent = db::group_agent::FindQuery::new(room.id())
        .execute(&mut conn)
        .await
        .error(AppErrorKind::DbQueryFailed)
        .transient()?;

    let mut groups = group_agent.groups();
    if !groups.is_agent_exist(&agent_id) {
        groups = groups.add_to_default_group(&agent_id);
    }

    let _existed_groups = db::group_agent::FindQuery::new(room.id())
        .execute(&mut conn)
        .await
        .error(AppErrorKind::DbQueryFailed)
        .transient()?
        .groups()
        .len();

    db::group_agent::UpsertQuery::new(room.id(), &groups)
        .execute(&mut conn)
        .await
        .error(AppErrorKind::DbQueryFailed)
        .transient()?;

    // Update rtc_reader_configs
    let configs = group_reader_config::update(&mut conn, room.id(), groups)
        .await
        .error(AppErrorKind::DbQueryFailed)
        .transient()?;

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

    let init_stage = VideoGroupUpdateJanusConfig::init(
        EventV1::from(e.event),
        room.classroom_id(),
        room.id(),
        e.backend_id,
        items,
    );

    let result = handle_stage(&ctx, &init_stage, &event_id).await;
    let next_stage = match result {
        Ok(Some(next_stage)) => next_stage,
        _ => Err(anyhow!("UpdateJanusConfigStage failed"))
            .error(ErrorKind::StageProcessingFailed)
            .transient()?,
    };

    let result = handle_stage(&ctx, &next_stage, &event_id).await;
    let next_stage = match result {
        Ok(Some(next_stage)) => next_stage,
        _ => Err(anyhow!("SendNatsNotificationStage failed"))
            .error(ErrorKind::StageProcessingFailed)
            .transient()?,
    };

    let result = handle_stage(&ctx, &next_stage, &event_id).await;
    match result {
        Ok(None) => (),
        _ => Err(anyhow!("SendMqttNotificationStage failed"))
            .error(ErrorKind::StageProcessingFailed)
            .transient()?,
    }

    Ok(())
}
