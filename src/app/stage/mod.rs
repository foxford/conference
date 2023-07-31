use crate::{
    app::{
        context::GlobalContext,
        error::Error,
        stage::video_group::{
            VideoGroupSendMqttNotification, VideoGroupSendNatsNotification,
            VideoGroupUpdateJanusConfig,
        },
    },
    db::{self, room::FindQueryable},
};
use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, str::FromStr, sync::Arc};
use svc_events::{
    stage::{SendMqttNotificationStageV1, SendNatsNotificationStageV1, UpdateJanusConfigStageV1},
    Event, EventId, EventV1,
};
use svc_nats_client::{
    consumer::{FailureKind, FailureKindExt, HandleMessageFailure},
    Subject,
};
use uuid::Uuid;

use crate::app::{
    error::{ErrorExt, ErrorKind},
    stage::video_group::SUBJECT_PREFIX,
};

pub mod nats_ids;
pub mod video_group;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub struct StageError {
    kind: String,
    error: BoxError,
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stage error, kind: {}, error: {}", self.kind, self.error)
    }
}

impl StageError {
    pub fn new(kind: String, error: BoxError) -> Self {
        Self { kind, error }
    }
}

#[async_trait::async_trait]
pub trait StageHandle
where
    Self: Sized + Clone + Send + Sync + 'static,
{
    type Context;
    type Stage;

    async fn handle(
        &self,
        ctx: &Self::Context,
        id: &EventId,
    ) -> Result<Option<Self::Stage>, StageError>;
}

#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Serialize, Clone)]
#[serde(tag = "name")]
pub enum AppStage {
    VideoGroupUpdateJanusConfig(VideoGroupUpdateJanusConfig),
    VideoGroupSendNatsNotification(VideoGroupSendNatsNotification),
    VideoGroupSendMqttNotification(VideoGroupSendMqttNotification),
}

#[async_trait::async_trait]
impl StageHandle for AppStage {
    type Context = Arc<dyn GlobalContext + Send + Sync>;
    type Stage = AppStage;

    async fn handle(&self, ctx: &Self::Context, id: &EventId) -> Result<Option<Self>, StageError> {
        match self {
            AppStage::VideoGroupUpdateJanusConfig(s) => s.handle(ctx, id).await,
            AppStage::VideoGroupSendNatsNotification(s) => s.handle(ctx, id).await,
            AppStage::VideoGroupSendMqttNotification(s) => s.handle(ctx, id).await,
        }
    }
}

impl From<Error> for StageError {
    fn from(error: Error) -> Self {
        StageError::new(error.error_kind().kind().into(), Box::new(error))
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
    let _room = {
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
    let _agent_id = headers.sender_id();

    let r: Result<(), HandleMessageFailure<Error>> = match event {
        Event::V1(EventV1::UpdateJanusConfigStage(e)) => {
            handle_update_janus_config_stage(ctx.clone(), e, classroom_id).await
        }
        Event::V1(EventV1::SendNatsNotificationStage(e)) => {
            handle_send_nats_notification_stage(ctx.clone(), e, classroom_id).await
        }
        Event::V1(EventV1::SendMqttNotificationStage(e)) => {
            handle_send_mqtt_notification_stage(ctx.clone(), e).await
        }
        _ => {
            // ignore
            Ok(())
        }
    };

    FailureKindExt::map_err(r, |e| anyhow!(e))
}

async fn handle_update_janus_config_stage(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    e: UpdateJanusConfigStageV1,
    classroom_id: Uuid,
) -> Result<(), HandleMessageFailure<Error>> {
    let stage: AppStage = serde_json::from_value(e.stage_state)
        .error(ErrorKind::StageStateDeserializationFailed)
        .permanent()?;

    let result = StageHandle::handle(&stage, &ctx, &e.event_id).await;
    match result {
        Ok(Some(next_stage)) => {
            let mut conn = ctx.get_conn().await.transient()?;

            let event_id = crate::app::stage::nats_ids::sqlx::get_next_seq_id(&mut conn)
                .await
                .error(ErrorKind::CreatingNewSequenceIdFailed)
                .transient()?
                .to_event_id();

            let serialized_stage = serde_json::to_value(next_stage)
                .context("serialization failed")
                .error(ErrorKind::StageStateSerializationFailed)
                .permanent()?;

            let event = Event::from(SendNatsNotificationStageV1 {
                event_id: event_id.clone(),
                stage_state: serialized_stage,
            });

            let payload = serde_json::to_vec(&event)
                .error(ErrorKind::InvalidPayload)
                .permanent()?;

            let subject = svc_nats_client::Subject::new(
                SUBJECT_PREFIX.to_string(),
                classroom_id,
                event_id.entity_type().to_string(),
            );

            let event = svc_nats_client::event::Builder::new(
                subject,
                payload,
                event_id.to_owned(),
                ctx.agent_id().to_owned(),
            )
            .build();

            ctx.nats_client()
                .ok_or_else(|| anyhow!("nats client not found"))
                .error(ErrorKind::NatsClientNotFound)
                .transient()?
                .publish(&event)
                .await
                .error(ErrorKind::NatsPublishFailed)
                .transient()?;
        }
        Ok(None) => (),
        _ => Err(anyhow!("UpdateJanusConfigStage failed"))
            .error(ErrorKind::StageProcessingFailed)
            .transient()?,
    }

    Ok(())
}

async fn handle_send_nats_notification_stage(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    e: SendNatsNotificationStageV1,
    classroom_id: Uuid,
) -> Result<(), HandleMessageFailure<Error>> {
    let stage: AppStage = serde_json::from_value(e.stage_state)
        .error(ErrorKind::StageStateDeserializationFailed)
        .permanent()?;

    let result = StageHandle::handle(&stage, &ctx, &e.event_id).await;
    match result {
        Ok(Some(next_stage)) => {
            let mut conn = ctx.get_conn().await.transient()?;

            let event_id = crate::app::stage::nats_ids::sqlx::get_next_seq_id(&mut conn)
                .await
                .error(ErrorKind::CreatingNewSequenceIdFailed)
                .transient()?
                .to_event_id();

            let serialized_stage = serde_json::to_value(next_stage)
                .context("serialization failed")
                .error(ErrorKind::StageStateSerializationFailed)
                .permanent()?;

            let event = Event::from(SendMqttNotificationStageV1 {
                event_id: event_id.clone(),
                stage_state: serialized_stage,
            });

            let payload = serde_json::to_vec(&event)
                .error(ErrorKind::InvalidPayload)
                .permanent()?;

            let subject = svc_nats_client::Subject::new(
                SUBJECT_PREFIX.to_string(),
                classroom_id,
                event_id.entity_type().to_string(),
            );

            let event = svc_nats_client::event::Builder::new(
                subject,
                payload,
                event_id.to_owned(),
                ctx.agent_id().to_owned(),
            )
            .build();

            ctx.nats_client()
                .ok_or_else(|| anyhow!("nats client not found"))
                .error(ErrorKind::NatsClientNotFound)
                .transient()?
                .publish(&event)
                .await
                .error(ErrorKind::NatsPublishFailed)
                .transient()?;
        }
        Ok(None) => (),
        _ => Err(anyhow!("SendNatsNotificationStage failed"))
            .error(ErrorKind::StageProcessingFailed)
            .transient()?,
    }

    Ok(())
}

async fn handle_send_mqtt_notification_stage(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    e: SendMqttNotificationStageV1,
) -> Result<(), HandleMessageFailure<Error>> {
    let stage: AppStage = serde_json::from_value(e.stage_state)
        .error(ErrorKind::StageStateDeserializationFailed)
        .permanent()?;

    let result = StageHandle::handle(&stage, &ctx, &e.event_id).await;
    match result {
        Ok(Some(_next_stage)) => (),
        Ok(None) => (),
        _ => Err(anyhow!("SendMqttNotificationStage failed"))
            .error(ErrorKind::StageProcessingFailed)
            .transient()?,
    }

    Ok(())
}
