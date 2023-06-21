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
    outbox::{error::StageError, StageHandle},
};
use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, str::FromStr, sync::Arc};
use svc_events::{
    ban::{BanAcceptedV1, BanVideoStreamingCompletedV1},
    Event, EventId, EventV1,
};
use svc_nats_client::{
    consumer::{FailureKind, FailureKindExt, HandleMessageFailure},
    Subject,
};

use super::error::{ErrorExt, ErrorKind};

pub mod video_group;

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

    let headers = svc_nats_client::Headers::try_from(msg.headers.clone().unwrap_or_default())
        .context("parse nats headers")
        .permanent()?;
    let _agent_id = headers.sender_id();

    let r = match event {
        Event::V1(EventV1::BanAccepted(e)) => {
            handle_ban_accepted(ctx.as_ref(), e, &room, subject, &headers).await
        }
        _ => {
            // ignore
            Ok(())
        }
    };

    FailureKindExt::map_err(r, |e| anyhow!(e))
}

async fn handle_ban_accepted(
    ctx: &(dyn GlobalContext + Sync),
    e: BanAcceptedV1,
    room: &db::room::Object,
    subject: Subject,
    headers: &svc_nats_client::Headers,
) -> Result<(), HandleMessageFailure<Error>> {
    let mut conn = ctx.get_conn().await.transient()?;

    let event_id = headers.event_id();
    let event = BanVideoStreamingCompletedV1::new_from_accepted(e, event_id.clone());

    let payload = serde_json::to_vec(&event)
        .error(ErrorKind::InvalidPayload)
        .permanent()?;

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

    Ok(())
}
