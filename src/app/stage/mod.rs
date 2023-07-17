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
use svc_events::{Event, EventId};
use svc_nats_client::{
    consumer::{FailureKind, FailureKindExt, HandleMessageFailure},
    Subject,
};

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

    // TODO: remove after adding more match bindings
    #[allow(clippy::match_single_binding)]
    let r: Result<(), HandleMessageFailure<Error>> = match event {
        _ => {
            // ignore
            Ok(())
        }
    };

    FailureKindExt::map_err(r, |e| anyhow!(e))
}
