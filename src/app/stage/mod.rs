use crate::{
    app::{
        context::GlobalContext,
        error::Error,
        stage::video_group::{
            VideoGroupSendMqttNotification, VideoGroupSendNatsNotification,
            VideoGroupUpdateJanusConfig,
        },
    },
    outbox::{StageError, StageHandle},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use svc_nats_client::EventId;

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
    type Context = Arc<dyn GlobalContext>;
    type Stage = AppStage;

    async fn handle(&self, ctx: &Self::Context, id: &EventId) -> Result<Option<Self>, StageError> {
        let result = match self {
            AppStage::VideoGroupUpdateJanusConfig(s) => s.handle(ctx, id).await,
            AppStage::VideoGroupSendNatsNotification(s) => s.handle(ctx, id).await,
            AppStage::VideoGroupSendMqttNotification(s) => s.handle(ctx, id).await,
        };

        if let Err(ref err) = result {
            if let Some(e) = err.error().downcast_ref::<Error>() {
                e.notify_sentry();
            }

            ctx.metrics().observe_outbox_error(err.code());
        }

        result
    }
}

impl From<Error> for StageError {
    fn from(error: Error) -> Self {
        StageError::new(error.error_kind().status().as_u16(), Box::new(error))
    }
}
