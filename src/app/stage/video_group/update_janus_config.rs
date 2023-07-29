use crate::{
    app::{
        context::GlobalContext,
        error::{ErrorExt, ErrorKind},
        stage::{video_group::VideoGroupSendNatsNotification, AppStage, StageError, StageHandle},
    },
    backend::janus::client::update_agent_reader_config::{
        UpdateReaderConfigRequest, UpdateReaderConfigRequestBody,
        UpdateReaderConfigRequestBodyConfigItem,
    },
    db,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use svc_agent::AgentId;
use svc_events::{EventId, EventV1 as Event};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VideoGroupUpdateJanusConfig {
    room_id: db::room::Id,
    classroom_id: Uuid,
    event: Event,
    backend_id: AgentId,
    configs: Vec<UpdateReaderConfigRequestBodyConfigItem>,
}

impl VideoGroupUpdateJanusConfig {
    pub fn init(
        event: Event,
        classroom_id: Uuid,
        room_id: db::room::Id,
        backend_id: AgentId,
        configs: Vec<UpdateReaderConfigRequestBodyConfigItem>,
    ) -> AppStage {
        let stage = Self {
            room_id,
            classroom_id,
            event,
            backend_id,
            configs,
        };

        AppStage::VideoGroupUpdateJanusConfig(stage)
    }
}

#[async_trait]
impl StageHandle for VideoGroupUpdateJanusConfig {
    type Context = Arc<dyn GlobalContext + Send + Sync>;
    type Stage = AppStage;

    async fn handle(
        &self,
        ctx: &Self::Context,
        _id: &EventId,
    ) -> Result<Option<Self::Stage>, StageError> {
        let mut conn = ctx.get_conn().await?;

        let janus_backend = db::janus_backend::FindQuery::new(&self.backend_id)
            .execute(&mut conn)
            .await
            .error(ErrorKind::DbQueryFailed)?
            .ok_or_else(|| anyhow!("Janus backend not found"))
            .error(ErrorKind::BackendNotFound)?;

        let request = UpdateReaderConfigRequest {
            session_id: janus_backend.session_id(),
            handle_id: janus_backend.handle_id(),
            body: UpdateReaderConfigRequestBody::new(self.configs.clone()),
        };

        ctx.janus_clients()
            .get_or_insert(&janus_backend)
            .error(ErrorKind::BackendClientCreationFailed)?
            .reader_update(request)
            .await
            .context("Reader update")
            .error(ErrorKind::BackendRequestFailed)?;

        let next_stage = AppStage::VideoGroupSendNatsNotification(VideoGroupSendNatsNotification {
            room_id: self.room_id.to_owned(),
            classroom_id: self.classroom_id.to_owned(),
            event: self.event.to_owned(),
        });

        Ok(Some(next_stage))
    }
}
