use crate::{
    app::{
        context::GlobalContext,
        error::{ErrorExt, ErrorKind},
        stage::{video_group::VideoGroupSendMqttNotification, AppStage},
    },
    db,
    outbox::{StageError, StageHandle},
};
use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use svc_conference_events::EventV1 as Event;
use svc_nats_client::EventId;
use uuid::Uuid;

const SUBJECT_PREFIX: &str = "classroom";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VideoGroupSendNatsNotification {
    pub room_id: db::room::Id,
    pub classroom_id: Uuid,
    pub event: Event,
}

#[async_trait]
impl StageHandle for VideoGroupSendNatsNotification {
    type Context = Arc<dyn GlobalContext>;
    type Stage = AppStage;

    async fn handle(
        &self,
        ctx: &Self::Context,
        id: &EventId,
    ) -> Result<Option<Self::Stage>, StageError> {
        let event = svc_conference_events::Event::from(self.event);

        let payload = serde_json::to_vec(&event)
            .context("invalid payload")
            .error(ErrorKind::InvalidPayload)?;

        let subject = svc_nats_client::Subject::new(
            SUBJECT_PREFIX.to_string(),
            self.classroom_id,
            id.entity_type().to_string(),
        );

        let event =
            svc_nats_client::Event::new(subject, payload, id.to_owned(), ctx.agent_id().to_owned());

        ctx.nats_client()
            .publish(&event)
            .await
            .error(ErrorKind::NatsPublishFailed)?;

        let next_stage = AppStage::VideoGroupSendMqttNotification(VideoGroupSendMqttNotification {
            room_id: self.room_id.to_owned(),
        });

        Ok(Some(next_stage))
    }
}
