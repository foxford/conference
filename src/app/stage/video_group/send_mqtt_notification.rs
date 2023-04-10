use crate::{
    app::{
        context::GlobalContext,
        error::{ErrorExt, ErrorKind},
        stage::AppStage,
    },
    db,
    outbox::{error::StageError, StageHandle},
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use svc_nats_client::EventId;

pub const MQTT_NOTIFICATION_LABEL: &str = "video_group.update";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VideoGroupSendMqttNotification {
    pub room_id: db::room::Id,
}

#[async_trait]
impl StageHandle for VideoGroupSendMqttNotification {
    type Context = Arc<dyn GlobalContext + Send>;
    type Stage = AppStage;

    async fn handle(
        &self,
        ctx: &Self::Context,
        _id: &EventId,
    ) -> Result<Option<Self::Stage>, StageError> {
        let topic = format!("rooms/{}/events", self.room_id);

        ctx.mqtt_client()
            .lock()
            .publish(MQTT_NOTIFICATION_LABEL, &topic)
            .error(ErrorKind::MqttPublishFailed)?;

        Ok(None)
    }
}
