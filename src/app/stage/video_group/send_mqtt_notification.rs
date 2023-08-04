use crate::{
    app::{
        context::GlobalContext,
        error::{Error, ErrorExt, ErrorKind},
    },
    db,
};
use std::sync::Arc;

pub const MQTT_NOTIFICATION_LABEL: &str = "video_group.update";

pub async fn send_mqtt_notification(
    ctx: Arc<dyn GlobalContext + Send + Sync>,
    room_id: db::room::Id,
) -> Result<(), Error> {
    let topic = format!("rooms/{}/events", room_id);

    ctx.mqtt_client()
        .lock()
        .publish(MQTT_NOTIFICATION_LABEL, &topic)
        .error(ErrorKind::MqttPublishFailed)?;

    Ok(())
}
