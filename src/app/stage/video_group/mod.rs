pub use send_mqtt_notification::{VideoGroupSendMqttNotification, MQTT_NOTIFICATION_LABEL};
pub use send_nats_notification::VideoGroupSendNatsNotification;
pub use update_janus_config::VideoGroupUpdateJanusConfig;

mod send_mqtt_notification;
mod send_nats_notification;
pub mod update_janus_config;

pub const ENTITY_TYPE: &str = "video_group";
pub const CREATED_OPERATION: &str = "created";
pub const UPDATED_OPERATION: &str = "updated";
pub const DELETED_OPERATION: &str = "deleted";
