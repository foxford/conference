pub use send_mqtt_notification::VideoGroupSendMqttNotification;
pub use send_nats_notification::VideoGroupSendNatsNotification;
pub use update_janus_config::VideoGroupUpdateJanusConfig;

mod send_mqtt_notification;
mod send_nats_notification;
pub mod update_janus_config;

pub const ENTITY_TYPE: &str = "video_group";
