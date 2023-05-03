use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use svc_agent::{
    mqtt::{Agent, OutgoingEvent, OutgoingEventProperties, ShortTermTimingProperties},
    Error,
};

#[async_trait]
pub trait MqttClient: Send + Sync {
    fn publish(&mut self, label: &'static str, path: &str) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct Client {
    agent: Agent,
}

pub fn new(agent: Agent) -> Client {
    Client { agent }
}

impl MqttClient for Client {
    fn publish(&mut self, label: &'static str, path: &str) -> Result<(), Error> {
        let timing = ShortTermTimingProperties::until_now(Utc::now());
        let props = OutgoingEventProperties::new(label, timing);

        let msg = Box::new(OutgoingEvent::broadcast(json!({}), props, path));

        self.agent.publish_publishable(msg)
    }
}
