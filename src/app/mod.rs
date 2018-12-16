use crate::transport;
use failure::{err_msg, Error};
use log::{error, info, warn};
use rumqtt::{MqttClient, MqttOptions, QoS};
use uuid::Uuid;

mod config;
mod janus;

#[derive(Debug)]
pub(crate) struct AgentBuilder {
    label: String,
    application: transport::ApplicationIdentity,
    backend: transport::ApplicationName,
}

impl AgentBuilder {
    fn new(
        label: &str,
        application: transport::ApplicationIdentity,
        backend: transport::ApplicationName,
    ) -> Self {
        Self {
            label: label.to_owned(),
            application,
            backend,
        }
    }

    fn start(
        self,
        config: &config::Mqtt,
    ) -> Result<(Agent, crossbeam_channel::Receiver<rumqtt::Notification>), Error> {
        let client_id = Self::mqtt_client_id(&self.application.agent_id(&self.label));
        let options = Self::mqtt_options(&client_id, &config)?;
        let (tx, rx) = MqttClient::start(options)?;

        let mut agent = Agent::new(self.application, self.backend, tx);
        let group = agent.application.group("loadbalancer");
        agent.tx.subscribe(
            agent.backend_responses_subscription(&group),
            QoS::AtLeastOnce,
        )?;

        Ok((agent, rx))
    }

    fn mqtt_client_id(agent_id: &transport::AgentId) -> String {
        format!("v1.mqtt3/agents/{agent_id}", agent_id = agent_id)
    }

    fn mqtt_options(client_id: &str, config: &config::Mqtt) -> Result<MqttOptions, Error> {
        let uri = config.uri.parse::<http::Uri>()?;
        let host = uri.host().ok_or_else(|| err_msg("missing MQTT host"))?;
        let port = uri
            .port_part()
            .ok_or_else(|| err_msg("missing MQTT port"))?;

        Ok(MqttOptions::new(client_id, host, port.as_u16()).set_keep_alive(30))
    }
}

pub(crate) struct Agent {
    application: transport::ApplicationIdentity,
    backend: transport::ApplicationName,
    tx: rumqtt::MqttClient,
}

impl Agent {
    fn new(
        application: transport::ApplicationIdentity,
        backend: transport::ApplicationName,
        tx: MqttClient,
    ) -> Self {
        Self {
            application,
            backend,
            tx,
        }
    }

    fn publish<T>(&mut self, topic: &str, payload: &T) -> Result<(), Error>
    where
        T: serde::Serialize,
    {
        use crate::transport::compat::to_envelope;

        let message = to_envelope(payload, None)?;
        let bytes = serde_json::to_string(&message)?;

        self.tx
            .publish(topic, QoS::AtLeastOnce, bytes)
            .map_err(|_| err_msg(format!("Failed to publish an MQTT message: {:?}", message)))
    }

    fn backend_input_topic(&self, backend_agent_id: &transport::AgentId) -> String {
        format!(
            "agents/{backend_agent_id}/api/v1/in/{app_name}",
            backend_agent_id = backend_agent_id,
            app_name = self.application.name()
        )
    }

    fn backend_responses_subscription(&self, group: &transport::ApplicationGroup) -> String {
        format!(
            "$share/{group}/apps/{backend_name}/api/v1/responses",
            group = group,
            backend_name = &self.backend,
        )
    }
}

pub(crate) fn run() {
    // Config
    let config = config::load().expect("Failed to load config");
    info!("App config: {:?}", config);

    // Agent
    let (mut tx, rx) = AgentBuilder::new("a", config.identity.clone(), config.backend.clone())
        .start(&config.mqtt)
        .expect("Failed to create an agent");

    // TODO: derive a backend agent id from a status message
    let backend_agent_id = transport::AgentId::new(
        "a",
        Uuid::parse_str("00000000-0000-1071-a000-000000000000").expect("Failed to parse UUID"),
        "example.org",
    );

    // TODO: Replace with Real-Time Connection data
    let room_id = Uuid::new_v4();
    let rtc_id = Uuid::new_v4();

    // Creating a Janus Gateway session
    let req = janus::create_session_request(room_id, rtc_id).expect("Failed to build a request");
    tx.publish(&tx.backend_input_topic(&backend_agent_id), &req)
        .expect("Failed to publish a message");

    for message in rx {
        match message {
            rumqtt::client::Notification::Publish(ref message) => {
                let topic = &message.topic_name;
                let data = &message.payload.as_slice();

                // Processing of backend messages
                if topic.contains(&format!("apps/{}", config.backend)) {
                    match janus::handle_message(&mut tx, data) {
                        Err(err) => handle_error(topic, data, err),
                        Ok(_) => info!("Message has been processed"),
                    }
                } else {
                    error!("Received a message with an unexpected topic = {}", topic)
                }
            }
            _ => error!("An unsupported type of message = {:?}", message),
        }
    }
}

fn handle_error(topic: &str, data: &[u8], error: Error) {
    let message = std::str::from_utf8(data).unwrap_or("[non-utf8 characters]");
    warn!(
        "Processing of a message = {} from a topic = {} failed because of an error = {}.",
        message, topic, error
    );
}
