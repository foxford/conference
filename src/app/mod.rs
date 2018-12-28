use crate::transport::mqtt::compat;
use crate::transport::mqtt::{Agent, AgentBuilder, Publish};
use crate::transport::{AgentId, MessageProperties};
use failure::{format_err, Error};
use log::{error, info};

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn run() {
    // Config
    let config = config::load().expect("Failed to load config");
    info!("App config: {:?}", config);

    // Agent
    let agent_id = AgentId::new("alpha", config.id);
    let (mut tx, rx) = AgentBuilder::new(agent_id, config.backend_id.clone())
        .start(&config.mqtt)
        .expect("Failed to create an agent");

    // TODO: Remove creating a demo room
    self::room::create_demo_room();

    // TODO: derive a backend agent id from a status message
    let backend_agent_id = AgentId::new("alpha", config.backend_id.clone());

    // Create Real-Time Connection resource
    let rtc = rtc::State { backend_agent_id };

    for message in rx {
        match message {
            rumqtt::client::Notification::Publish(ref message) => {
                let topic = &message.topic_name;
                let bytes = &message.payload.as_slice();

                let result = if topic.starts_with(&format!("apps/{}", &config.backend_id)) {
                    janus::handle_message(&mut tx, bytes)
                } else {
                    handle_message(&mut tx, bytes, &rtc)
                };

                match result {
                    Err(err) => {
                        let text = std::str::from_utf8(bytes).unwrap_or("[non-utf8 characters]");
                        error!(
                            "Error processing a message = {text} sent to the topic = {topic}: {detail}",
                            text = text,
                            topic = topic,
                            detail = err,
                        )
                    }
                    _ => (),
                }
            }
            _ => error!("An unsupported type of message = {:?}", message),
        }
    }
}

fn handle_message(tx: &mut Agent, bytes: &[u8], rtc: &rtc::State) -> Result<(), Error> {
    let envelope = serde_json::from_slice::<compat::Envelope>(bytes)?;
    match envelope.properties() {
        MessageProperties::Request(ref req) => match req.method() {
            "rtc.create" => {
                let req = rtc.create(&compat::into_message(envelope)?)?;
                req.publish(tx)
            }
            _ => Err(format_err!("Unsupported request method: {:?}", envelope)),
        },
        _ => Err(format_err!("Unsupported message type: {:?}", envelope)),
    }
}

mod config;
mod janus;
mod model;
mod room;
mod rtc;
