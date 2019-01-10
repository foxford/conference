use crate::authn::AgentId;
use crate::db::ConnectionPool;
use crate::transport::mqtt::compat;
use crate::transport::mqtt::{Agent, AgentBuilder, Publish};
use failure::{format_err, Error};
use log::{error, info};

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn run(db: &ConnectionPool) {
    // Config
    let config = config::load().expect("Failed to load config");
    info!("App config: {:?}", config);

    // Agent
    let agent_id = AgentId::new("alpha", config.id);
    let (mut tx, rx) = AgentBuilder::new(agent_id.clone(), config.backend_id.clone())
        .start(&config.mqtt)
        .expect("Failed to create an agent");

    // TODO: Remove creating a demo room
    {
        let conn = db.get().expect("Error getting a database connection");
        self::room::create_demo_room(&conn, agent_id.account_id().audience());
    }

    // TODO: derive a backend agent id from a status message
    let backend_agent_id = AgentId::new("alpha", config.backend_id.clone());

    // Create Real-Time Connection resource
    let rtc = rtc::State::new(db.clone(), backend_agent_id);

    // Create Backend resource
    let backend = janus::State::new(db.clone());

    for message in rx {
        match message {
            rumqtt::client::Notification::Publish(ref message) => {
                let topic = &message.topic_name;
                let bytes = &message.payload.as_slice();

                let result = if topic.starts_with(&format!("apps/{}", &config.backend_id)) {
                    janus::handle_message(&mut tx, bytes, &backend)
                } else {
                    handle_message(&mut tx, bytes, &rtc)
                };

                if let Err(err) = result {
                    let text = std::str::from_utf8(bytes).unwrap_or("[non-utf8 characters]");
                    error!(
                        "Error processing a message = {text} sent to the topic = {topic}: {detail}",
                        text = text,
                        topic = topic,
                        detail = err,
                    )
                }
            }
            _ => error!("An unsupported type of message = {:?}", message),
        }
    }
}

fn handle_message(tx: &mut Agent, bytes: &[u8], rtc: &rtc::State) -> Result<(), Error> {
    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(bytes)?;
    match envelope.properties() {
        compat::IncomingEnvelopeProperties::Request(ref req) => match req.method() {
            "rtc.create" => {
                let next = rtc.create(compat::into_request(envelope)?)?;
                next.publish(tx)
            }
            "rtc.read" => {
                let req = compat::into_request(envelope)?;
                let next = rtc.read(&req)?;
                next.publish(tx)
            }
            _ => Err(format_err!("Unsupported request method: {:?}", envelope)),
        },
        _ => Err(format_err!("Unsupported message type: {:?}", envelope)),
    }
}

mod config;
mod janus;
mod room;
mod rtc;
