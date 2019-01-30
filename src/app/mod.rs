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
    let agent_id = AgentId::new(&generate_agent_label(), config.id);
    info!("Agent id: {:?}", &agent_id);
    let (mut tx, rx) = AgentBuilder::new(agent_id.clone(), config.backend_id.clone())
        .start(&config.mqtt)
        .expect("Failed to create an agent");

    // TODO: derive a backend agent id from a status message
    let backend_agent_id = AgentId::new("alpha", config.backend_id.clone());

    // Create Room resource
    let room = room::State::new(db.clone());

    // Create Real-Time Connection resource
    let rtc = rtc::State::new(db.clone(), backend_agent_id.clone());

    // Create Signal resource
    let signal = signal::State::new(db.clone());

    // Create Backend resource
    let backend = janus::State::new(db.clone());

    for message in rx {
        match message {
            rumqtt::client::Notification::Publish(ref message) => {
                let topic = &message.topic_name;
                let bytes = &message.payload.as_slice();

                {
                    // Log incoming messages
                    let text = std::str::from_utf8(bytes).unwrap_or("[non-utf8 characters]");
                    info!(
                        "incoming message = '{}' sent to the topic = '{}'",
                        text, topic,
                    )
                }

                let result = if topic.starts_with(&format!("apps/{}", &config.backend_id)) {
                    janus::handle_message(&mut tx, bytes, &backend)
                } else {
                    handle_message(&mut tx, bytes, &rtc, &signal, &room)
                };

                if let Err(err) = result {
                    let text = std::str::from_utf8(bytes).unwrap_or("[non-utf8 characters]");
                    error!(
                        "error processing a message = '{text}' sent to the topic = '{topic}', '{detail}'",
                        text = text,
                        topic = topic,
                        detail = err,
                    )
                }
            }
            _ => error!("an unsupported type of message = '{:?}'", message),
        }
    }
}

fn handle_message(
    tx: &mut Agent,
    bytes: &[u8],
    rtc: &rtc::State,
    signal: &signal::State,
    room: &room::State,
) -> Result<(), Error> {
    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(bytes)?;
    match envelope.properties() {
        compat::IncomingEnvelopeProperties::Request(ref req) => match req.method() {
            "room.create" => {
                let req = compat::into_request(envelope)?;
                let next = room.create(&req)?;
                next.publish(tx)
            }
            "rtc.create" => {
                // TODO: catch and process errors: unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = rtc.create(&req)?;
                next.publish(tx)
            }
            "rtc.read" => {
                // TODO: catch and process errors: not found, unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = rtc.read(&req)?;
                next.publish(tx)
            }
            "rtc.list" => {
                // TODO: catch and process errors: unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = rtc.list(&req)?;
                next.publish(tx)
            }
            "signal.create" => {
                // TODO: catch and process errors: unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = signal.create(&req)?;
                next.publish(tx)
            }
            _ => Err(format_err!("unsupported request method – {:?}", envelope)),
        },
        _ => Err(format_err!("unsupported message type – {:?}", envelope)),
    }
}

fn generate_agent_label() -> String {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    thread_rng().sample_iter(&Alphanumeric).take(32).collect()
}

mod config;
mod janus;
mod room;
mod rtc;
mod signal;
