use failure::{format_err, Error};
use log::{error, info};
use svc_agent::mqtt::{
    compat::{self, IntoEnvelope},
    Agent, AgentBuilder, Publish, QoS,
};
use svc_agent::{AgentId, SharedGroup, Subscription};
use svc_authn::Authenticable;

use crate::db::ConnectionPool;

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn run(db: &ConnectionPool) {
    // Config
    let config = config::load().expect("Failed to load config");
    info!("App config: {:?}", config);

    // Authz
    let authz = svc_authz::ClientMap::new(&config.id, config.authz)
        .expect("Error converting authz config to clients");

    // Agent
    let agent_id = AgentId::new(&generate_agent_label(), config.id.clone());
    info!("Agent id: {:?}", &agent_id);
    let group = SharedGroup::new("loadbalancer", agent_id.as_account_id().clone());
    let (mut tx, rx) = AgentBuilder::new(agent_id.clone())
        .start(&config.mqtt)
        .expect("Failed to create an agent");
    tx.subscribe(
        &Subscription::broadcast_events(&config.backend_id, "responses"),
        QoS::AtLeastOnce,
        Some(&group),
    )
    .expect("Error subscribing to backend responses");
    tx.subscribe(
        &Subscription::multicast_requests(),
        QoS::AtLeastOnce,
        Some(&group),
    )
    .expect("Error subscribing to everyone's output messages");

    // TODO: derive a backend agent id from a status message
    let backend_agent_id = AgentId::new("alpha", config.backend_id.clone());

    // Create Room resource
    let room = room::State::new(authz.clone(), db.clone());

    // Create Real-Time Connection resource
    let rtc = rtc::State::new(authz.clone(), db.clone(), backend_agent_id.clone());

    // Create Signal resource
    let signal = signal::State::new(authz.clone(), db.clone());

    // Create System resource
    let mut system = system::State::new(
        authz.clone(),
        db.clone(),
        config.id.clone(),
        backend_agent_id.clone(),
    );

    // Create Backend resource
    let backend = janus::State::new(db.clone());

    let system_session_request = janus::create_system_session_request(&config.backend_id)
        .expect("Error preparing system session request")
        .into_envelope()
        .expect("Error preparing system session request envelope");
    tx.publish(&system_session_request)
        .expect("Error publishing system session request");

    for message in rx {
        match message {
            svc_agent::mqtt::Notification::Publish(ref message) => {
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
                    janus::handle_message(&mut tx, bytes, &backend, &mut system)
                } else {
                    handle_message(&mut tx, bytes, &rtc, &signal, &room, &system)
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
    system: &system::State,
) -> Result<(), Error> {
    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(bytes)?;
    match envelope.properties() {
        compat::IncomingEnvelopeProperties::Request(ref req) => match req.method() {
            "room.create" => {
                let req = compat::into_request(envelope)?;
                let next = room.create(&req)?;
                next.publish(tx)
            }
            "room.read" => {
                let req = compat::into_request(envelope)?;
                let next = room.read(&req)?;
                next.publish(tx)
            }
            "room.delete" => {
                let req = compat::into_request(envelope)?;
                let next = room.delete(&req)?;
                next.publish(tx)
            }
            "room.update" => {
                let req = compat::into_request(envelope)?;
                let next = room.update(&req)?;
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
            "system.upload" => {
                let req = compat::into_request(envelope)?;
                let next = system.upload(&req)?;
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
mod system;
