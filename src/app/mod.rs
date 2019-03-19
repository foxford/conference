use failure::{format_err, Error};
use log::{error, info};
use svc_agent::mqtt::{
    compat, Agent, AgentBuilder, ConnectionMode, Publish, QoS, SubscriptionTopic,
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
    let agent_id = AgentId::new(&config.agent_label, config.id.clone());
    info!("Agent id: {:?}", &agent_id);
    let group = SharedGroup::new("loadbalancer", agent_id.as_account_id().clone());
    let (mut tx, rx) = AgentBuilder::new(agent_id.clone())
        .mode(ConnectionMode::Service)
        .start(&config.mqtt)
        .expect("Failed to create an agent");

    let janus_events_subscription_topic = {
        let subscription = Subscription::broadcast_events(&config.backend_id, "events/status");
        tx.subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
            .expect("Error subscribing to backend events");

        subscription
            .subscription_topic(&agent_id)
            .expect("Error building janus events subscription topic")
    };
    let janus_responses_subscription_topic = {
        let subscription = Subscription::broadcast_events(&config.backend_id, "responses");
        tx.subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
            .expect("Error subscribing to backend responses");

        subscription
            .subscription_topic(&agent_id)
            .expect("Error building janus responses subscription topic")
    };
    tx.subscribe(
        &Subscription::multicast_requests(),
        QoS::AtLeastOnce,
        Some(&group),
    )
    .expect("Error subscribing to everyone's output messages");

    // Create Room resource
    let room = room::State::new(authz.clone(), db.clone());

    // Create Real-Time Connection resource
    let rtc = rtc::State::new(authz.clone(), db.clone());

    // Create Rtc Stream resource
    let rtc_stream = rtc_stream::State::new(authz.clone(), db.clone());

    // Create Rtc Signal resource
    let rtc_signal = rtc_signal::State::new(authz.clone(), db.clone());

    // Create System resource
    let system = system::State::new(config.id.clone(), authz.clone(), db.clone());

    // Create Backend resource
    let backend = janus::State::new(db.clone());

    for message in rx {
        match message {
            svc_agent::mqtt::Notification::Publish(ref message) => {
                let topic: &str = &message.topic_name;
                let bytes = &message.payload.as_slice();

                {
                    // Log incoming messages
                    let text = std::str::from_utf8(bytes).unwrap_or("[non-utf8 characters]");
                    info!(
                        "incoming message = '{}' sent to the topic = '{}'",
                        text, topic,
                    )
                }

                let result = match topic {
                    val if val == &janus_events_subscription_topic => {
                        janus::handle_events(&mut tx, bytes, &backend)
                    }
                    val if val == &janus_responses_subscription_topic => {
                        janus::handle_responses(&mut tx, bytes, &backend)
                    }
                    _ => handle_message(
                        &mut tx,
                        bytes,
                        &rtc,
                        &rtc_signal,
                        &rtc_stream,
                        &room,
                        &system,
                    ),
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
    rtc_signal: &rtc_signal::State,
    rtc_stream: &rtc_stream::State,
    room: &room::State,
    system: &system::State,
) -> Result<(), Error> {
    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(bytes)?;
    match envelope.properties() {
        compat::IncomingEnvelopeProperties::Request(ref req) => match req.method() {
            "room.create" => {
                // TODO: catch and process errors: unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = room.create(&req)?;
                next.publish(tx)
            }
            "room.read" => {
                // TODO: catch and process errors: not found, unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = room.read(&req)?;
                next.publish(tx)
            }
            "room.delete" => {
                // TODO: catch and process errors: not found, unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = room.delete(&req)?;
                next.publish(tx)
            }
            "room.update" => {
                // TODO: catch and process errors: not found, unprocessable entry
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
            "rtc.connect" => {
                // TODO: catch and process errors: unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = rtc.connect(&req)?;
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
            "rtc_signal.create" => {
                // TODO: catch and process errors: unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = rtc_signal.create(&req)?;
                next.publish(tx)
            }
            "rtc_stream.list" => {
                // TODO: catch and process errors: unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = rtc_stream.list(&req)?;
                next.publish(tx)
            }
            "system.upload" => {
                // TODO: catch and process errors: unprocessable entry
                let req = compat::into_request(envelope)?;
                let next = system.upload(&req)?;
                next.publish(tx)
            }
            _ => Err(format_err!("unsupported request method – {:?}", envelope)),
        },
        _ => Err(format_err!("unsupported message type – {:?}", envelope)),
    }
}

mod config;
mod janus;
mod room;
mod rtc;
mod rtc_signal;
mod rtc_stream;
mod system;
