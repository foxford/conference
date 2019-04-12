use failure::{format_err, Error};
use futures::{executor::ThreadPool, task::SpawnExt, StreamExt};
use log::{error, info};
use std::sync::Arc;
use std::thread;
use svc_agent::mqtt::{
    compat, Agent, AgentBuilder, ConnectionMode, Notification, QoS, SubscriptionTopic,
};
use svc_agent::{AgentId, SharedGroup, Subscription};
use svc_authn::Authenticable;

use crate::db::ConnectionPool;

////////////////////////////////////////////////////////////////////////////////

struct State {
    room: endpoint::room::State,
    rtc: endpoint::rtc::State,
    rtc_signal: endpoint::rtc_signal::State,
    rtc_stream: endpoint::rtc_stream::State,
    system: endpoint::system::State,
}

struct Route {
    janus_events_subscription_topic: String,
    janus_responses_subscription_topic: String,
}

pub(crate) async fn run(db: &ConnectionPool) -> Result<(), Error> {
    // Config
    let config = config::load().expect("Failed to load config");
    info!("App config: {:?}", config);

    // Agent
    let agent_id = AgentId::new(&config.agent_label, config.id.clone());
    info!("Agent id: {:?}", &agent_id);
    let group = SharedGroup::new("loadbalancer", agent_id.as_account_id().clone());
    let (mut tx, rx) = AgentBuilder::new(agent_id.clone())
        .mode(ConnectionMode::Service)
        .start(&config.mqtt)
        .expect("Failed to create an agent");

    //
    let (ch_tx, mut ch_rx) = futures_channel::mpsc::unbounded::<Notification>();
    thread::spawn(move || {
        for message in rx {
            if let Err(e) = ch_tx.unbounded_send(message) {
                error!(
                    "Error sending message to the internal channel, {detail}",
                    detail = e
                );
            }
        }
    });
    //

    // Authz
    let authz = svc_authz::ClientMap::new(&config.id, config.authz)
        .expect("Error converting authz config to clients");

    // Application resources
    let state = Arc::new(State {
        room: endpoint::room::State::new(authz.clone(), db.clone()),
        rtc: endpoint::rtc::State::new(authz.clone(), db.clone()),
        rtc_signal: endpoint::rtc_signal::State::new(authz.clone(), db.clone()),
        rtc_stream: endpoint::rtc_stream::State::new(authz.clone(), db.clone()),
        system: endpoint::system::State::new(config.id.clone(), authz.clone(), db.clone()),
    });

    // Create Backend resource
    let backend = Arc::new(janus::State::new(db.clone()));

    // Create Subscriptions
    let route = Arc::new(Route {
        janus_events_subscription_topic: {
            let subscription = Subscription::broadcast_events(&config.backend_id, "events/status");
            tx.subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
                .expect("Error subscribing to backend events");

            subscription
                .subscription_topic(&agent_id)
                .expect("Error building janus events subscription topic")
        },
        janus_responses_subscription_topic: {
            let subscription = Subscription::broadcast_events(&config.backend_id, "responses");
            tx.subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
                .expect("Error subscribing to backend responses");

            subscription
                .subscription_topic(&agent_id)
                .expect("Error building janus responses subscription topic")
        },
    });
    tx.subscribe(
        &Subscription::multicast_requests(),
        QoS::AtLeastOnce,
        Some(&group),
    )
    .expect("Error subscribing to everyone's output messages");

    // Thread Pool
    let mut threadpool = ThreadPool::new()?;

    while let Some(message) = await!(ch_rx.next()) {
        let tx = tx.clone();
        let state = state.clone();
        let backend = backend.clone();
        let route = route.clone();
        threadpool.spawn(async move {
            let mut tx = tx.clone();
            match message {
                svc_agent::mqtt::Notification::Publish(message) => {
                    let topic: &str = &message.topic_name;

                    {
                        // Log incoming messages
                        let bytes = &message.payload.as_slice();
                        let text = std::str::from_utf8(bytes).unwrap_or("[non-utf8 characters]");
                        info!(
                            "Incoming message = '{}' sent to the topic = '{}'",
                            text, topic,
                        )
                    }

                    let result = match topic {
                        val if val == &route.janus_events_subscription_topic => {
                            await!(janus::handle_events(
                                &mut tx,
                                message.payload.clone(),
                                backend.clone(),
                            ))
                        }
                        val if val == &route.janus_responses_subscription_topic => {
                            await!(janus::handle_responses(
                                &mut tx,
                                message.payload.clone(),
                                backend.clone(),
                            ))
                        }
                        _ => await!(handle_message(
                            &mut tx,
                            message.payload.clone(),
                            state.clone(),
                        )),
                    };

                    if let Err(err) = result {
                        let bytes = &message.payload.as_slice();
                        let text = std::str::from_utf8(bytes).unwrap_or("[non-utf8 characters]");
                        error!(
                            "Error processing a message = '{text}' sent to the topic = '{topic}', '{detail}'",
                            text = text,
                            topic = topic,
                            detail = err,
                        )
                    }
                }
                _ => error!("An unsupported type of message = '{:?}'", message),
            }

        }).unwrap();
    }

    Ok(())
}

async fn handle_message(
    tx: &mut Agent,
    payload: Arc<Vec<u8>>,
    state: Arc<State>,
) -> Result<(), Error> {
    use endpoint::handle_error;

    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(payload.as_slice())?;
    match envelope.properties() {
        compat::IncomingEnvelopeProperties::Request(ref req) => match req.method() {
            "room.create" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.room.create(req));
                handle_error(
                    "error:room.create",
                    "Error creating a room",
                    tx,
                    &props,
                    next,
                )
            }
            "room.read" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.room.read(req));
                handle_error(
                    "error:room.read",
                    "Error reading the room",
                    tx,
                    &props,
                    next,
                )
            }
            "room.update" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.room.update(req));
                handle_error(
                    "error:room.update",
                    "Error updating the room",
                    tx,
                    &props,
                    next,
                )
            }
            "room.delete" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.room.delete(req));
                handle_error(
                    "error:room.delete",
                    "Error deleting the room",
                    tx,
                    &props,
                    next,
                )
            }
            "rtc.create" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.rtc.create(req));
                handle_error(
                    "error:rtc.create",
                    "Error creating the rtc",
                    tx,
                    &props,
                    next,
                )
            }
            "rtc.connect" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.rtc.connect(req));
                handle_error(
                    "error:rtc.connect",
                    "Error connection to the rtc",
                    tx,
                    &props,
                    next,
                )
            }
            "rtc.read" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.rtc.read(req));
                handle_error("error:rtc.read", "Error reading the rtc", tx, &props, next)
            }
            "rtc.list" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.rtc.list(req));
                handle_error("error:rtc.list", "Error listing rtcs", tx, &props, next)
            }
            "rtc_signal.create" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.rtc_signal.create(req));
                handle_error(
                    "error:rtc_signal.create",
                    "Error creating a rtc signal",
                    tx,
                    &props,
                    next,
                )
            }
            "rtc_stream.list" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.rtc_stream.list(req));
                handle_error(
                    "error:rtc_stream.list",
                    "Error listing rtc streams",
                    tx,
                    &props,
                    next,
                )
            }
            "system.vacuum" => {
                let req = compat::into_request(envelope)?;
                let props = req.properties().clone();
                let next = await!(state.system.vacuum(req));
                handle_error(
                    "error:system.vacuum",
                    "Error vacuuming data",
                    tx,
                    &props,
                    next,
                )
            }
            _ => Err(format_err!(
                "unsupported request method, envelope = '{:?}'",
                envelope
            )),
        },
        _ => Err(format_err!(
            "unsupported message type, envelope = '{:?}'",
            envelope
        )),
    }
}

mod config;
mod endpoint;
mod janus;
