use failure::{format_err, Error};
use futures::{executor::ThreadPoolBuilder, task::SpawnExt, StreamExt};
use log::{error, info};
use std::sync::Arc;
use std::thread;
use svc_agent::mqtt::{
    compat, Agent, AgentBuilder, ConnectionMode, Notification, Publish, QoS, SubscriptionTopic,
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
    message: endpoint::message::State,
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
        message: endpoint::message::State::new(agent_id.clone()),
        system: endpoint::system::State::new(config.id.clone(), authz.clone(), db.clone()),
    });

    // Create Backend resource
    let backend = Arc::new(janus::State::new(db.clone()));

    // Create Subscriptions
    let route = Arc::new(Route {
        janus_events_subscription_topic: {
            let subscription = Subscription::broadcast_events(&config.backend_id, "events/status");
            tx.subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
                .expect("Error subscribing to backend events topic");

            subscription
                .subscription_topic(&agent_id)
                .expect("Error building janus events subscription topic")
        },
        janus_responses_subscription_topic: {
            let subscription = Subscription::broadcast_events(&config.backend_id, "responses");
            tx.subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
                .expect("Error subscribing to backend responses topic");

            subscription
                .subscription_topic(&agent_id)
                .expect("Error building janus responses subscription topic")
        },
    });
    tx.subscribe(
        &Subscription::multicast_requests(),
        QoS::AtMostOnce,
        Some(&group),
    )
    .expect("Error subscribing to everyone's output messages");

    // Thread Pool
    let mut threadpool = ThreadPoolBuilder::new().create()?;

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

                    // Log incoming messages
                    info!(
                        "Incoming message = '{}' sent to the topic = '{}', dup = '{}', pkid = '{:?}'",
                        String::from_utf8_lossy(message.payload.as_slice()), topic, message.dup, message.pkid,
                    );

                    let result = match topic {
                        val if val == &route.janus_events_subscription_topic => {
                            await!(janus::handle_event(
                                &mut tx,
                                message.payload.clone(),
                                backend.clone(),
                            ))
                        }
                        val if val == &route.janus_responses_subscription_topic => {
                            await!(janus::handle_response(
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
                        error!(
                            "Error processing a message = '{text}' sent to the topic = '{topic}', '{detail}'",
                            text = String::from_utf8_lossy(message.payload.as_slice()),
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
    use endpoint::{handle_badrequest, handle_badrequest_method, handle_response};

    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(payload.as_slice())?;
    match envelope.properties() {
        compat::IncomingEnvelopeProperties::Request(ref reqp) => {
            let reqp = reqp.clone();
            match reqp.method() {
                method @ "room.create" => {
                    let error_title = "Error creating a room";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.room.create(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "room.read" => {
                    let error_title = "Error reading the room";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.room.read(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "room.update" => {
                    let error_title = "Error updating a room";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.room.update(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "room.delete" => {
                    let error_title = "Error deleting a room";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.room.delete(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "rtc.create" => {
                    let error_title = "Error creating the rtc";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.rtc.create(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "rtc.connect" => {
                    let error_title = "Error connection to the rtc";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.rtc.connect(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "rtc.read" => {
                    let error_title = "Error reading the rtc";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.rtc.read(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "rtc.list" => {
                    let error_title = "Error listing rtcs";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.rtc.list(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "rtc_signal.create" => {
                    let error_title = "Error creating a rtc signal";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.rtc_signal.create(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "rtc_stream.list" => {
                    let error_title = "Error listing rtc streams";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.rtc_stream.list(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "message.create" => {
                    let error_title = "Error creating an agent signal";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.message.create(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method @ "system.vacuum" => {
                    let error_title = "Error vacuuming data";
                    match compat::into_request(envelope) {
                        Ok(req) => {
                            let next = await!(state.system.vacuum(req));
                            handle_response(method, error_title, tx, &reqp, next)
                        }
                        Err(err) => handle_badrequest(method, error_title, tx, &reqp, &err),
                    }
                }
                method => handle_badrequest_method(method, tx, &reqp),
            }
        }
        compat::IncomingEnvelopeProperties::Response(_) => {
            let resp = compat::into_response(envelope)?;
            let next = await!(state.message.callback(resp))?;
            next.publish(tx).map_err(Into::into)
        }
        _ => Err(format_err!(
            "unsupported message type, envelope = '{:?}'",
            envelope
        )),
    }
}

mod config;
mod endpoint;
mod janus;
