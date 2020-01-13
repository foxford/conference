use std::sync::Arc;
use std::thread;

use chrono::{DateTime, Utc};
use failure::Error;
use futures::{executor::ThreadPoolBuilder, task::SpawnExt, StreamExt};
use log::{error, info, warn};
use serde_derive::Deserialize;
use svc_agent::mqtt::{
    compat, AgentBuilder, ConnectionMode, Notification, Publishable, QoS, SubscriptionTopic,
};
use svc_agent::{AgentId, Authenticable, SharedGroup, Subscription};
use svc_authn::{jose::Algorithm, token::jws_compact};

use crate::db::ConnectionPool;

pub(crate) const API_VERSION: &str = "v1";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct IdTokenConfig {
    #[serde(deserialize_with = "svc_authn::serde::algorithm")]
    algorithm: Algorithm,
    #[serde(deserialize_with = "svc_authn::serde::file")]
    key: Vec<u8>,
}

////////////////////////////////////////////////////////////////////////////////

struct State {
    agent: endpoint::agent::State,
    room: endpoint::room::State,
    rtc: endpoint::rtc::State,
    rtc_signal: endpoint::rtc_signal::State,
    rtc_stream: endpoint::rtc_stream::State,
    message: endpoint::message::State,
    subscription: endpoint::subscription::State,
    system: endpoint::system::State,
}

struct Route {
    janus_status_subscription_topic: String,
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
    let token = jws_compact::TokenBuilder::new()
        .issuer(&agent_id.as_account_id().audience().to_string())
        .subject(&agent_id)
        .key(config.id_token.algorithm, config.id_token.key.as_slice())
        .build()
        .expect("Error creating an id token");
    let mut agent_config = config.mqtt.clone();
    agent_config.set_password(&token);
    let group = SharedGroup::new("loadbalancer", agent_id.as_account_id().clone());
    let (mut tx, rx) = AgentBuilder::new(agent_id.clone(), API_VERSION)
        .connection_mode(ConnectionMode::Service)
        .start(&agent_config)
        .expect("Failed to create an agent");

    // Event loop for incoming messages of MQTT Agent
    let (mq_tx, mut mq_rx) = futures_channel::mpsc::unbounded::<Notification>();
    thread::spawn(move || {
        for message in rx {
            if let Err(_) = mq_tx.unbounded_send(message) {
                error!("Error sending message to the internal channel");
            }
        }
    });

    // Authz
    let authz = svc_authz::ClientMap::new(&config.id, None, config.authz)
        .expect("Error converting authz config to clients");

    // Sentry
    if let Some(sentry_config) = config.sentry.as_ref() {
        svc_error::extension::sentry::init(sentry_config);
    }

    // Application resources
    let state = Arc::new(State {
        agent: endpoint::agent::State::new(authz.clone(), db.clone()),
        room: endpoint::room::State::new(authz.clone(), db.clone()),
        rtc: endpoint::rtc::State::new(authz.clone(), db.clone(), agent_id.clone()),
        rtc_signal: endpoint::rtc_signal::State::new(authz.clone(), db.clone(), agent_id.clone()),
        rtc_stream: endpoint::rtc_stream::State::new(authz.clone(), db.clone()),
        message: endpoint::message::State::new(agent_id.clone(), db.clone()),
        subscription: endpoint::subscription::State::new(config.broker_id, db.clone()),
        system: endpoint::system::State::new(config.id.clone(), authz.clone(), db.clone()),
    });

    // Create RoomBackend resource
    let backend = Arc::new(janus::State::new(db.clone()));

    // Create Subscriptions
    let route = Arc::new(Route {
        janus_status_subscription_topic: {
            let subscription =
                Subscription::broadcast_events(&config.backend_id, API_VERSION, "status");

            tx.subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
                .expect("Error subscribing to backend events topic");

            subscription
                .subscription_topic(&agent_id, API_VERSION)
                .expect("Error building janus events subscription topic")
        },
        janus_events_subscription_topic: {
            let subscription =
                Subscription::broadcast_events(&config.backend_id, API_VERSION, "events");

            tx.subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
                .expect("Error subscribing to backend events topic");

            subscription
                .subscription_topic(&agent_id, API_VERSION)
                .expect("Error building janus events subscription topic")
        },
        janus_responses_subscription_topic: {
            let subscription = Subscription::unicast_responses_from(&config.backend_id);

            tx.subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
                .expect("Error subscribing to backend responses topic");

            subscription
                .subscription_topic(&agent_id, API_VERSION)
                .expect("Error building janus responses subscription topic")
        },
    });
    tx.subscribe(
        &Subscription::multicast_requests(Some(API_VERSION)),
        QoS::AtMostOnce,
        Some(&group),
    )
    .expect("Error subscribing to everyone's output messages");

    // Thread Pool
    let threadpool = ThreadPoolBuilder::new().create()?;

    while let Some(message) = mq_rx.next().await {
        let start_timestamp = Utc::now();
        let mut tx = tx.clone();
        let state = state.clone();
        let backend = backend.clone();
        let route = route.clone();
        let agent_id = agent_id.clone();
        threadpool.spawn(async move {
            match message {
                svc_agent::mqtt::Notification::Publish(message) => {
                    let topic: &str = &message.topic_name;

                    // Log incoming messages
                    info!(
                        "Incoming message = '{}' sent to the topic = '{}', dup = '{}', pkid = '{:?}'",
                        String::from_utf8_lossy(message.payload.as_slice()), topic, message.dup, message.pkid,
                    );

                    let result = match topic {
                        val if val == &route.janus_status_subscription_topic => {
                            janus::handle_status(
                                message.payload.clone(),
                                backend.clone(),
                                start_timestamp,
                                agent_id,
                            ).await
                        }
                        val if val == &route.janus_events_subscription_topic => {
                            janus::handle_event(
                                message.payload.clone(),
                                backend.clone(),
                                start_timestamp,
                            ).await
                        }
                        val if val == &route.janus_responses_subscription_topic => {
                            janus::handle_response(
                                message.payload.clone(),
                                backend.clone(),
                                start_timestamp,
                                agent_id,
                            ).await
                        }
                        _ => handle_message(
                            message.payload.clone(),
                            state.clone(),
                            start_timestamp,
                        ).await,
                    };

                    let result = result.and_then(|messages| {
                        for message in messages.into_iter() {
                            tx.publish(message)?;
                        }

                        Ok(())
                    });

                    if let Err(err) = result {
                        error!(
                            "Error processing a message = '{text}' sent to the topic = '{topic}', {detail}",
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
    payload: Arc<Vec<u8>>,
    state: Arc<State>,
    start_timestamp: DateTime<Utc>,
) -> Result<Vec<Box<dyn Publishable>>, Error> {
    use endpoint::{handle_event, handle_request, handle_unknown_method};

    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(payload.as_slice())?;
    match envelope.properties() {
        compat::IncomingEnvelopeProperties::Request(ref reqp) => {
            let reqp = reqp.clone();
            match reqp.method() {
                "agent.list" => {
                    handle_request(
                        "agent.list",
                        "Error listing agents",
                        &reqp,
                        endpoint::agent::State::list,
                        &state.agent,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "room.create" => {
                    handle_request(
                        "room.create",
                        "Error creating a room",
                        &reqp,
                        endpoint::room::State::create,
                        &state.room,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "room.read" => {
                    handle_request(
                        "room.read",
                        "Error reading the room",
                        &reqp,
                        endpoint::room::State::read,
                        &state.room,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "room.update" => {
                    handle_request(
                        "room.update",
                        "Error updating a room",
                        &reqp,
                        endpoint::room::State::update,
                        &state.room,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "room.delete" => {
                    handle_request(
                        "room.delete",
                        "Error deleting a room",
                        &reqp,
                        endpoint::room::State::delete,
                        &state.room,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "room.enter" => {
                    handle_request(
                        "room.enter",
                        "Error entering a room",
                        &reqp,
                        endpoint::room::State::enter,
                        &state.room,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "room.leave" => {
                    handle_request(
                        "rtc.leave",
                        "Error leaving a room",
                        &reqp,
                        endpoint::room::State::leave,
                        &state.room,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "rtc.create" => {
                    handle_request(
                        "rtc.create",
                        "Error creating the rtc",
                        &reqp,
                        endpoint::rtc::State::create,
                        &state.rtc,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "rtc.connect" => {
                    handle_request(
                        "rtc.connect",
                        "Error connection to the rtc",
                        &reqp,
                        endpoint::rtc::State::connect,
                        &state.rtc,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "rtc.read" => {
                    handle_request(
                        "rtc.read",
                        "Error reading the rtc",
                        &reqp,
                        endpoint::rtc::State::read,
                        &state.rtc,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "rtc.list" => {
                    handle_request(
                        "rtc.list",
                        "Error listing rtcs",
                        &reqp,
                        endpoint::rtc::State::list,
                        &state.rtc,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "rtc_signal.create" => {
                    handle_request(
                        "rtc_signal.create",
                        "Error creating an rtc signal",
                        &reqp,
                        endpoint::rtc_signal::State::create,
                        &state.rtc_signal,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "rtc_stream.list" => {
                    handle_request(
                        "rtc_stream.list",
                        "Error listing rtc streams",
                        &reqp,
                        endpoint::rtc_stream::State::list,
                        &state.rtc_stream,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "message.broadcast" => {
                    handle_request(
                        "message.broadcast",
                        "Error broadcasting a message",
                        &reqp,
                        endpoint::message::State::broadcast,
                        &state.message,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                // TODO: message.create is deprecated and renamed to message.unicast.
                "message.unicast" | "message.create" => {
                    handle_request(
                        "message.unicast",
                        "Error creating an agent signal",
                        &reqp,
                        endpoint::message::State::unicast,
                        &state.message,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                "system.vacuum" => {
                    handle_request(
                        "system.vacuum",
                        "Error vacuuming data",
                        &reqp,
                        endpoint::system::State::vacuum,
                        &state.system,
                        envelope,
                        start_timestamp,
                    )
                    .await
                }
                method => handle_unknown_method(method, &reqp, start_timestamp),
            }
        }
        compat::IncomingEnvelopeProperties::Response(_) => {
            let resp = compat::into_response(envelope)?;
            state.message.callback(resp, start_timestamp).await
        }
        compat::IncomingEnvelopeProperties::Event(ref evp) => match evp.label() {
            Some("subscription.create") => {
                handle_event(
                    "subscription.create",
                    "Error handling subscription creation",
                    endpoint::subscription::State::create,
                    &state.subscription,
                    envelope,
                    start_timestamp,
                )
                .await
            }
            Some("subscription.delete") => {
                handle_event(
                    "subscription.delete",
                    "Error handling subscription deletion",
                    endpoint::subscription::State::delete,
                    &state.subscription,
                    envelope,
                    start_timestamp,
                )
                .await
            }
            Some(label) => {
                warn!("Unknown event {}", label);
                Ok(vec![])
            }
            None => {
                warn!("Missing `label` field in the event");
                Ok(vec![])
            }
        },
    }
}

mod config;
mod endpoint;
mod janus;
