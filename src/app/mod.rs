use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use crate::{
    app::error::{Error as AppError, ErrorKind as AppErrorKind},
    backend::janus::{client_pool::Clients, JANUS_API_VERSION},
    config::{self, Config, KruonisConfig},
    db::ConnectionPool,
};
use anyhow::{Context as AnyhowContext, Result};
use async_std::task;
use chrono::Utc;
use context::{AppContext, GlobalContext, JanusTopics};
use futures::StreamExt;
use message_handler::MessageHandler;
use prometheus::{Encoder, Registry, TextEncoder};
use serde_json::json;
use signal_hook::consts::TERM_SIGNALS;
use slog::{error, info, warn};
use svc_agent::{
    mqtt::{
        Agent, AgentBuilder, AgentNotification, ConnectionMode, OutgoingRequest,
        OutgoingRequestProperties, QoS, ShortTermTimingProperties, SubscriptionTopic,
    },
    AccountId, AgentId, Authenticable, SharedGroup, Subscription,
};
use svc_authn::token::jws_compact;
use svc_authz::cache::{Cache as AuthzCache, ConnectionPool as RedisConnectionPool};

pub const API_VERSION: &str = "v1";

////////////////////////////////////////////////////////////////////////////////

pub async fn run(
    db: &ConnectionPool,
    redis_pool: Option<RedisConnectionPool>,
    authz_cache: Option<AuthzCache>,
) -> Result<()> {
    // Config
    let is_stopped = Arc::new(AtomicBool::new(false));
    let config = config::load().expect("Failed to load config");
    info!(crate::LOG, "App config: {:?}", config);

    // Agent
    let agent_id = AgentId::new(&config.agent_label, config.id.clone());
    info!(crate::LOG, "Agent id: {}", &agent_id);

    let token = jws_compact::TokenBuilder::new()
        .issuer(&agent_id.as_account_id().audience().to_string())
        .subject(&agent_id)
        .key(config.id_token.algorithm, config.id_token.key.as_slice())
        .build()
        .context("Error creating an id token")?;

    let mut agent_config = config.mqtt.clone();
    agent_config.set_password(&token);

    let (mut agent, rx) = AgentBuilder::new(agent_id.clone(), API_VERSION)
        .connection_mode(ConnectionMode::Service)
        .start(&agent_config)
        .context("Failed to create an agent")?;

    // Event loop for incoming messages of MQTT Agent
    let (mq_tx, mq_rx) = async_std::channel::unbounded();

    thread::Builder::new()
        .name("conference-notifications-loop".to_owned())
        .spawn({
            let is_stopped = is_stopped.clone();
            move || {
                for message in rx {
                    if is_stopped.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }
                    mq_tx
                        .try_send(message)
                        .expect("Messages receiver must be alive")
                }
            }
        })
        .expect("Failed to start conference notifications loop");

    // Authz
    let authz = svc_authz::ClientMap::new(&config.id, authz_cache, config.authz.clone())
        .context("Error converting authz config to clients")?;

    // Sentry
    if let Some(sentry_config) = config.sentry.as_ref() {
        svc_error::extension::sentry::init(sentry_config);
    }

    //metrics
    let metrics_registry = Registry::new();
    let metrics = crate::app::metrics::Metrics::new(&metrics_registry)?;
    let janus_metrics = crate::backend::janus::metrics::Metrics::new(&metrics_registry)?;
    thread::spawn({
        let db = db.clone();
        let collect_interval = config.metrics.janus_metrics_collect_interval;
        move || janus_metrics.start_collector(db.clone(), collect_interval)
    });
    task::spawn(start_metrics_collector(
        metrics_registry,
        config.metrics.http.bind_address,
    ));

    // Subscribe to topics
    let janus_topics = subscribe(&mut agent, &agent_id, &config)?;

    let (ev_tx, mut ev_rx) = async_std::channel::unbounded();
    let clients = Clients::new(ev_tx);
    // Context
    let context = AppContext::new(
        config.clone(),
        authz,
        db.clone(),
        janus_topics,
        clients,
        Arc::new(metrics),
    );

    let context = match redis_pool {
        Some(pool) => context.add_redis_pool(pool),
        None => context,
    };

    // Message handler
    let message_handler = Arc::new(MessageHandler::new(agent.clone(), context));

    let events_task = {
        let message_handler = message_handler.clone();
        async_std::task::spawn(async move {
            loop {
                let ev = ev_rx
                    .next()
                    .await
                    .expect("At least one events sender must be alive");
                let message_handler = message_handler.clone();
                async_std::task::spawn(async move {
                    message_handler.handle_events(ev).await;
                });
            }
        })
    };
    let messages_task = async_std::task::spawn({
        let message_handler = message_handler.clone();
        let is_stopped = is_stopped.clone();
        async move {
            loop {
                if is_stopped.load(Ordering::SeqCst) {
                    break;
                }
                let message = mq_rx.recv().await.expect("Messages sender must be alive");

                let message_handler = message_handler.clone();
                task::spawn(async move {
                    match message {
                        AgentNotification::Message(message, metadata) => {
                            message_handler
                                .global_context()
                                .metrics()
                                .total_requests
                                .inc();
                            message_handler.handle(&message, &metadata.topic).await;
                        }
                        AgentNotification::Reconnection => {
                            error!(crate::LOG, "Reconnected to broker");

                            resubscribe(
                                &mut message_handler.agent().to_owned(),
                                message_handler.global_context().agent_id(),
                                message_handler.global_context().config(),
                            );
                        }
                        AgentNotification::Puback(_) => (),
                        AgentNotification::Pubrec(_) => (),
                        AgentNotification::Pubcomp(_) => (),
                        AgentNotification::Suback(_) => (),
                        AgentNotification::Unsuback(_) => (),
                        AgentNotification::ConnectionError => (),
                        AgentNotification::Connect(_) => (),
                        AgentNotification::Connack(_) => (),
                        AgentNotification::Pubrel(_) => (),
                        AgentNotification::Subscribe(_) => (),
                        AgentNotification::Unsubscribe(_) => (),
                        AgentNotification::PingReq => (),
                        AgentNotification::PingResp => (),
                        AgentNotification::Disconnect => {
                            error!(crate::LOG, "Disconnected from broker")
                        }
                    }
                });
            }
        }
    });

    let mut signals_stream = signal_hook_async_std::Signals::new(TERM_SIGNALS)?.fuse();
    let signals = signals_stream.next();
    let app = futures::future::join_all([events_task, messages_task]);
    futures::future::select(app, signals).await;
    is_stopped.store(true, Ordering::SeqCst);
    message_handler.global_context().janus_clients().clear();
    // wait 5 seconds for stop
    task::sleep(Duration::from_secs(5)).await;
    Ok(())
}

fn subscribe(agent: &mut Agent, agent_id: &AgentId, config: &Config) -> Result<JanusTopics> {
    let group = SharedGroup::new("loadbalancer", agent_id.as_account_id().clone());

    // Multicast requests
    agent
        .subscribe(
            &Subscription::multicast_requests(Some(API_VERSION)),
            QoS::AtMostOnce,
            Some(&group),
        )
        .context("Error subscribing to multicast requests")?;

    // Dynsub responses
    agent
        .subscribe(
            &Subscription::unicast_responses_from(&config.broker_id),
            QoS::AtLeastOnce,
            None,
        )
        .context("Error subscribing to dynsub responses")?;

    // Janus status events
    let subscription =
        Subscription::broadcast_events(&config.backend.id, JANUS_API_VERSION, "status");

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
        .context("Error subscribing to backend events topic")?;

    let janus_status_events_topic = subscription
        .subscription_topic(agent_id, API_VERSION)
        .context("Error building janus events subscription topic")?;

    // Kruonis
    if let KruonisConfig {
        id: Some(ref kruonis_id),
    } = config.kruonis
    {
        subscribe_to_kruonis(kruonis_id, agent)?;
    }

    // Return Janus subscription topics
    Ok(JanusTopics::new(&janus_status_events_topic))
}

fn subscribe_to_kruonis(kruonis_id: &AccountId, agent: &mut Agent) -> Result<()> {
    let timing = ShortTermTimingProperties::new(Utc::now());

    let topic = Subscription::unicast_requests_from(kruonis_id)
        .subscription_topic(agent.id(), API_VERSION)
        .context("Failed to build subscription topic")?;

    let props = OutgoingRequestProperties::new("kruonis.subscribe", &topic, "", timing);
    let event = OutgoingRequest::multicast(json!({}), props, kruonis_id, API_VERSION);

    agent.publish(event).context("Failed to publish message")?;

    Ok(())
}

fn resubscribe(agent: &mut Agent, agent_id: &AgentId, config: &Config) {
    if let Err(err) = subscribe(agent, agent_id, config) {
        let err = err.context("Failed to resubscribe after reconnection");
        error!(crate::LOG, "{:?}", err);

        let app_error = AppError::new(AppErrorKind::ResubscriptionFailed, err);
        app_error.notify_sentry(&crate::LOG);
    }
}

async fn start_metrics_collector(
    registry: Registry,
    bind_addr: SocketAddr,
) -> async_std::io::Result<()> {
    let mut app = tide::with_state(registry);
    app.at("/metrics")
        .get(|req: tide::Request<Registry>| async move {
            let registry = req.state();
            let mut buffer = vec![];
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            match encoder.encode(&metric_families, &mut buffer) {
                Ok(_) => {
                    let mut response = tide::Response::new(200);
                    response.set_body(buffer);
                    Ok(response)
                }
                Err(err) => {
                    warn!(crate::LOG, "Metrics not gathered: {:?}", err);
                    Ok(tide::Response::new(500))
                }
            }
        });
    app.listen(bind_addr).await
}

pub mod context;
pub mod endpoint;
pub mod error;
pub mod handle_id;
pub mod message_handler;
pub mod metrics;
