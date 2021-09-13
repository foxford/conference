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
use crossbeam_channel::select;
use futures::StreamExt;
use message_handler::MessageHandler;
use prometheus::{Encoder, Registry, TextEncoder};
use serde_json::json;
use signal_hook::consts::TERM_SIGNALS;
use svc_agent::{
    mqtt::{
        Agent, AgentBuilder, AgentNotification, ConnectionMode, OutgoingRequest,
        OutgoingRequestProperties, QoS, ShortTermTimingProperties, SubscriptionTopic,
    },
    AccountId, AgentId, Authenticable, SharedGroup, Subscription,
};
use svc_authn::token::jws_compact;
use svc_authz::cache::{Cache as AuthzCache, ConnectionPool as RedisConnectionPool};
use tracing::{error, info, warn};

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

    // Agent
    let agent_id = AgentId::new(&config.agent_label, config.id.clone());
    info!(config = ?config, agent_id = ?agent_id, "App started");

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
    let (ev_tx, ev_rx) = crossbeam_channel::unbounded();
    let clients = Clients::new(ev_tx, config.janus_group.clone());
    thread::spawn({
        let db = db.clone();
        let collect_interval = config.metrics.janus_metrics_collect_interval;
        let clients = clients.clone();
        move || janus_metrics.start_collector(db, clients, collect_interval)
    });

    // Subscribe to topics
    let janus_topics = subscribe(&mut agent, &agent_id, &config)?;
    // Context
    let metrics = Arc::new(metrics);
    let context = AppContext::new(
        config.clone(),
        authz,
        db.clone(),
        janus_topics,
        clients.clone(),
        metrics.clone(),
    );
    task::spawn(start_metrics_collector(
        metrics_registry,
        config.metrics.http.bind_address,
        context.clone(),
    ));

    let context = match redis_pool {
        Some(pool) => context.add_redis_pool(pool),
        None => context,
    };

    // Message handler
    let message_handler = Arc::new(MessageHandler::new(agent, context));
    {
        let is_stopped = is_stopped.clone();
        thread::spawn(move || loop {
            if is_stopped.load(Ordering::SeqCst) {
                message_handler
                    .global_context()
                    .janus_clients()
                    .stop_polling();
                while let Ok(msg) = ev_rx.try_recv() {
                    let message_handler = message_handler.clone();
                    task::spawn(async move {
                        message_handler.handle_events(msg).await;
                    });
                }
                break;
            }
            select! {
                recv(rx) -> msg => {
                    let msg = msg.expect("Agent must be alive");
                    handle_message(msg, message_handler.clone());
                },
                recv(ev_rx) -> msg => {
                    let msg = msg.expect("Events sender must exist");
                    let message_handler = message_handler.clone();
                    task::spawn(async move {
                        message_handler.handle_events(msg).await;
                    });
                },
            }
        });
    }
    let mut signals_stream = signal_hook_async_std::Signals::new(TERM_SIGNALS)?.fuse();
    let signals = signals_stream.next();
    let _ = signals.await;
    is_stopped.store(true, Ordering::SeqCst);
    task::sleep(Duration::from_secs(2)).await;
    info!(
        requests_left = metrics.running_requests_total.get(),
        "Running requests left",
    );
    Ok(())
}

fn handle_message(message: AgentNotification, message_handler: Arc<MessageHandler<AppContext>>) {
    let metric_handle = message_handler.global_context().metrics().request_started();
    task::spawn(async move {
        let metrics = message_handler.global_context().metrics();
        match message {
            AgentNotification::Message(message, metadata) => {
                metrics.total_requests.inc();
                message_handler.handle(message, &metadata.topic).await;
            }
            AgentNotification::Reconnection => {
                error!("Reconnected to broker");
                metrics.mqtt_reconnection.inc();
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
            AgentNotification::ConnectionError => {
                metrics.mqtt_connection_error.inc();
            }
            AgentNotification::Connect(_) => (),
            AgentNotification::Connack(_) => (),
            AgentNotification::Pubrel(_) => (),
            AgentNotification::Subscribe(_) => (),
            AgentNotification::Unsubscribe(_) => (),
            AgentNotification::PingReq => (),
            AgentNotification::PingResp => (),
            AgentNotification::Disconnect => {
                metrics.mqtt_disconnect.inc();
                error!("Disconnected from broker")
            }
        }
        drop(metric_handle)
    });
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
        error!(?err, "Resubscription error");

        let app_error = AppError::new(AppErrorKind::ResubscriptionFailed, err);
        app_error.notify_sentry();
    }
}

async fn start_metrics_collector(
    registry: Registry,
    bind_addr: SocketAddr,
    context: AppContext,
) -> async_std::io::Result<()> {
    let mut app = tide::with_state(registry);
    app.at("/metrics").get(move |req: tide::Request<Registry>| {
        let context = context.clone();
        async move {
            context.metrics().observe_caches(&context);
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
                    warn!(?err, "Metrics not gathered");
                    Ok(tide::Response::new(500))
                }
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
