use std::{net::SocketAddr, sync::Arc, thread, time::Duration};

use crate::{
    app::error::{Error as AppError, ErrorKind as AppErrorKind},
    backend::janus::{client_pool::Clients, JANUS_API_VERSION},
    config::{self, Config, KruonisConfig},
    db::ConnectionPool,
};
use anyhow::{Context as AnyhowContext, Result};
use chrono::Utc;
use context::{AppContext, GlobalContext, JanusTopics};
use crossbeam_channel::select;
use futures::StreamExt;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Response, Server, StatusCode,
};
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
use tokio::task;
use tracing::{error, info, warn};

pub const API_VERSION: &str = "v1";

////////////////////////////////////////////////////////////////////////////////

pub async fn run(
    db: &ConnectionPool,
    redis_pool: Option<RedisConnectionPool>,
    authz_cache: Option<AuthzCache>,
) -> Result<()> {
    // Config
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
    task::spawn(start_metrics_collector(
        metrics_registry,
        config.metrics.http.bind_address,
    ));

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

    let context = match redis_pool {
        Some(pool) => context.add_redis_pool(pool),
        None => context,
    };

    // Message handler
    let message_handler = Arc::new(MessageHandler::new(agent.clone(), context));
    {
        let message_handler = message_handler.clone();
        thread::spawn(move || loop {
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
    let mut signals_stream = signal_hook_tokio::Signals::new(TERM_SIGNALS)?.fuse();
    let signals = signals_stream.next();
    let _ = signals.await;
    unsubscribe(&mut agent, &agent_id, &config)?;
    message_handler
        .global_context()
        .janus_clients()
        .stop_polling();

    task::sleep(Duration::from_secs(3)).await;
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
            AgentNotification::Unsubscribe(_) => {
                info!("Unsubscribed")
            }
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

fn unsubscribe(agent: &mut Agent, agent_id: &AgentId, config: &Config) -> anyhow::Result<()> {
    let group = SharedGroup::new("loadbalancer", agent_id.as_account_id().clone());

    // Multicast requests
    agent
        .unsubscribe(
            &Subscription::multicast_requests(Some(API_VERSION)),
            Some(&group),
        )
        .context("Error unsubscribing to multicast requests")?;

    // Dynsub responses
    agent
        .unsubscribe(
            &Subscription::unicast_responses_from(&config.broker_id),
            None,
        )
        .context("Error unsubscribing to dynsub responses")?;

    // Janus status events
    let subscription =
        Subscription::broadcast_events(&config.backend.id, JANUS_API_VERSION, "status");

    agent
        .unsubscribe(&subscription, Some(&group))
        .context("Error unsubscribing to backend events topic")?;

    Ok(())
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

async fn start_metrics_collector(registry: Registry, bind_addr: SocketAddr) -> anyhow::Result<()> {
    let service = make_service_fn(move |_| {
        let registry = registry.clone();
        std::future::ready::<Result<_, hyper::Error>>(Ok(service_fn(move |_| {
            let registry = registry.clone();
            async move {
                let mut buffer = vec![];
                let encoder = TextEncoder::new();
                let metric_families = registry.gather();
                match encoder.encode(&metric_families, &mut buffer) {
                    Ok(_) => {
                        let response = Response::new(Body::from(buffer));
                        Ok::<_, hyper::Error>(response)
                    }
                    Err(err) => {
                        warn!(?err, "Metrics not gathered");
                        let mut response = Response::default();
                        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                        Ok(response)
                    }
                }
            }
        })))
    });
    let server = Server::bind(&bind_addr).serve(service);

    server.await?;

    Ok(())
}

pub mod context;
pub mod endpoint;
pub mod error;
pub mod handle_id;
pub mod message_handler;
pub mod metrics;
