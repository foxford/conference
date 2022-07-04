use std::{net::SocketAddr, sync::Arc, thread, time::Duration};

use crate::{
    app::{
        error::{Error as AppError, ErrorKind as AppErrorKind},
        http::build_router,
    },
    backend::janus::{
        client_pool::Clients, online_handler::start_janus_reg_handler, JANUS_API_VERSION,
    },
    client::mqtt_gateway::MqttGatewayHttpClient,
    config::{self, Config},
    db::ConnectionPool,
};
use anyhow::{Context as AnyhowContext, Result};
use context::{AppContext, GlobalContext};
use futures::StreamExt;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Response, Server, StatusCode,
};
use message_handler::MessageHandler;
use prometheus::{Encoder, Registry, TextEncoder};
use signal_hook::consts::TERM_SIGNALS;
use svc_agent::{
    mqtt::{Agent, AgentBuilder, AgentNotification, ConnectionMode, QoS},
    AgentId, Authenticable, SharedGroup, Subscription,
};
use svc_authn::token::jws_compact;
use svc_authz::cache::{AuthzCache, ConnectionPool as RedisConnectionPool, RedisCache};
use tokio::{select, task};
use tracing::{error, info, warn};

pub const API_VERSION: &str = "v1";

////////////////////////////////////////////////////////////////////////////////

pub async fn run(
    db: &ConnectionPool,
    redis_pool: Option<RedisConnectionPool>,
    authz_cache: Option<Box<RedisCache>>,
) -> Result<()> {
    // Config
    let config = config::load().expect("Failed to load config");

    // Agent
    let agent_id = AgentId::new(&config.agent_label, config.id.clone());
    info!(config = ?config, agent_id = ?agent_id, "App started");

    let token = jws_compact::TokenBuilder::new()
        .issuer(agent_id.as_account_id().audience())
        .subject(&agent_id)
        .key(config.id_token.algorithm, config.id_token.key.as_slice())
        .build()
        .context("Error creating an id token")?;

    let mut agent_config = config.mqtt.clone();
    agent_config.set_password(&token);

    let (mut agent, mut rx) = AgentBuilder::new(agent_id.clone(), API_VERSION)
        .connection_mode(ConnectionMode::Service)
        .start(&agent_config)
        .context("Failed to create an agent")?;

    // Authz
    let authz = svc_authz::ClientMap::new(
        &config.id,
        authz_cache.map(|x| x as Box<dyn AuthzCache>),
        config.authz.clone(),
        None,
    )
    .context("Error converting authz config to clients")?;
    // Sentry
    if let Some(sentry_config) = config.sentry.as_ref() {
        svc_error::extension::sentry::init(sentry_config);
    }
    //metrics
    let metrics_registry = Registry::new();
    let metrics = crate::app::metrics::Metrics::new(&metrics_registry)?;
    let janus_metrics = crate::backend::janus::metrics::Metrics::new(&metrics_registry)?;
    let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel();
    let clients = Clients::new(
        ev_tx,
        config.janus_group.clone(),
        db.clone(),
        config.waitlist_epoch_duration,
    );
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
    subscribe(&mut agent, &agent_id, &config)?;
    // Context
    let metrics = Arc::new(metrics);
    let mqtt_gateway_client =
        MqttGatewayHttpClient::new(token.clone(), config.mqtt_api_host_uri.clone());
    let context = AppContext::new(
        config.clone(),
        authz,
        db.clone(),
        clients.clone(),
        metrics.clone(),
        mqtt_gateway_client,
    );
    let reg_handler = tokio::spawn(start_janus_reg_handler(
        config.janus_registry.clone(),
        context.janus_clients(),
        context.db().clone(),
    ));

    let context = match redis_pool {
        Some(pool) => context.add_redis_pool(pool),
        None => context,
    };
    let (graceful_tx, graceful_rx) = tokio::sync::oneshot::channel();
    let _http_task = tokio::spawn(
        axum::Server::bind(&config.http_addr)
            .serve(
                build_router(
                    Arc::new(context.clone()),
                    agent.clone(),
                    config.authn.clone(),
                )
                .into_make_service(),
            )
            .with_graceful_shutdown(async move {
                let _ = graceful_rx.await;
            }),
    );

    // Message handler
    let message_handler = Arc::new(MessageHandler::new(agent.clone(), context));
    {
        let message_handler = message_handler.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    msg = rx.recv() => {
                        if let Some(msg) = msg {
                            handle_message(msg, message_handler.clone())
                        }
                    },
                    msg = ev_rx.recv() => {
                        if let Some(msg) = msg {
                            let message_handler = message_handler.clone();
                            tokio::spawn(async move {
                                message_handler.handle_events(msg).await;
                            });
                        }
                    },
                }
            }
        });
    }
    let mut signals_stream = signal_hook_tokio::Signals::new(TERM_SIGNALS)?.fuse();
    let signals = signals_stream.next();
    let _ = signals.await;
    let _ = graceful_tx.send(());
    unsubscribe(&mut agent, &agent_id, &config)?;
    message_handler
        .global_context()
        .janus_clients()
        .stop_polling();
    reg_handler.abort();
    tokio::time::sleep(Duration::from_secs(3)).await;
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

fn subscribe(agent: &mut Agent, agent_id: &AgentId, config: &Config) -> Result<()> {
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

    // Return Janus subscription topics
    Ok(())
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
        std::future::ready::<Result<_, hyper::Error>>(Ok(service_fn(move |req| {
            let registry = registry.clone();
            async move {
                match req.uri().path() {
                    "/metrics" => {
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
                    path => {
                        warn!(?path, "Not found");
                        let mut response = Response::default();
                        *response.status_mut() = StatusCode::NOT_FOUND;
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
pub mod http;
pub mod message_handler;
pub mod metrics;
pub mod service_utils;
