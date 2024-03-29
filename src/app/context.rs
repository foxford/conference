use std::sync::Arc;

use anyhow::Context as AnyhowContext;
use chrono::{DateTime, Utc};
use futures::{future::BoxFuture, FutureExt};
use parking_lot::Mutex;

use svc_agent::AgentId;
use svc_authz::{cache::ConnectionPool as RedisConnectionPool, ClientMap as Authz};
use svc_nats_client::NatsClient;

use crate::{
    app::error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind},
    backend::janus::client_pool::Clients,
    client::{
        conference::ConferenceHttpClient, mqtt::MqttClient, mqtt_gateway::MqttGatewayHttpClient,
    },
    config::Config,
};

use super::metrics::Metrics;

///////////////////////////////////////////////////////////////////////////////

pub trait Context: GlobalContext + MessageContext {}

pub trait GlobalContext {
    fn authz(&self) -> &Authz;
    fn config(&self) -> &Config;
    fn db(&self) -> &sqlx::PgPool;
    fn agent_id(&self) -> &AgentId;
    fn janus_clients(&self) -> Clients;
    fn redis_pool(&self) -> &Option<RedisConnectionPool>;
    fn metrics(&self) -> Arc<Metrics>;
    fn mqtt_gateway_client(&self) -> &MqttGatewayHttpClient;
    fn conference_client(&self) -> &ConferenceHttpClient;
    fn mqtt_client(&self) -> &Mutex<dyn MqttClient>;
    fn nats_client(&self) -> Option<&dyn NatsClient>;
    fn get_conn(&self) -> BoxFuture<Result<sqlx::pool::PoolConnection<sqlx::Postgres>, AppError>> {
        let db = self.db().clone();
        async move {
            db.acquire()
                .await
                .context("failed to acquire DB connection")
                .error(AppErrorKind::DbConnAcquisitionFailed)
        }
        .boxed()
    }
}

impl GlobalContext for Arc<dyn GlobalContext> {
    fn authz(&self) -> &Authz {
        self.as_ref().authz()
    }

    fn config(&self) -> &Config {
        self.as_ref().config()
    }

    fn db(&self) -> &sqlx::PgPool {
        self.as_ref().db()
    }

    fn agent_id(&self) -> &AgentId {
        self.as_ref().agent_id()
    }

    fn janus_clients(&self) -> Clients {
        self.as_ref().janus_clients()
    }

    fn redis_pool(&self) -> &Option<RedisConnectionPool> {
        self.as_ref().redis_pool()
    }

    fn metrics(&self) -> Arc<Metrics> {
        self.as_ref().metrics()
    }

    fn mqtt_gateway_client(&self) -> &MqttGatewayHttpClient {
        self.as_ref().mqtt_gateway_client()
    }

    fn conference_client(&self) -> &ConferenceHttpClient {
        self.as_ref().conference_client()
    }

    fn mqtt_client(&self) -> &Mutex<dyn MqttClient> {
        self.as_ref().mqtt_client()
    }

    fn nats_client(&self) -> Option<&dyn NatsClient> {
        self.as_ref().nats_client()
    }
}

pub trait MessageContext {
    fn start_timestamp(&self) -> DateTime<Utc>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct AppContext {
    config: Arc<Config>,
    authz: Authz,
    db: sqlx::PgPool,
    agent_id: AgentId,
    redis_pool: Option<RedisConnectionPool>,
    clients: Clients,
    metrics: Arc<Metrics>,
    mqtt_gateway_client: MqttGatewayHttpClient,
    conference_client: ConferenceHttpClient,
    mqtt_client: Arc<Mutex<dyn MqttClient>>,
    nats_client: Option<Arc<dyn NatsClient>>,
}

#[allow(clippy::too_many_arguments)]
impl AppContext {
    pub fn new<M>(
        config: Config,
        authz: Authz,
        db: sqlx::PgPool,
        clients: Clients,
        metrics: Arc<Metrics>,
        mqtt_gateway_client: MqttGatewayHttpClient,
        conference_client: ConferenceHttpClient,
        mqtt_client: M,
    ) -> Self
    where
        M: MqttClient + 'static,
    {
        let agent_id = AgentId::new(&config.agent_label, config.id.to_owned());

        Self {
            config: Arc::new(config),
            authz,
            agent_id,
            redis_pool: None,
            clients,
            metrics,
            mqtt_gateway_client,
            conference_client,
            mqtt_client: Arc::new(Mutex::new(mqtt_client)),
            nats_client: None,
            db,
        }
    }

    pub fn add_redis_pool(self, pool: RedisConnectionPool) -> Self {
        Self {
            redis_pool: Some(pool),
            ..self
        }
    }

    pub fn add_nats_client(self, nats_client: impl NatsClient + 'static) -> Self {
        Self {
            nats_client: Some(Arc::new(nats_client)),
            ..self
        }
    }

    pub fn start_message(&self) -> AppMessageContext<'_, Self> {
        AppMessageContext::new(self, Utc::now())
    }
}

impl GlobalContext for AppContext {
    fn authz(&self) -> &Authz {
        &self.authz
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn db(&self) -> &sqlx::PgPool {
        &self.db
    }

    fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    fn janus_clients(&self) -> Clients {
        self.clients.clone()
    }

    fn redis_pool(&self) -> &Option<RedisConnectionPool> {
        &self.redis_pool
    }

    fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    fn mqtt_gateway_client(&self) -> &MqttGatewayHttpClient {
        &self.mqtt_gateway_client
    }

    fn conference_client(&self) -> &ConferenceHttpClient {
        &self.conference_client
    }

    fn mqtt_client(&self) -> &Mutex<dyn MqttClient> {
        self.mqtt_client.as_ref()
    }

    fn nats_client(&self) -> Option<&dyn NatsClient> {
        self.nats_client.as_deref()
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct AppMessageContext<'a, C: GlobalContext> {
    global_context: &'a C,
    start_timestamp: DateTime<Utc>,
}

impl<'a, C: GlobalContext> AppMessageContext<'a, C> {
    pub fn new(global_context: &'a C, start_timestamp: DateTime<Utc>) -> Self {
        Self {
            global_context,
            start_timestamp,
        }
    }
}

impl<'a, C: GlobalContext> GlobalContext for AppMessageContext<'a, C> {
    fn authz(&self) -> &Authz {
        self.global_context.authz()
    }

    fn config(&self) -> &Config {
        self.global_context.config()
    }

    fn db(&self) -> &sqlx::PgPool {
        self.global_context.db()
    }

    fn agent_id(&self) -> &AgentId {
        self.global_context.agent_id()
    }

    fn janus_clients(&self) -> Clients {
        self.global_context.janus_clients()
    }

    fn redis_pool(&self) -> &Option<RedisConnectionPool> {
        self.global_context.redis_pool()
    }

    fn metrics(&self) -> Arc<Metrics> {
        self.global_context.metrics()
    }

    fn mqtt_gateway_client(&self) -> &MqttGatewayHttpClient {
        self.global_context.mqtt_gateway_client()
    }

    fn conference_client(&self) -> &ConferenceHttpClient {
        self.global_context.conference_client()
    }

    fn mqtt_client(&self) -> &Mutex<dyn MqttClient> {
        self.global_context.mqtt_client()
    }

    fn nats_client(&self) -> Option<&dyn NatsClient> {
        self.global_context.nats_client()
    }
}

impl<'a, C: GlobalContext> MessageContext for AppMessageContext<'a, C> {
    fn start_timestamp(&self) -> DateTime<Utc> {
        self.start_timestamp
    }
}

impl<'a, C: GlobalContext> Context for AppMessageContext<'a, C> {}
