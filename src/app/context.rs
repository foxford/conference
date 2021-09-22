use std::sync::Arc;

use chrono::{DateTime, Utc};
use diesel::{
    pg::PgConnection,
    r2d2::{ConnectionManager, PooledConnection},
};
use futures::{future::BoxFuture, FutureExt};
use svc_agent::AgentId;
use svc_authz::{cache::ConnectionPool as RedisConnectionPool, ClientMap as Authz};


use crate::{
    app::error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind},
    backend::janus::client_pool::Clients,
    config::Config,
    db::ConnectionPool as Db,
};

use super::metrics::Metrics;

///////////////////////////////////////////////////////////////////////////////

pub trait Context: GlobalContext + MessageContext {}

pub trait GlobalContext: Sync {
    fn authz(&self) -> &Authz;
    fn config(&self) -> &Config;
    fn db(&self) -> &Db;
    fn agent_id(&self) -> &AgentId;
    fn janus_clients(&self) -> Clients;
    fn janus_topics(&self) -> &JanusTopics;
    fn redis_pool(&self) -> &Option<RedisConnectionPool>;
    fn metrics(&self) -> Arc<Metrics>;

    fn get_conn(
        &self,
    ) -> BoxFuture<Result<PooledConnection<ConnectionManager<PgConnection>>, AppError>> {
        let db = self.db().clone();
        async move {
            crate::util::spawn_blocking(move || {
                db.get()
                    .map_err(|err| {
                        anyhow::Error::from(err).context("Failed to acquire DB connection")
                    })
                    .error(AppErrorKind::DbConnAcquisitionFailed)
            })
            .await
        }
        .boxed()
    }
}

pub trait MessageContext: Send {
    fn start_timestamp(&self) -> DateTime<Utc>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct AppContext {
    config: Arc<Config>,
    authz: Authz,
    db: Db,
    agent_id: AgentId,
    janus_topics: JanusTopics,
    redis_pool: Option<RedisConnectionPool>,
    clients: Clients,
    metrics: Arc<Metrics>,
}

impl AppContext {
    pub fn new(
        config: Config,
        authz: Authz,
        db: Db,
        janus_topics: JanusTopics,
        clients: Clients,
        metrics: Arc<Metrics>,
    ) -> Self {
        let agent_id = AgentId::new(&config.agent_label, config.id.to_owned());

        Self {
            config: Arc::new(config),
            authz,
            db,
            agent_id,
            janus_topics,
            redis_pool: None,
            clients,
            metrics,
        }
    }

    pub fn add_redis_pool(self, pool: RedisConnectionPool) -> Self {
        Self {
            redis_pool: Some(pool),
            ..self
        }
    }
}

impl GlobalContext for AppContext {
    fn authz(&self) -> &Authz {
        &self.authz
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn db(&self) -> &Db {
        &self.db
    }

    fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    fn janus_topics(&self) -> &JanusTopics {
        &self.janus_topics
    }

    fn redis_pool(&self) -> &Option<RedisConnectionPool> {
        &self.redis_pool
    }

    fn janus_clients(&self) -> Clients {
        self.clients.clone()
    }

    fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
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

    fn db(&self) -> &Db {
        self.global_context.db()
    }

    fn agent_id(&self) -> &AgentId {
        self.global_context.agent_id()
    }

    fn janus_topics(&self) -> &JanusTopics {
        self.global_context.janus_topics()
    }

    fn redis_pool(&self) -> &Option<RedisConnectionPool> {
        self.global_context.redis_pool()
    }

    fn janus_clients(&self) -> Clients {
        self.global_context.janus_clients()
    }

    fn metrics(&self) -> Arc<Metrics> {
        self.global_context.metrics()
    }
}

impl<'a, C: GlobalContext> MessageContext for AppMessageContext<'a, C> {
    fn start_timestamp(&self) -> DateTime<Utc> {
        self.start_timestamp
    }
}

impl<'a, C: GlobalContext> Context for AppMessageContext<'a, C> {}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct JanusTopics {
    status_events_topic: String,
}

impl JanusTopics {
    pub fn new(status_events_topic: &str) -> Self {
        Self {
            status_events_topic: status_events_topic.to_owned(),
        }
    }

    pub fn status_events_topic(&self) -> &str {
        &self.status_events_topic
    }
}
