use std::sync::Arc;

use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, PooledConnection};
use slog::{Logger, OwnedKV, SendSyncRefUnwindSafeKV};
use svc_agent::{queue_counter::QueueCounterHandle, AgentId};
use svc_authz::cache::ConnectionPool as RedisConnectionPool;
use svc_authz::ClientMap as Authz;

use crate::app::error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind};
use crate::app::metrics::{DynamicStatsCollector, Metric};
use crate::backend::janus::Client as JanusClient;
use crate::config::Config;
use crate::db::ConnectionPool as Db;

///////////////////////////////////////////////////////////////////////////////

pub(crate) trait Context: GlobalContext + MessageContext {}

pub(crate) trait GlobalContext: Sync {
    fn authz(&self) -> &Authz;
    fn config(&self) -> &Config;
    fn db(&self) -> &Db;
    fn agent_id(&self) -> &AgentId;
    fn janus_client(&self) -> Arc<JanusClient>;
    fn janus_topics(&self) -> &JanusTopics;
    fn queue_counter(&self) -> &Option<QueueCounterHandle>;
    fn redis_pool(&self) -> &Option<RedisConnectionPool>;
    fn dynamic_stats(&self) -> Option<&DynamicStatsCollector>;
    fn get_metrics(&self) -> anyhow::Result<Vec<Metric>>;

    fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, AppError> {
        self.db()
            .get()
            .map_err(|err| anyhow::Error::from(err).context("Failed to acquire DB connection"))
            .error(AppErrorKind::DbConnAcquisitionFailed)
    }
}

pub(crate) trait MessageContext: Send {
    fn start_timestamp(&self) -> DateTime<Utc>;
    fn logger(&self) -> &Logger;

    fn add_logger_tags<T>(&mut self, tags: OwnedKV<T>)
    where
        T: SendSyncRefUnwindSafeKV + Sized + 'static;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub(crate) struct AppContext {
    config: Arc<Config>,
    authz: Authz,
    db: Db,
    agent_id: AgentId,
    janus_client: Arc<JanusClient>,
    janus_topics: JanusTopics,
    queue_counter: Option<QueueCounterHandle>,
    redis_pool: Option<RedisConnectionPool>,
    dynamic_stats: Option<Arc<DynamicStatsCollector>>,
}

impl AppContext {
    pub(crate) fn new(
        config: Config,
        authz: Authz,
        db: Db,
        janus_client: JanusClient,
        janus_topics: JanusTopics,
    ) -> Self {
        let agent_id = AgentId::new(&config.agent_label, config.id.to_owned());

        Self {
            config: Arc::new(config),
            authz,
            db,
            agent_id,
            janus_client: Arc::new(janus_client),
            janus_topics,
            queue_counter: None,
            redis_pool: None,
            dynamic_stats: Some(Arc::new(DynamicStatsCollector::start())),
        }
    }

    pub(crate) fn add_queue_counter(self, qc: QueueCounterHandle) -> Self {
        Self {
            queue_counter: Some(qc),
            ..self
        }
    }

    pub(crate) fn add_redis_pool(self, pool: RedisConnectionPool) -> Self {
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

    fn janus_client(&self) -> Arc<JanusClient> {
        self.janus_client.clone()
    }

    fn janus_topics(&self) -> &JanusTopics {
        &self.janus_topics
    }

    fn queue_counter(&self) -> &Option<QueueCounterHandle> {
        &self.queue_counter
    }

    fn redis_pool(&self) -> &Option<RedisConnectionPool> {
        &self.redis_pool
    }

    fn dynamic_stats(&self) -> Option<&DynamicStatsCollector> {
        self.dynamic_stats.as_deref()
    }

    fn get_metrics(&self) -> anyhow::Result<Vec<Metric>> {
        crate::app::metrics::Collector::new(self).get()
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct AppMessageContext<'a, C: GlobalContext> {
    global_context: &'a C,
    start_timestamp: DateTime<Utc>,
    logger: Logger,
}

impl<'a, C: GlobalContext> AppMessageContext<'a, C> {
    pub(crate) fn new(global_context: &'a C, start_timestamp: DateTime<Utc>) -> Self {
        Self {
            global_context,
            start_timestamp,
            logger: crate::LOG.new(o!()),
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

    fn janus_client(&self) -> Arc<JanusClient> {
        self.global_context.janus_client()
    }

    fn janus_topics(&self) -> &JanusTopics {
        self.global_context.janus_topics()
    }

    fn queue_counter(&self) -> &Option<QueueCounterHandle> {
        self.global_context.queue_counter()
    }

    fn redis_pool(&self) -> &Option<RedisConnectionPool> {
        self.global_context.redis_pool()
    }

    fn dynamic_stats(&self) -> Option<&DynamicStatsCollector> {
        self.global_context.dynamic_stats()
    }

    fn get_metrics(&self) -> anyhow::Result<Vec<Metric>> {
        self.global_context.get_metrics()
    }
}

impl<'a, C: GlobalContext> MessageContext for AppMessageContext<'a, C> {
    fn start_timestamp(&self) -> DateTime<Utc> {
        self.start_timestamp
    }

    fn logger(&self) -> &Logger {
        &self.logger
    }

    fn add_logger_tags<T>(&mut self, tags: OwnedKV<T>)
    where
        T: SendSyncRefUnwindSafeKV + Sized + 'static,
    {
        self.logger = self.logger.new(tags);
    }
}

impl<'a, C: GlobalContext> Context for AppMessageContext<'a, C> {}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub(crate) struct JanusTopics {
    status_events_topic: String,
    events_topic: String,
    responses_topic: String,
}

impl JanusTopics {
    pub(crate) fn new(
        status_events_topic: &str,
        events_topic: &str,
        responses_topic: &str,
    ) -> Self {
        Self {
            status_events_topic: status_events_topic.to_owned(),
            events_topic: events_topic.to_owned(),
            responses_topic: responses_topic.to_owned(),
        }
    }

    pub(crate) fn status_events_topic(&self) -> &str {
        &self.status_events_topic
    }

    pub(crate) fn events_topic(&self) -> &str {
        &self.events_topic
    }

    pub(crate) fn responses_topic(&self) -> &str {
        &self.responses_topic
    }
}
