use std::sync::Arc;

use svc_agent::{queue_counter::QueueCounterHandle, AgentId};
use svc_authz::cache::ConnectionPool as RedisConnectionPool;
use svc_authz::ClientMap as Authz;

use crate::app::metrics::{DbPoolStatsCollector, DynamicStatsCollector};
use crate::backend::janus::Client as JanusClient;
use crate::config::Config;
use crate::db::ConnectionPool as Db;

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
    db_pool_stats: Option<DbPoolStatsCollector>,
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
            db_pool_stats: None,
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

    pub(crate) fn db_pool_stats(self, stats: DbPoolStatsCollector) -> Self {
        Self {
            db_pool_stats: Some(stats),
            ..self
        }
    }
}

pub(crate) trait Context: Sync {
    fn authz(&self) -> &Authz;
    fn config(&self) -> &Config;
    fn db(&self) -> &Db;
    fn agent_id(&self) -> &AgentId;
    fn janus_client(&self) -> Arc<JanusClient>;
    fn janus_topics(&self) -> &JanusTopics;
    fn queue_counter(&self) -> &Option<QueueCounterHandle>;
    fn redis_pool(&self) -> &Option<RedisConnectionPool>;
    fn db_pool_stats(&self) -> &Option<DbPoolStatsCollector>;
    fn dynamic_stats(&self) -> Option<&DynamicStatsCollector>;
}

impl Context for AppContext {
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

    fn db_pool_stats(&self) -> &Option<DbPoolStatsCollector> {
        &self.db_pool_stats
    }

    fn dynamic_stats(&self) -> Option<&DynamicStatsCollector> {
        self.dynamic_stats.as_deref()
    }
}

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
