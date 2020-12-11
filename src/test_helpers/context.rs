use std::sync::{atomic::AtomicI64, Arc};

use chrono::{DateTime, Utc};
use serde_json::json;
use slog::{Logger, OwnedKV, SendSyncRefUnwindSafeKV};
use svc_agent::{queue_counter::QueueCounterHandle, AgentId};
use svc_authz::cache::ConnectionPool as RedisConnectionPool;
use svc_authz::ClientMap as Authz;

use crate::app::context::{Context, GlobalContext, JanusTopics, MessageContext};
use crate::app::metrics::DynamicStatsCollector;
use crate::backend::janus::Client as JanusClient;
use crate::config::Config;
use crate::db::ConnectionPool as Db;

use super::authz::TestAuthz;
use super::db::TestDb;
use super::{SVC_AUDIENCE, USR_AUDIENCE};

///////////////////////////////////////////////////////////////////////////////

fn build_config() -> Config {
    let id = format!("conference.{}", SVC_AUDIENCE);
    let broker_id = format!("mqtt-gateway.{}", SVC_AUDIENCE);
    let backend_id = format!("janus-gateway.{}", SVC_AUDIENCE);

    let config = json!({
        "id": id,
        "agent_label": "alpha",
        "broker_id": broker_id,
        "id_token": {
            "algorithm": "ES256",
            "key": "data/keys/svc.private_key.p8.der.sample",
        },
        "authz": {},
        "mqtt": {
            "uri": "mqtt://0.0.0.0:1883",
            "clean_session": false,
        },
        "backend": {
            "id": backend_id,
            "default_timeout": 5,
            "stream_upload_timeout": 600,
            "transaction_watchdog_check_period": 1,
        },
        "upload": {
            USR_AUDIENCE: {
                "backend": "EXAMPLE",
                "bucket": format!("origin.webinar.{}", USR_AUDIENCE),
            }
        }
    });

    serde_json::from_value::<Config>(config).expect("Failed to parse test config")
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub(crate) struct TestContext {
    config: Config,
    authz: Authz,
    db: TestDb,
    agent_id: AgentId,
    janus_client: Arc<JanusClient>,
    janus_topics: JanusTopics,
    logger: Logger,
    start_timestamp: DateTime<Utc>,
}

impl TestContext {
    pub(crate) fn new(db: TestDb, authz: TestAuthz) -> Self {
        let config = build_config();
        let agent_id = AgentId::new(&config.agent_label, config.id.clone());

        let janus_client = JanusClient::start(&config.backend, agent_id.clone(), None)
            .expect("Failed to start janus client");

        Self {
            config,
            authz: authz.into(),
            db,
            agent_id,
            janus_client: Arc::new(janus_client),
            janus_topics: JanusTopics::new("ignore", "ignore", "ignore"),
            logger: crate::LOG.new(o!()),
            start_timestamp: Utc::now(),
        }
    }
}

impl GlobalContext for TestContext {
    fn authz(&self) -> &Authz {
        &self.authz
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn db(&self) -> &Db {
        self.db.connection_pool()
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
        &None
    }

    fn redis_pool(&self) -> &Option<RedisConnectionPool> {
        &None
    }

    fn dynamic_stats(&self) -> Option<&DynamicStatsCollector> {
        None
    }

    fn get_metrics(&self) -> anyhow::Result<Vec<crate::app::metrics::Metric>> {
        Ok(vec![])
    }

    fn running_requests(&self) -> Option<Arc<AtomicI64>> {
        None
    }
}

impl MessageContext for TestContext {
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

impl Context for TestContext {}
