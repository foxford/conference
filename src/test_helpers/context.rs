use std::sync::Arc;

use serde_json::json;
use svc_agent::{queue_counter::QueueCounterHandle, AgentId};
use svc_authz::cache::ConnectionPool as RedisConnectionPool;
use svc_authz::ClientMap as Authz;

use crate::app::context::{Context, JanusTopics};
use crate::app::janus::Client as JanusClient;
use crate::app::metrics::{DbPoolStatsCollector, DynamicStatsCollector};
use crate::config::Config;
use crate::db::ConnectionPool as Db;

use super::authz::TestAuthz;
use super::db::TestDb;
use super::SVC_AUDIENCE;

///////////////////////////////////////////////////////////////////////////////

fn build_config() -> Config {
    let id = format!("conference.{}", SVC_AUDIENCE);
    let broker_id = format!("mqtt-gateway.{}", SVC_AUDIENCE);
    let backend_id = format!("janus-gateway.{}", SVC_AUDIENCE);

    let config = json!({
        "id": id,
        "agent_label": "alpha",
        "broker_id": broker_id,
        "backend_id": backend_id,
        "id_token": {
            "algorithm": "ES256",
            "key": "data/keys/svc.private_key.p8.der.sample",
        },
        "authz": {},
        "mqtt": {
            "uri": "mqtt://0.0.0.0:1883",
            "clean_session": false,
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
}

impl TestContext {
    pub(crate) fn new(db: TestDb, authz: TestAuthz) -> Self {
        let config = build_config();
        let agent_id = AgentId::new(&config.agent_label, config.id.clone());

        let janus_client =
            JanusClient::start(agent_id.clone()).expect("Failed to start janus client");

        Self {
            config,
            authz: authz.into(),
            db,
            agent_id,
            janus_client: Arc::new(janus_client),
            janus_topics: JanusTopics::new("ignore", "ignore", "ignore"),
        }
    }
}

impl Context for TestContext {
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

    fn db_pool_stats(&self) -> &Option<DbPoolStatsCollector> {
        &None
    }

    fn dynamic_stats(&self) -> Option<&DynamicStatsCollector> {
        None
    }
}
