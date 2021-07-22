use std::sync::Arc;

use chrono::{DateTime, Utc};
use crossbeam_channel::Sender;
use prometheus::Registry;
use serde_json::json;
use slog::{o, Logger, OwnedKV, SendSyncRefUnwindSafeKV};
use svc_agent::AgentId;
use svc_authz::{cache::ConnectionPool as RedisConnectionPool, ClientMap as Authz};

use crate::{
    app::{
        context::{Context, GlobalContext, JanusTopics, MessageContext},
        metrics::Metrics,
    },
    backend::janus::{client::IncomingEvent, client_pool::Clients},
    config::Config,
    db::ConnectionPool as Db,
};

use super::{authz::TestAuthz, db::TestDb, SVC_AUDIENCE, USR_AUDIENCE};

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
        "metrics": {
            "http": {
                "bind_address": "0.0.0.0:1234",
            },
            "janus_metrics_collect_interval": "100 seconds"
        },
        "backend": {
            "id": backend_id,
            "default_timeout": 5,
            "stream_upload_timeout": 600,
            "transaction_watchdog_check_period": 1,
        },
        "upload": {
            "shared": {
                USR_AUDIENCE: {
                    "backend": "EXAMPLE",
                    "bucket": format!("origin.webinar.{}", USR_AUDIENCE),
                }
            },
            "owned": {
                USR_AUDIENCE: {
                    "backend": "EXAMPLE",
                    "bucket": format!("origin.minigroup.{}", USR_AUDIENCE),
                }
            }
        },
        "max_room_duration": 7,
    });

    serde_json::from_value::<Config>(config).expect("Failed to parse test config")
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct TestContext {
    config: Config,
    authz: Authz,
    db: TestDb,
    agent_id: AgentId,
    janus_topics: JanusTopics,
    logger: Logger,
    start_timestamp: DateTime<Utc>,
    clients: Option<Clients>,
}

impl TestContext {
    pub fn new(db: TestDb, authz: TestAuthz) -> Self {
        let config = build_config();
        let agent_id = AgentId::new(&config.agent_label, config.id.clone());

        Self {
            config,
            authz: authz.into(),
            db,
            agent_id,
            janus_topics: JanusTopics::new("ignore"),
            logger: crate::LOG.new(o!()),
            start_timestamp: Utc::now(),
            clients: None,
        }
    }

    pub fn with_janus(&mut self, events_sink: Sender<IncomingEvent>) {
        self.clients = Some(Clients::new(events_sink, None));
    }

    pub fn config_mut(&mut self) -> &mut Config {
        &mut self.config
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

    fn janus_topics(&self) -> &JanusTopics {
        &self.janus_topics
    }

    fn redis_pool(&self) -> &Option<RedisConnectionPool> {
        &None
    }

    fn janus_clients(&self) -> crate::backend::janus::client_pool::Clients {
        self.clients
            .as_ref()
            .expect("You should initialise janus")
            .clone()
    }

    fn metrics(&self) -> Arc<Metrics> {
        let registry = Registry::new();
        Arc::new(Metrics::new(&registry).unwrap())
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
