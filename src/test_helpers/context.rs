use std::sync::Arc;

use chrono::{DateTime, Utc};
use prometheus::Registry;
use serde_json::json;
use svc_agent::AgentId;
use svc_authz::{cache::ConnectionPool as RedisConnectionPool, ClientMap as Authz};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    app::{
        context::{Context, GlobalContext, MessageContext},
        metrics::Metrics,
    },
    backend::janus::{client::IncomingEvent, client_pool::Clients},
    clients::mqtt_gateway::MqttGatewayHttpClient,
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
        "http_addr": "0.0.0.0:1239",
        "broker_id": broker_id,
        "id_token": {
            "algorithm": "ES256",
            "key": "data/keys/svc.private_key.p8.der.sample",
        },
        "authz": {},
        "authn": {},
        "mqtt_api_host_uri": "http://0.0.0.0:3030",
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
        "orphaned_room_timeout": "1 seconds",
        "janus_registry": {
            "token": "test",
            "bind_addr": "0.0.0.0:1235"

        },
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
    start_timestamp: DateTime<Utc>,
    clients: Option<Clients>,
    mqtt_gateway_client: MqttGatewayHttpClient,
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
            start_timestamp: Utc::now(),
            clients: None,
            mqtt_gateway_client: MqttGatewayHttpClient::new(
                "test".to_owned(),
                config.mqtt_api_host_uri.clone(),
            ),
        }
    }

    pub fn with_janus(&mut self, events_sink: UnboundedSender<IncomingEvent>) {
        self.clients = Some(Clients::new(events_sink, None, self.db().clone()));
    }

    pub fn with_grouped_janus(&mut self, group: &str, events_sink: UnboundedSender<IncomingEvent>) {
        self.clients = Some(Clients::new(
            events_sink,
            Some(group.to_string()),
            self.db().clone(),
        ));
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

    fn mqtt_gateway_client(&self) -> &MqttGatewayHttpClient {
        &self.mqtt_gateway_client
    }
}

impl MessageContext for TestContext {
    fn start_timestamp(&self) -> DateTime<Utc> {
        self.start_timestamp
    }
}

impl Context for TestContext {}
