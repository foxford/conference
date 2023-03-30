use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use httpmock::MockServer;
use parking_lot::Mutex;
use prometheus::Registry;
use serde_json::json;
use svc_agent::AgentId;
use svc_authz::{cache::ConnectionPool as RedisConnectionPool, ClientMap as Authz};
use svc_nats_client::{
    Event, Message, MessageStream, NatsClient, PublishError, SubscribeError, TermMessageError,
};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    app::{
        context::{Context, GlobalContext, MessageContext},
        metrics::Metrics,
    },
    backend::janus::{client::IncomingEvent, client_pool::Clients},
    client::{
        conference::ConferenceHttpClient, mqtt::MqttClient, mqtt_gateway::MqttGatewayHttpClient,
    },
    config::Config,
    db::ConnectionPool as Db,
};

use super::{authz::TestAuthz, db::TestDb, SVC_AUDIENCE, USR_AUDIENCE};

///////////////////////////////////////////////////////////////////////////////

fn build_config(mock: &MockServer) -> Config {
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
        "mqtt_api_host_uri": mock.base_url(),
        "mqtt": {
            "uri": "mqtt://0.0.0.0:1883",
            "clean_session": false,
        },
        "outbox": {
            "messages_per_try": 20,
            "try_wake_interval": 60,
            "max_delivery_interval": 86400,
        },
        "nats": {
            "url": "nats://0.0.0.0:4222",
            "creds": "nats.creds"
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

struct TestNatsClient;

#[async_trait]
impl NatsClient for TestNatsClient {
    async fn publish(&self, _event: &Event) -> Result<(), PublishError> {
        Ok(())
    }

    async fn subscribe(&self) -> Result<MessageStream, SubscribeError> {
        unimplemented!()
    }

    async fn term_message(&self, _message: Message) -> Result<(), TermMessageError> {
        unimplemented!()
    }
}

struct TestMqttClient;

impl MqttClient for TestMqttClient {
    fn publish(&mut self, _label: &'static str, _path: &str) -> Result<(), svc_agent::Error> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct TestContext {
    config: Config,
    authz: Authz,
    db: TestDb,
    agent_id: AgentId,
    start_timestamp: DateTime<Utc>,
    clients: Option<Clients>,
    mqtt_gateway_client: MqttGatewayHttpClient,
    conference_client: ConferenceHttpClient,
    mqtt_client: Arc<Mutex<dyn MqttClient>>,
    nats_client: Option<Arc<dyn NatsClient>>,
}

const WAITLIST_DURATION: std::time::Duration = std::time::Duration::from_secs(10);

impl TestContext {
    pub fn new(db: TestDb, authz: TestAuthz) -> Self {
        // can be safely dropped
        let mock_server = MockServer::start();
        let _subscriptions_mock = mock_server.mock(|when, then| {
            when.path("/api/v1/subscriptions")
                .method(httpmock::Method::POST);
            then.status(200);
        });

        let config = build_config(&mock_server);
        let agent_id = AgentId::new(&config.agent_label, config.id.clone());
        let mqtt_api_host_uri = config.mqtt_api_host_uri.clone();

        Self {
            config,
            authz: authz.into(),
            db,
            agent_id,
            start_timestamp: Utc::now(),
            clients: None,
            mqtt_gateway_client: MqttGatewayHttpClient::new("test".to_owned(), mqtt_api_host_uri),
            conference_client: ConferenceHttpClient::new("test".to_owned()),
            mqtt_client: Arc::new(Mutex::new(TestMqttClient)),
            nats_client: Some(Arc::new(TestNatsClient {}) as Arc<dyn NatsClient>),
        }
    }

    pub fn with_janus(&mut self, events_sink: UnboundedSender<IncomingEvent>) {
        self.clients = Some(Clients::new(
            events_sink,
            None,
            self.db().clone(),
            WAITLIST_DURATION,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            None,
        ));
    }

    pub fn with_grouped_janus(&mut self, group: &str, events_sink: UnboundedSender<IncomingEvent>) {
        self.clients = Some(Clients::new(
            events_sink,
            Some(group.to_string()),
            self.db().clone(),
            WAITLIST_DURATION,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            None,
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

    fn janus_clients(&self) -> Clients {
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

    fn conference_client(&self) -> &ConferenceHttpClient {
        &self.conference_client
    }

    fn mqtt_client(&self) -> Arc<Mutex<dyn MqttClient>> {
        self.mqtt_client.clone()
    }

    fn nats_client(&self) -> Option<&dyn NatsClient> {
        self.nats_client.as_deref()
    }
}

impl MessageContext for TestContext {
    fn start_timestamp(&self) -> DateTime<Utc> {
        self.start_timestamp
    }
}

impl Context for TestContext {}
