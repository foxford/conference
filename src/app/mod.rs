use std::sync::Arc;
use std::thread;

use async_std::task;
use chrono::Utc;
use futures::StreamExt;
use log::{error, info};
use serde_json::json;
use svc_agent::{
    mqtt::{
        Agent, AgentBuilder, ConnectionMode, IntoPublishableDump, Notification, OutgoingRequest,
        OutgoingRequestProperties, QoS, ShortTermTimingProperties, SubscriptionTopic,
    },
    AccountId, AgentId, Authenticable, SharedGroup, Subscription,
};
use svc_authn::token::jws_compact;
use svc_authz::cache::Cache as AuthzCache;

use crate::config::{self, KruonisConfig};
use crate::db::ConnectionPool;
use context::{AppContext, JanusTopics};
use message_handler::MessageHandler;

pub(crate) const API_VERSION: &str = "v1";

////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn run(
    db: &ConnectionPool,
    authz_cache: Option<AuthzCache>,
) -> Result<(), String> {
    // Config
    let config = config::load().expect("Failed to load config");
    info!("App config: {:?}", config);

    // Agent
    let agent_id = AgentId::new(&config.agent_label, config.id.clone());
    info!("Agent id: {:?}", &agent_id);

    let token = jws_compact::TokenBuilder::new()
        .issuer(&agent_id.as_account_id().audience().to_string())
        .subject(&agent_id)
        .key(config.id_token.algorithm, config.id_token.key.as_slice())
        .build()
        .map_err(|err| format!("Error creating an id token: {}", err))?;

    let mut agent_config = config.mqtt.clone();
    agent_config.set_password(&token);

    let (mut agent, rx) = AgentBuilder::new(agent_id.clone(), API_VERSION)
        .connection_mode(ConnectionMode::Service)
        .start(&agent_config)
        .map_err(|err| format!("Failed to create an agent: {}", err))?;

    // Event loop for incoming messages of MQTT Agent
    let (mq_tx, mut mq_rx) = futures_channel::mpsc::unbounded::<Notification>();

    thread::spawn(move || {
        for message in rx {
            if let Err(_) = mq_tx.unbounded_send(message) {
                error!("Error sending message to the internal channel");
            }
        }
    });

    // Authz
    let authz = svc_authz::ClientMap::new(&config.id, authz_cache, config.authz.clone())
        .map_err(|err| format!("Error converting authz config to clients: {}", err))?;

    // Sentry
    if let Some(sentry_config) = config.sentry.as_ref() {
        svc_error::extension::sentry::init(sentry_config);
    }

    // Subscribe to multicast requests
    let group = SharedGroup::new("loadbalancer", agent_id.as_account_id().clone());

    agent
        .subscribe(
            &Subscription::multicast_requests(Some(API_VERSION)),
            QoS::AtMostOnce,
            Some(&group),
        )
        .map_err(|err| format!("Error subscribing to multicast requests: {}", err))?;

    // Subscribe to Janus status
    let subscription = Subscription::broadcast_events(&config.backend_id, API_VERSION, "status");

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
        .map_err(|err| format!("Error subscribing to backend events topic: {}", err))?;

    let janus_status_events_topic = subscription
        .subscription_topic(&agent_id, API_VERSION)
        .map_err(|err| format!("Error building janus events subscription topic: {}", err))?;

    // Subscribe to Janus events
    let subscription = Subscription::broadcast_events(&config.backend_id, API_VERSION, "events");

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
        .map_err(|err| format!("Error subscribing to backend events topic: {}", err))?;

    let janus_events_topic = subscription
        .subscription_topic(&agent_id, API_VERSION)
        .map_err(|err| format!("Error building janus events subscription topic: {}", err))?;

    // Subscribe to Janus responses
    let subscription = Subscription::unicast_responses_from(&config.backend_id);

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
        .map_err(|err| format!("Error subscribing to backend responses topic: {}", err))?;

    agent
        .subscribe(
            &Subscription::unicast_requests(),
            QoS::AtMostOnce,
            Some(&group),
        )
        .map_err(|err| format!("Error subscribing to unicast requests: {}", err))?;

    if let KruonisConfig {
        id: Some(ref kruonis_id),
    } = config.kruonis
    {
        subscribe_to_kruonis(kruonis_id, &mut agent)?;
    }

    let janus_responses_topic = subscription
        .subscription_topic(&agent_id, API_VERSION)
        .map_err(|err| format!("Error building janus responses subscription topic: {}", err))?;

    // Context
    let janus_topics = JanusTopics::new(
        &janus_status_events_topic,
        &janus_events_topic,
        &janus_responses_topic,
    );

    let context = AppContext::new(config, authz, db.clone(), janus_topics);

    // Message handler
    let message_handler = Arc::new(MessageHandler::new(agent, context));

    // Message loop
    while let Some(message) = mq_rx.next().await {
        let message_handler = message_handler.clone();

        task::spawn(async move {
            match message {
                svc_agent::mqtt::Notification::Publish(message) => {
                    message_handler
                        .handle(&message.payload, &message.topic_name)
                        .await
                }
                _ => error!("Unsupported notification type = '{:?}'", message),
            }
        });
    }

    Ok(())
}

fn subscribe_to_kruonis(kruonis_id: &AccountId, agent: &mut Agent) -> Result<(), String> {
    let timing = ShortTermTimingProperties::new(Utc::now());
    let topic = Subscription::unicast_requests_from(kruonis_id)
        .subscription_topic(agent.id(), API_VERSION)
        .map_err(|err| format!("Failed to build subscription topic: {:?}", err))?;
    let props = OutgoingRequestProperties::new("kruonis.subscribe", &topic, "", timing);
    let event = OutgoingRequest::multicast(json!({}), props, kruonis_id);
    let message = Box::new(event) as Box<dyn IntoPublishableDump + Send>;

    let dump = message
        .into_dump(agent.address())
        .map_err(|err| format!("Failed to dump message: {}", err))?;

    info!(
        "Outgoing message = '{}' sending to the topic = '{}'",
        dump.payload(),
        dump.topic(),
    );

    agent
        .publish_dump(dump)
        .map_err(|err| format!("Failed to publish message: {}", err))?;
    Ok(())
}

pub(crate) mod context;
pub(crate) mod endpoint;
pub(crate) mod handle_id;
mod janus;
pub(crate) mod message_handler;
