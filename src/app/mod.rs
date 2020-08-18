use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{Context as AnyhowContext, Result};
use async_std::task;
use chrono::Utc;
use futures::StreamExt;
use log::{error, info, warn};
use serde_json::json;
use svc_agent::{
    mqtt::{
        Agent, AgentBuilder, AgentNotification, ConnectionMode, OutgoingRequest,
        OutgoingRequestProperties, QoS, ShortTermTimingProperties, SubscriptionTopic,
    },
    AccountId, AgentId, Authenticable, SharedGroup, Subscription,
};
use svc_authn::token::jws_compact;
use svc_authz::cache::Cache as AuthzCache;
use svc_error::{extension::sentry, Error as SvcError};

use crate::app::context::Context;
use crate::config::{self, Config, KruonisConfig};
use crate::db::ConnectionPool;
use context::{AppContext, JanusTopics};
use message_handler::MessageHandler;

pub(crate) const API_VERSION: &str = "v1";

////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn run(db: &ConnectionPool, authz_cache: Option<AuthzCache>) -> Result<()> {
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
        .context("Error creating an id token")?;

    let mut agent_config = config.mqtt.clone();
    agent_config.set_password(&token);

    let (mut agent, rx) = AgentBuilder::new(agent_id.clone(), API_VERSION)
        .connection_mode(ConnectionMode::Service)
        .start(&agent_config)
        .context("Failed to create an agent")?;

    // Event loop for incoming messages of MQTT Agent
    let (mq_tx, mut mq_rx) = futures_channel::mpsc::unbounded::<AgentNotification>();

    thread::Builder::new()
        .name("conference-notifications-loop".to_owned())
        .spawn(move || {
            for message in rx {
                if mq_tx.unbounded_send(message).is_err() {
                    error!("Error sending message to the internal channel");
                }
            }
        })
        .expect("Failed to start conference notifications loop");

    // Authz
    let authz = svc_authz::ClientMap::new(&config.id, authz_cache, config.authz.clone())
        .context("Error converting authz config to clients")?;

    // Sentry
    if let Some(sentry_config) = config.sentry.as_ref() {
        svc_error::extension::sentry::init(sentry_config);
    }

    // Subscribe to topics
    let janus_topics = subscribe(&mut agent, &agent_id, &config)?;

    // Context
    let context = AppContext::new(config.clone(), authz, db.clone(), janus_topics)
        .add_queue_counter(agent.get_queue_counter());

    // Message handler
    let message_handler = Arc::new(MessageHandler::new(agent.clone(), context));

    // Message loop
    let term_check_period = Duration::from_secs(1);
    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::SIGTERM, Arc::clone(&term))?;
    signal_hook::flag::register(signal_hook::SIGINT, Arc::clone(&term))?;

    while !term.load(Ordering::Relaxed) {
        let fut = async_std::future::timeout(term_check_period, mq_rx.next());

        if let Ok(Some(message)) = fut.await {
            let message_handler = message_handler.clone();

            task::spawn_blocking(move || match message {
                AgentNotification::Message(message, metadata) => {
                    async_std::task::block_on(message_handler.handle(&message, &metadata.topic));
                }
                AgentNotification::Disconnection => error!("Disconnected from broker"),
                AgentNotification::Reconnection => {
                    error!("Reconnected to broker");

                    resubscribe(
                        &mut message_handler.agent().to_owned(),
                        message_handler.context().agent_id(),
                        message_handler.context().config(),
                    );
                }
                AgentNotification::Puback(_) => (),
                AgentNotification::Pubrec(_) => (),
                AgentNotification::Pubcomp(_) => (),
                AgentNotification::Suback(_) => (),
                AgentNotification::Unsuback(_) => (),
                AgentNotification::Abort(err) => error!("MQTT client aborted: {}", err),
            });
        }
    }

    Ok(())
}

fn subscribe(agent: &mut Agent, agent_id: &AgentId, config: &Config) -> Result<JanusTopics> {
    let group = SharedGroup::new("loadbalancer", agent_id.as_account_id().clone());

    // Multicast requests
    agent
        .subscribe(
            &Subscription::multicast_requests(Some(API_VERSION)),
            QoS::AtMostOnce,
            Some(&group),
        )
        .context("Error subscribing to multicast requests")?;

    // Unicast requests
    agent
        .subscribe(&Subscription::unicast_requests(), QoS::AtMostOnce, None)
        .context("Error subscribing to unicast requests")?;

    // Janus status events
    let subscription = Subscription::broadcast_events(&config.backend_id, API_VERSION, "status");

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
        .context("Error subscribing to backend events topic")?;

    let janus_status_events_topic = subscription
        .subscription_topic(agent_id, API_VERSION)
        .context("Error building janus events subscription topic")?;

    // Janus events
    let subscription = Subscription::broadcast_events(&config.backend_id, API_VERSION, "events");

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
        .context("Error subscribing to backend events topic")?;

    let janus_events_topic = subscription
        .subscription_topic(agent_id, API_VERSION)
        .context("Error building janus events subscription topic")?;

    // Janus responses
    let subscription = Subscription::unicast_responses_from(&config.backend_id);

    agent
        .subscribe(&subscription, QoS::AtLeastOnce, Some(&group))
        .context("Error subscribing to backend responses topic")?;

    let janus_responses_topic = subscription
        .subscription_topic(agent_id, API_VERSION)
        .context("Error building janus responses subscription topic")?;

    // Kruonis
    if let KruonisConfig {
        id: Some(ref kruonis_id),
    } = config.kruonis
    {
        subscribe_to_kruonis(kruonis_id, agent)?;
    }

    // Return Janus subscription topics
    Ok(JanusTopics::new(
        &janus_status_events_topic,
        &janus_events_topic,
        &janus_responses_topic,
    ))
}

fn subscribe_to_kruonis(kruonis_id: &AccountId, agent: &mut Agent) -> Result<()> {
    let timing = ShortTermTimingProperties::new(Utc::now());

    let topic = Subscription::unicast_requests_from(kruonis_id)
        .subscription_topic(agent.id(), API_VERSION)
        .context("Failed to build subscription topic")?;

    let props = OutgoingRequestProperties::new("kruonis.subscribe", &topic, "", timing);
    let event = OutgoingRequest::multicast(json!({}), props, kruonis_id);

    agent.publish(event).context("Failed to publish message")?;

    Ok(())
}

fn resubscribe(agent: &mut Agent, agent_id: &AgentId, config: &Config) {
    if let Err(err) = subscribe(agent, agent_id, config) {
        let err = format!("Failed to resubscribe after reconnection: {}", err);
        error!("{}", err);

        let svc_error = SvcError::builder()
            .kind("resubscription_error", "Resubscription error")
            .detail(&err)
            .build();

        sentry::send(svc_error)
            .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
    }
}

pub(crate) mod context;
pub(crate) mod endpoint;
pub(crate) mod handle_id;
mod janus;
pub(crate) mod message_handler;
