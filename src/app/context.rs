use std::sync::Arc;

use svc_agent::AgentId;
use svc_authz::ClientMap as Authz;

use crate::config::Config;
use crate::db::ConnectionPool as Db;

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub(crate) struct AppContext {
    config: Arc<Config>,
    authz: Authz,
    db: Db,
    agent_id: AgentId,
    janus_topics: JanusTopics,
}

impl AppContext {
    pub(crate) fn new(config: Config, authz: Authz, db: Db, janus_topics: JanusTopics) -> Self {
        let agent_id = AgentId::new(&config.agent_label, config.id.to_owned());

        Self {
            config: Arc::new(config),
            authz,
            db,
            agent_id,
            janus_topics,
        }
    }
}

pub(crate) trait Context: Sync {
    fn authz(&self) -> &Authz;
    fn config(&self) -> &Config;
    fn db(&self) -> &Db;
    fn agent_id(&self) -> &AgentId;
    fn janus_topics(&self) -> &JanusTopics;
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

    fn janus_topics(&self) -> &JanusTopics {
        &self.janus_topics
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
