use super::client::{IncomingEvent, JanusClient, SessionId};
use crate::db::janus_backend;
use async_std::{channel::Sender, task::JoinHandle};
use slog::{error, warn};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};
use svc_agent::AgentId;

#[derive(Debug, CLone)]
pub struct Clients {
    clients: Arc<RwLock<HashMap<AgentId, ClientPoller>>>,
    events_sink: Sender<IncomingEvent>,
}

impl Clients {
    pub fn new(events_sink: Sender<IncomingEvent>) -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            events_sink,
        }
    }

    pub fn get_client(&self, agent_id: &AgentId) -> Option<JanusClient> {
        let guard = self.clients.read().expect("Must not panic");
        Some(guard.get(agent_id)?.client.clone())
    }

    pub fn put_client(&self, janus_backend: &janus_backend::Object) -> anyhow::Result<()> {
        let client = JanusClient::new(janus_backend.janus_url());
        let guard = self.clients.write().expect("Must not panic");
        match guard.entry(janus_backend.id()) {}
    }

    pub fn remove_client(&self, agent_id: &AgentId) {
        let guard = self.clients.write().expect("Must not panic");
        guard.remove(agent_id);
    }
}

#[derive(Debug, Clone)]
struct ClientPoller {
    session_id: SessionId,
    client: JanusClient,
}

struct PollerGuard {
    clients: Clients,
    agent_id: AgentId,
}

impl Drop for PollerGuard {
    fn drop(&mut self) {
        self.clients.remove_client(&self.agent_id)
    }
}

pub async fn start_polling(
    janus_client: JanusClient,
    session_id: SessionId,
    sink: async_std::channel::Sender<IncomingEvent>,
) {
    loop {
        let poll_result = janus_client.poll(session_id).await;
        match poll_result {
            Ok(PollResult::SessionNotFound) => {
                warn!(crate::LOG, "Session {} not found", session_id);
            }
            Ok(PollResult::Continue) => {
                continue;
            }
            Ok(PollResult::Events(events)) => {
                for event in events {
                    sink.send(event).await.expect("Receiver must exist");
                }
            }
            Err(err) => {
                error!(crate::LOG, "Polling error for {}: {:#}", session_id, err);
                async_std::task::sleep(Duration::from_millis(100)).await
            }
        }
    }
}
