use super::client::{IncomingEvent, JanusClient, PollResult, SessionId};
use crate::db::janus_backend;
use async_std::channel::Sender;
use slog::{error, warn};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, RwLock},
    time::Duration,
};
use svc_agent::AgentId;

#[derive(Debug, Clone)]
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
        let mut guard = self.clients.write().expect("Must not panic");
        match guard.entry(janus_backend.id().clone()) {
            Entry::Occupied(_) => Err(anyhow!("Client already exist")),
            Entry::Vacant(v) => {
                let this = self.clone();
                let client = JanusClient::new(janus_backend.janus_url().parse()?)?;
                let session_id = janus_backend.session_id();
                let agent_id = janus_backend.id().clone();
                v.insert(ClientPoller {
                    session_id: janus_backend.session_id(),
                    client: client.clone(),
                });
                async_std::task::spawn(async move {
                    let sink = this.events_sink.clone();
                    let _guard = PollerGuard {
                        clients: this,
                        agent_id,
                    };
                    start_polling(client, session_id, sink).await;
                });
                Ok(())
            }
        }
    }

    pub fn remove_client(&self, agent_id: &AgentId) {
        let mut guard = self.clients.write().expect("Must not panic");
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

async fn start_polling(
    janus_client: JanusClient,
    session_id: SessionId,
    sink: async_std::channel::Sender<IncomingEvent>,
) {
    loop {
        let poll_result = janus_client.poll(session_id).await;
        match poll_result {
            Ok(PollResult::SessionNotFound) => {
                warn!(crate::LOG, "Session {} not found", session_id);
                break;
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
                async_std::task::sleep(Duration::from_millis(500)).await
            }
        }
    }
}
