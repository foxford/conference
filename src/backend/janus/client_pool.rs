use super::client::{IncomingEvent, JanusClient, PollResult, SessionId};
use crate::db::janus_backend;
use crossbeam_channel::Sender;
use slog::{error, warn};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};
use svc_agent::AgentId;

#[derive(Debug, Clone)]
pub struct Clients {
    clients: Arc<RwLock<HashMap<AgentId, ClientHandle>>>,
    events_sink: Sender<IncomingEvent>,
    group: Option<String>,
}

impl Clients {
    pub fn new(events_sink: Sender<IncomingEvent>, group: Option<String>) -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            events_sink,
            group,
        }
    }

    pub fn get_or_insert(&self, backend: &janus_backend::Object) -> anyhow::Result<JanusClient> {
        self.get_client(backend)
            .map(Ok)
            .unwrap_or_else(|| self.put_client(backend))
    }

    fn get_client(&self, backend: &janus_backend::Object) -> Option<JanusClient> {
        let guard = self.clients.read().expect("Must not panic");
        let handle = guard.get(backend.id())?;
        if handle.janus_url != backend.janus_url() {
            drop(guard);
            self.remove_client(backend.id());
            None
        } else {
            Some(handle.client.clone())
        }
    }

    fn put_client(&self, backend: &janus_backend::Object) -> anyhow::Result<JanusClient> {
        let mut guard = self.clients.write().expect("Must not panic");
        match guard.entry(backend.id().clone()) {
            Entry::Occupied(o) => Ok(o.get().client.clone()),
            Entry::Vacant(v) => {
                let this = self.clone();
                let client = JanusClient::new(backend.janus_url())?;
                let session_id = backend.session_id();
                let agent_id = backend.id().clone();
                let is_cancelled = Arc::new(AtomicBool::new(false));
                v.insert(ClientHandle {
                    client: client.clone(),
                    is_cancelled: is_cancelled.clone(),
                    janus_url: backend.janus_url().to_owned(),
                });
                if self.group.as_deref() == backend.group() {
                    let agent = backend.id().clone();
                    async_std::task::spawn({
                        let client = client.clone();
                        async move {
                            let sink = this.events_sink.clone();
                            let _guard = PollerGuard {
                                clients: &this,
                                agent_id: &agent_id,
                            };
                            start_polling(client, session_id, sink, &is_cancelled, agent).await;
                        }
                    });
                }
                Ok(client)
            }
        }
    }

    pub fn remove_client(&self, agent_id: &AgentId) {
        let mut guard = self.clients.write().expect("Must not panic");
        if let Some(handle) = guard.remove(agent_id) {
            handle.is_cancelled.store(true, Ordering::SeqCst)
        }
    }

    pub fn stop_polling(&self) {
        let guard = self.clients.read().expect("Must not panic");
        for (_, handle) in guard.iter() {
            handle.is_cancelled.store(true, Ordering::SeqCst)
        }
    }
}

#[derive(Debug, Clone)]
struct ClientHandle {
    client: JanusClient,
    is_cancelled: Arc<AtomicBool>,
    janus_url: String,
}

#[derive(Debug, Clone)]
struct PollerGuard<'a> {
    clients: &'a Clients,
    agent_id: &'a AgentId,
}

impl<'a> Drop for PollerGuard<'a> {
    fn drop(&mut self) {
        self.clients.remove_client(&self.agent_id)
    }
}

async fn start_polling(
    janus_client: JanusClient,
    session_id: SessionId,
    sink: Sender<IncomingEvent>,
    is_cancelled: &AtomicBool,
    agent: AgentId,
) {
    let mut retries_count = 5;
    loop {
        if retries_count == 0 {
            break;
        }
        if is_cancelled.load(Ordering::SeqCst) {
            break;
        }
        let poll_result = janus_client.poll(session_id).await;
        match poll_result {
            Ok(PollResult::SessionNotFound) => {
                warn!(
                    crate::LOG,
                    "Session {} not found on agent {}", session_id, agent
                );
                break;
            }
            Ok(PollResult::Events(events)) => {
                for event in events {
                    sink.send(event).expect("Receiver must exist");
                }
            }
            Err(err) => {
                error!(crate::LOG, "Polling error for {}: {:?}", session_id, err);
                async_std::task::sleep(Duration::from_millis(500)).await;
                retries_count -= 1;
            }
        }
    }
}
