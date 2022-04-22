use super::client::{IncomingEvent, JanusClient, PollResult, SessionId};
use crate::{
    db::{agent_connection, janus_backend, ConnectionPool},
    util::spawn_blocking,
};
use anyhow::anyhow;
use diesel::Connection;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, PoisonError, RwLock,
    },
    time::Duration,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};

#[derive(Clone)]
pub struct Clients {
    clients: Arc<RwLock<HashMap<janus_backend::Object, ClientHandle>>>,
    events_sink: UnboundedSender<IncomingEvent>,
    group: Option<String>,
    db: ConnectionPool,
}

impl Clients {
    pub fn new(
        events_sink: UnboundedSender<IncomingEvent>,
        group: Option<String>,
        db: ConnectionPool,
    ) -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            events_sink,
            group,
            db,
        }
    }

    pub fn clients_count(&self) -> usize {
        self.clients
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    pub fn get_or_insert(&self, backend: &janus_backend::Object) -> anyhow::Result<JanusClient> {
        if backend.group() != self.group.as_deref() {
            return Err(anyhow!(
                "Wrong backend_group. Expected: {:?}. Got: {:?}",
                self.group,
                backend.group()
            ));
        }
        self.get_client(backend)
            .map(Ok)
            .unwrap_or_else(|| self.put_client(backend.clone()))
    }

    fn get_client(&self, backend: &janus_backend::Object) -> Option<JanusClient> {
        let guard = self.clients.read().expect("Must not panic");
        Some(guard.get(backend)?.client.clone())
    }

    fn put_client(&self, backend: janus_backend::Object) -> anyhow::Result<JanusClient> {
        let mut guard = self.clients.write().expect("Must not panic");
        match guard.entry(backend.clone()) {
            Entry::Occupied(o) => Ok(o.get().client.clone()),
            Entry::Vacant(v) => {
                let this = self.clone();
                let client = JanusClient::new(backend.janus_url())?;
                let session_id = backend.session_id();
                let is_cancelled = Arc::new(AtomicBool::new(false));
                v.insert(ClientHandle {
                    client: client.clone(),
                    is_cancelled: is_cancelled.clone(),
                    janus_url: backend.janus_url().to_owned(),
                });
                tokio::task::spawn({
                    let client = client.clone();
                    let db = self.db.clone();
                    async move {
                        let sink = this.events_sink.clone();
                        let _guard = PollerGuard {
                            clients: &this,
                            backend: &backend,
                        };
                        start_polling(client, session_id, sink, db, &is_cancelled, &backend).await;
                    }
                });
                Ok(client)
            }
        }
    }

    pub fn remove_client(&self, backend: &janus_backend::Object) {
        let mut guard = self.clients.write().expect("Must not panic");
        if let Some(handle) = guard.remove(backend) {
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
    #[allow(dead_code)]
    janus_url: String,
}

#[derive(Clone)]
struct PollerGuard<'a> {
    clients: &'a Clients,
    backend: &'a janus_backend::Object,
}

impl<'a> Drop for PollerGuard<'a> {
    fn drop(&mut self) {
        self.clients.remove_client(self.backend)
    }
}

async fn start_polling(
    janus_client: JanusClient,
    session_id: SessionId,
    sink: UnboundedSender<IncomingEvent>,
    db: ConnectionPool,
    is_cancelled: &AtomicBool,
    janus_backend: &janus_backend::Object,
) {
    let mut fail_retries_count = 5;
    loop {
        if fail_retries_count == 0 {
            remove_backend(janus_backend, db).await;
            break;
        }
        if is_cancelled.load(Ordering::SeqCst) {
            break;
        }
        let poll_result = janus_client.poll(session_id).await;
        match poll_result {
            Ok(PollResult::SessionNotFound) => {
                warn!(?janus_backend, "Session not found");
                remove_backend(janus_backend, db).await;
                break;
            }
            Ok(PollResult::Events(events)) => {
                fail_retries_count = 5;
                if let [event] = events.as_slice() {
                    let keep_alive = event.get("janus").and_then(|x| x.as_str());
                    if Some("keepalive") == keep_alive {
                        continue;
                    }
                }
                for event in events {
                    match serde_json::from_value(event) {
                        Ok(event) => {
                            sink.send(event).expect("Receiver must exist");
                        }
                        Err(err) => {
                            warn!(?err, ?janus_backend, "Got unknown event");
                        }
                    }
                }
            }
            Err(err) => {
                error!(?err, ?janus_backend, "Polling error");
                tokio::time::sleep(Duration::from_millis(500)).await;
                fail_retries_count -= 1;
            }
        }
    }
}

async fn remove_backend(backend: &janus_backend::Object, db: ConnectionPool) {
    let result = spawn_blocking({
        let backend = backend.clone();
        move || {
            let conn = db.get()?;
            conn.transaction::<_, diesel::result::Error, _>(|| {
                let deleted = janus_backend::DeleteQuery::new(
                    backend.id(),
                    backend.session_id(),
                    backend.handle_id(),
                )
                .execute(&conn)?;
                if deleted > 0 {
                    agent_connection::BulkDisconnectByBackendQuery::new(backend.id())
                        .execute(&conn)?;
                }
                Ok(())
            })?;
            Ok::<_, anyhow::Error>(())
        }
    })
    .await;
    if let Err(err) = result {
        error!(backend = ?backend, ?err, "Error removing backend");
    }
}
