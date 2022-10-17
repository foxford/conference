use anyhow::anyhow;
use chrono::Utc;
use diesel::Connection;
use std::{
    collections::{hash_map::Entry, HashMap},
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, PoisonError, RwLock,
    },
    time::Duration,
};
use svc_agent::mqtt::Agent;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};

use crate::{
    app::{
        endpoint::{rtc_signal::CreateResponseData, rtc_stream},
        error::Error,
    },
    db::{agent_connection, janus_backend, janus_rtc_stream, ConnectionPool},
    util::spawn_blocking,
};

use super::{
    client::{IncomingEvent, JanusClient, PollResult, SessionId},
    waitlist::WaitList,
};

#[derive(Clone)]
pub struct Clients {
    clients: Arc<RwLock<HashMap<janus_backend::Object, ClientHandle>>>,
    events_sink: UnboundedSender<IncomingEvent>,
    group: Option<String>,
    db: ConnectionPool,
    stream_waitlist: WaitList<Result<CreateResponseData, Error>>,
    ip_addr: IpAddr,
    mqtt_agent: Option<Agent>,
}

impl Clients {
    pub fn new(
        events_sink: UnboundedSender<IncomingEvent>,
        group: Option<String>,
        db: ConnectionPool,
        waitlist_epoch_duration: std::time::Duration,
        ip_addr: IpAddr,
        mqtt_agent: Option<Agent>,
    ) -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            events_sink,
            group,
            db,
            stream_waitlist: WaitList::new(waitlist_epoch_duration),
            ip_addr,
            mqtt_agent,
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
                let mqtt_agent = self.mqtt_agent.clone();
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
                        start_polling(
                            client,
                            session_id,
                            sink,
                            db,
                            &is_cancelled,
                            &backend,
                            mqtt_agent,
                        )
                        .await;
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

    pub fn stream_waitlist(&self) -> &WaitList<Result<CreateResponseData, Error>> {
        &self.stream_waitlist
    }

    pub fn own_ip_addr(&self) -> IpAddr {
        self.ip_addr
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
    mqtt_agent: Option<Agent>,
) {
    let mut fail_retries_count = 5;
    loop {
        if fail_retries_count == 0 {
            remove_backend(janus_backend, db, mqtt_agent).await;
            break;
        }
        if is_cancelled.load(Ordering::SeqCst) {
            break;
        }
        let poll_result = janus_client.poll(session_id).await;
        match poll_result {
            Ok(PollResult::SessionNotFound) => {
                warn!(?janus_backend, "Session not found");
                remove_backend(janus_backend, db, mqtt_agent).await;
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

async fn remove_backend(backend: &janus_backend::Object, db: ConnectionPool, agent: Option<Agent>) {
    let result = spawn_blocking({
        let backend = backend.clone();
        move || {
            let conn = db.get()?;
            let stopped_rtcs_streams = conn.transaction::<_, diesel::result::Error, _>(|| {
                janus_backend::DeleteQuery::new(
                    backend.id(),
                    backend.session_id(),
                    backend.handle_id(),
                )
                .execute(&conn)?;

                // since backend can be up again we should disconnect everyone and stop
                // all running streams regardless of whether backend was deleted or not
                agent_connection::BulkDisconnectByBackendQuery::new(backend.id()).execute(&conn)?;
                let stopped_streams =
                    janus_rtc_stream::stop_running_streams_by_backend(backend.id(), &conn)?;

                Ok(stopped_streams)
            })?;
            Ok::<_, anyhow::Error>(stopped_rtcs_streams)
        }
    })
    .await;

    match (result, agent) {
        (Ok(stopped_rtcs_streams), Some(mut agent)) => {
            let now = Utc::now();
            for stream in stopped_rtcs_streams {
                let end_time = match stream.janus_rtc_stream.time() {
                    Some((_start, end)) => match end {
                        std::ops::Bound::Included(t) | std::ops::Bound::Excluded(t) => t,
                        std::ops::Bound::Unbounded => continue,
                    },
                    None => now,
                };
                let update_evt =
                    rtc_stream::update_event(stream.room_id, stream.janus_rtc_stream, end_time);
                if let Err(err) = agent.publish(update_evt) {
                    error!(backend = ?backend, ?err, "Failed to publish rtc_stream.update evt");
                }
            }
        }
        (Ok(_streams), None) => {
            // not sending events since no agent provided
        }
        (Err(err), _) => {
            error!(backend = ?backend, ?err, "Error removing backend");
        }
    }
}
