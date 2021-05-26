use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Error, Result};
use svc_agent::AgentId;

use super::Client as JanusClient;
use crate::app::context::MessagePublisher;
use crate::db::{self, ConnectionPool as Db};

const CREATE_REQUEST_BATCH_SIZE: usize = 100;
const INSERT_BULK_SIZE: usize = 100;

enum Message {
    Backend {
        id: AgentId,
        session_id: i64,
        handles_count: usize,
    },
    Handle {
        backend_id: AgentId,
        handle_id: i64,
    },
}

pub(crate) struct HandlePool {
    tx: crossbeam_channel::Sender<Message>,
}

impl HandlePool {
    pub(crate) fn start<A: 'static + MessagePublisher>(
        agent: A,
        janus_client: Arc<JanusClient>,
        db: Db,
    ) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();

        async_std::task::spawn_blocking(move || {
            HandlePoolMessageHandler::new(agent, janus_client, db).start(rx);
        });

        Self { tx }
    }

    pub(crate) fn create_handles(
        &self,
        backend_id: &AgentId,
        session_id: i64,
        handles_count: usize,
    ) -> Result<()> {
        self.tx
            .send(Message::Backend {
                id: backend_id.to_owned(),
                session_id,
                handles_count,
            })
            .context("Failed to send backend message to handle pool message handler")
    }

    pub(crate) fn handle_created_callback(
        &self,
        backend_id: &AgentId,
        handle_id: i64,
    ) -> Result<()> {
        self.tx
            .send(Message::Handle {
                backend_id: backend_id.to_owned(),
                handle_id,
            })
            .context("Failed to send handle message to handle pool message handler")
    }
}

struct HandlePoolMessageHandler<A: MessagePublisher> {
    state: HashMap<AgentId, BackendState>,
    agent: A,
    janus_client: Arc<JanusClient>,
    db: Db,
}

impl<A: MessagePublisher> HandlePoolMessageHandler<A> {
    fn new(agent: A, janus_client: Arc<JanusClient>, db: Db) -> Self {
        Self {
            state: HashMap::new(),
            agent,
            janus_client,
            db,
        }
    }

    fn start(&mut self, rx: crossbeam_channel::Receiver<Message>) {
        while let Ok(message) = rx.recv() {
            if let Err(err) = self.handle_message(message) {
                error!(crate::LOG, "Failed to handle handle pool message: {}", err);
            }
        }
    }

    fn handle_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::Backend {
                id,
                session_id,
                handles_count,
            } => self.handle_backend(id, session_id, handles_count),
            Message::Handle {
                backend_id,
                handle_id,
            } => self.handle_handle(backend_id, handle_id),
        }
    }

    fn handle_backend(&mut self, id: AgentId, session_id: i64, handles_count: usize) -> Result<()> {
        let backend_state = BackendState::new(session_id, handles_count);
        self.state.insert(id.clone(), backend_state);
        self.create_handles(id, std::cmp::min(handles_count, CREATE_REQUEST_BATCH_SIZE))?;
        Ok(())
    }

    fn handle_handle(&mut self, backend_id: AgentId, handle_id: i64) -> Result<()> {
        let mut maybe_handle_ids = None;
        let mut maybe_handles_remained = None;

        if let Some(backend_state) = self.state.get_mut(&backend_id) {
            backend_state.add_handle_id(handle_id);

            if backend_state.is_ready_to_flush() {
                maybe_handle_ids = Some(backend_state.flush());
            }

            maybe_handles_remained = Some(backend_state.handles_remained());
        }

        if let Some(ref handle_ids) = maybe_handle_ids {
            self.flush(&backend_id, handle_ids)?;
        }

        match maybe_handles_remained {
            None => (),
            Some(0) => {
                self.state.remove(&backend_id);
            }
            Some(handles_remained) => {
                let handles_count = std::cmp::min(handles_remained, CREATE_REQUEST_BATCH_SIZE);
                self.create_handles(backend_id, handles_count)?;
            }
        }

        Ok(())
    }

    fn create_handles(&self, backend_id: AgentId, handles_count: usize) -> Result<()> {
        let backend_state = self
            .state
            .get(&backend_id)
            .ok_or_else(|| anyhow!("Backend not registered in the pool"))?;

        for _ in 0..handles_count {
            let req = self
                .janus_client
                .create_pool_handle_request(&backend_id, backend_state.session_id)
                .map_err(|err| anyhow!("Failed to build pool handle creation request: {}", err))?;

            self.agent.clone().publish(Box::new(req)).map_err(|err| {
                anyhow!("Failed to publish pool handle creation request: {}", err)
            })?;
        }

        Ok(())
    }

    fn flush(&self, backend_id: &AgentId, handle_ids: &[i64]) -> Result<()> {
        let conn = self
            .db
            .get()
            .map_err(|err| Error::from(err).context("Failed to acquire DB connection"))?;

        db::janus_backend_handle::BulkInsertQuery::new(backend_id, handle_ids)
            .execute(&conn)
            .map(|_| ())
            .context("Failed to insert janus backend handles")
    }
}

struct BackendState {
    session_id: i64,
    expected_handles_count: usize,
    inserted_handles_count: usize,
    handle_ids_buffer: Vec<i64>,
}

impl BackendState {
    fn new(session_id: i64, expected_handles_count: usize) -> Self {
        Self {
            session_id,
            expected_handles_count,
            inserted_handles_count: 0,
            handle_ids_buffer: Vec::with_capacity(INSERT_BULK_SIZE),
        }
    }

    fn add_handle_id(&mut self, handle_id: i64) -> &mut Self {
        self.handle_ids_buffer.push(handle_id);
        self
    }

    fn flush(&mut self) -> Vec<i64> {
        self.inserted_handles_count += self.handle_ids_buffer.len();
        let handle_ids = self.handle_ids_buffer.clone();
        self.handle_ids_buffer.clear();
        handle_ids
    }

    fn handles_remained(&self) -> usize {
        self.expected_handles_count - self.inserted_handles_count - self.handle_ids_buffer.len()
    }

    fn is_ready_to_flush(&self) -> bool {
        self.handle_ids_buffer.len() >= INSERT_BULK_SIZE || self.handles_remained() == 0
    }
}
