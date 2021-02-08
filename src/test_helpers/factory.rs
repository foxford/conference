use diesel::pg::PgConnection;
use rand::Rng;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::db;

use super::agent::TestAgent;
use super::shared_helpers::{insert_janus_backend, insert_room, insert_rtc};

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct Room<'a> {
    audience: Option<String>,
    time: Option<db::room::Time>,
    backend: db::room::RoomBackend,
    backend_id: Option<&'a AgentId>,
    reserve: Option<i32>,
}

impl<'a> Room<'a> {
    pub(crate) fn new() -> Self {
        Self {
            audience: None,
            time: None,
            backend: db::room::RoomBackend::None,
            backend_id: None,
            reserve: None,
        }
    }

    pub(crate) fn audience(self, audience: &str) -> Self {
        Self {
            audience: Some(audience.to_owned()),
            ..self
        }
    }

    pub(crate) fn time(self, time: db::room::Time) -> Self {
        Self {
            time: Some(time),
            ..self
        }
    }

    pub(crate) fn reserve(self, reserve: i32) -> Self {
        Self {
            reserve: Some(reserve),
            ..self
        }
    }

    pub(crate) fn backend(self, backend: db::room::RoomBackend) -> Self {
        Self { backend, ..self }
    }

    pub(crate) fn backend_id(self, backend_id: &'a AgentId) -> Self {
        Self {
            backend_id: Some(backend_id),
            ..self
        }
    }

    pub(crate) fn insert(self, conn: &PgConnection) -> db::room::Object {
        let audience = self.audience.expect("Audience not set");
        let time = self.time.expect("Time not set");

        let mut q = db::room::InsertQuery::new(time, &audience, self.backend);

        if let Some(backend_id) = self.backend_id {
            q = q.backend_id(backend_id);
        }

        if let Some(reserve) = self.reserve {
            q = q.reserve(reserve);
        }

        q.execute(conn).expect("Failed to insert room")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct Agent<'a> {
    audience: Option<&'a str>,
    agent_id: Option<&'a AgentId>,
    room_id: Option<Uuid>,
    status: db::agent::Status,
}

impl<'a> Agent<'a> {
    pub(crate) fn new() -> Self {
        Self {
            audience: None,
            agent_id: None,
            room_id: None,
            status: db::agent::Status::Ready,
        }
    }

    pub(crate) fn agent_id(self, agent_id: &'a AgentId) -> Self {
        Self {
            agent_id: Some(agent_id),
            ..self
        }
    }

    pub(crate) fn room_id(self, room_id: Uuid) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
        }
    }

    pub(crate) fn status(self, status: db::agent::Status) -> Self {
        Self { status, ..self }
    }

    pub(crate) fn insert(&self, conn: &PgConnection) -> db::agent::Object {
        let agent_id = match (self.agent_id, self.audience) {
            (Some(agent_id), _) => agent_id.to_owned(),
            (None, Some(audience)) => {
                let mut rng = rand::thread_rng();
                let label = format!("user{}", rng.gen::<u16>());
                let test_agent = TestAgent::new("web", &label, audience);
                test_agent.agent_id().to_owned()
            }
            _ => panic!("Expected agent_id either audience"),
        };

        let room_id = self.room_id.unwrap_or_else(|| insert_room(conn).id());

        db::agent::InsertQuery::new(&agent_id, room_id)
            .status(self.status)
            .execute(conn)
            .expect("Failed to insert agent")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct AgentConnection {
    agent_id: Uuid,
    handle_id: i64,
}

impl AgentConnection {
    pub(crate) fn new(agent_id: Uuid, handle_id: i64) -> Self {
        Self {
            agent_id,
            handle_id,
        }
    }

    pub(crate) fn insert(&self, conn: &PgConnection) -> db::agent_connection::Object {
        db::agent_connection::UpsertQuery::new(self.agent_id, self.handle_id)
            .execute(conn)
            .expect("Failed to insert agent_connection")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct Rtc {
    room_id: Uuid,
}

impl Rtc {
    pub(crate) fn new(room_id: Uuid) -> Self {
        Self { room_id }
    }

    pub(crate) fn insert(&self, conn: &PgConnection) -> db::rtc::Object {
        db::rtc::InsertQuery::new(self.room_id)
            .execute(conn)
            .expect("Failed to insert janus_backend")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct JanusBackend {
    id: AgentId,
    handle_id: i64,
    session_id: i64,
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
}

impl JanusBackend {
    pub(crate) fn new(id: AgentId, handle_id: i64, session_id: i64) -> Self {
        Self {
            id,
            handle_id,
            session_id,
            capacity: None,
            balancer_capacity: None,
        }
    }

    pub(crate) fn capacity(self, capacity: i32) -> Self {
        Self {
            capacity: Some(capacity),
            ..self
        }
    }

    pub(crate) fn balancer_capacity(self, balancer_capacity: i32) -> Self {
        Self {
            balancer_capacity: Some(balancer_capacity),
            ..self
        }
    }

    pub(crate) fn insert(&self, conn: &PgConnection) -> db::janus_backend::Object {
        let mut q = db::janus_backend::UpsertQuery::new(&self.id, self.handle_id, self.session_id);

        if let Some(capacity) = self.capacity {
            q = q.capacity(capacity);
        }

        if let Some(balancer_capacity) = self.balancer_capacity {
            q = q.balancer_capacity(balancer_capacity);
        }

        q.execute(conn).expect("Failed to insert janus_backend")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct JanusRtcStream<'a> {
    audience: &'a str,
    backend: Option<&'a db::janus_backend::Object>,
    rtc: Option<&'a db::rtc::Object>,
    sent_by: Option<&'a AgentId>,
}

impl<'a> JanusRtcStream<'a> {
    pub(crate) fn new(audience: &'a str) -> Self {
        Self {
            audience,
            backend: None,
            rtc: None,
            sent_by: None,
        }
    }

    pub(crate) fn backend(self, backend: &'a db::janus_backend::Object) -> Self {
        Self {
            backend: Some(backend),
            ..self
        }
    }

    pub(crate) fn rtc(self, rtc: &'a db::rtc::Object) -> Self {
        Self {
            rtc: Some(rtc),
            ..self
        }
    }

    pub(crate) fn sent_by(self, sent_by: &'a AgentId) -> Self {
        Self {
            sent_by: Some(sent_by),
            ..self
        }
    }

    pub(crate) fn insert(&self, conn: &PgConnection) -> db::janus_rtc_stream::Object {
        let default_backend;

        let backend = match self.backend {
            Some(value) => value,
            None => {
                default_backend = insert_janus_backend(conn);
                &default_backend
            }
        };

        let default_rtc;

        let rtc = match self.rtc {
            Some(value) => value,
            None => {
                default_rtc = insert_rtc(conn);
                &default_rtc
            }
        };

        let default_agent;

        let sent_by = match self.sent_by {
            Some(value) => value,
            None => {
                default_agent = TestAgent::new("web", "user123", self.audience);
                default_agent.agent_id()
            }
        };

        db::janus_rtc_stream::InsertQuery::new(
            backend.handle_id(),
            rtc.id(),
            backend.id(),
            "alpha",
            sent_by,
        )
        .execute(conn)
        .expect("Failed to insert janus_rtc_stream")
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct Recording<'a> {
    rtc: Option<&'a db::rtc::Object>,
}

impl<'a> Recording<'a> {
    pub(crate) fn new() -> Self {
        Default::default()
    }

    pub(crate) fn rtc(self, rtc: &'a db::rtc::Object) -> Self {
        Self {
            rtc: Some(rtc),
            ..self
        }
    }

    pub(crate) fn insert(&self, conn: &PgConnection) -> db::recording::Object {
        let default_rtc;

        let rtc = match self.rtc {
            Some(value) => value,
            None => {
                default_rtc = insert_rtc(conn);
                &default_rtc
            }
        };

        db::recording::InsertQuery::new(rtc.id())
            .execute(conn)
            .expect("Failed to insert recording")
    }
}
