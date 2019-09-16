use std::ops::Bound;

use chrono::Utc;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use failure::{err_msg, format_err, Error};
use rand::Rng;
use svc_agent::AgentId;
use uuid::Uuid;

use super::agent::TestAgent;
use crate::db::{agent, janus_backend, janus_rtc_stream, room, rtc};

pub struct Agent<'a> {
    audience: Option<&'a str>,
    agent_id: Option<&'a AgentId>,
    room_id: Option<Uuid>,
}

impl<'a> Agent<'a> {
    pub(crate) fn new() -> Self {
        Self {
            audience: None,
            agent_id: None,
            room_id: None,
        }
    }

    pub(crate) fn audience(self, audience: &'a str) -> Self {
        Self {
            audience: Some(audience),
            ..self
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

    pub(crate) fn insert(&self, conn: &PgConnection) -> Result<agent::Object, Error> {
        let agent_id = match (self.agent_id, self.audience) {
            (Some(agent_id), _) => Ok(agent_id.to_owned()),
            (None, Some(audience)) => {
                let mut rng = rand::thread_rng();
                let label = format!("user{}", rng.gen::<u16>());
                let test_agent = TestAgent::new("web", &label, audience);
                Ok(test_agent.agent_id().to_owned())
            }
            _ => Err(err_msg("Expected agent_id either audience")),
        }?;

        let room_id = match (self.room_id, self.audience) {
            (Some(room_id), _) => Ok(room_id),
            (None, Some(audience)) => Ok(insert_room(conn, audience).id()),
            _ => Err(err_msg("Expected room_id either audience")),
        }?;

        agent::InsertQuery::new(&agent_id, room_id)
            .execute(conn)
            .map_err(|err| format_err!("Failed to insert agent: {}", err))
    }
}

pub(crate) fn insert_janus_backend(conn: &PgConnection, audience: &str) -> janus_backend::Object {
    let mut rng = rand::thread_rng();
    let agent = TestAgent::new("alpha", "janus-gateway", audience);

    janus_backend::UpdateQuery::new(agent.agent_id(), rng.gen(), rng.gen())
        .execute(conn)
        .expect("Failed to insert janus backend")
}

pub(crate) fn insert_room(conn: &PgConnection, audience: &str) -> room::Object {
    let time = (Bound::Included(Utc::now()), Bound::Unbounded);

    room::InsertQuery::new(time, audience, room::RoomBackend::Janus)
        .execute(conn)
        .expect("Failed to insert room")
}

pub(crate) fn insert_rtc(conn: &PgConnection, audience: &str) -> rtc::Object {
    let room = insert_room(conn, audience);
    rtc::InsertQuery::new(room.id())
        .execute(conn)
        .expect("Failed to insert rtc")
}

pub(crate) fn insert_janus_rtc_stream(
    conn: &PgConnection,
    audience: &str,
) -> janus_rtc_stream::Object {
    let backend = insert_janus_backend(conn, audience);
    let rtc = insert_rtc(conn, audience);
    let agent = TestAgent::new("web", "user123", audience);

    let rtc_stream = janus_rtc_stream::InsertQuery::new(
        Uuid::new_v4(),
        backend.handle_id(),
        rtc.id(),
        backend.id(),
        "alpha",
        agent.agent_id(),
    )
    .execute(&conn)
    .expect("Failed to insert janus rtc stream");

    janus_rtc_stream::start(*rtc_stream.id(), conn)
        .expect("Failed to start janus rtc stream")
        .unwrap()
}
