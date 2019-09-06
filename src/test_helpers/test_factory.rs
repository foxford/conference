use std::ops::Bound;

use chrono::Utc;
use diesel::pg::PgConnection;
use rand::Rng;

use crate::db::{janus_backend, room, rtc};
use super::test_agent::TestAgent;

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
    rtc::InsertQuery::new(room.id()).execute(conn).expect("Failed to insert rtc")
}
