use std::ops::Bound;

use chrono::Utc;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use rand::Rng;
use uuid::Uuid;

use super::agent::TestAgent;
use crate::db::{janus_backend, janus_rtc_stream, room, rtc};

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
