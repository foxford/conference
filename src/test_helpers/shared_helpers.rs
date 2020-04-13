use std::ops::Bound;

use chrono::{Duration, SubsecRound, Utc};
use diesel::pg::PgConnection;
use rand::Rng;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::db::agent::{Object as Agent, Status as AgentStatus};
use crate::db::janus_backend::Object as JanusBackend;
use crate::db::room::{Object as Room, RoomBackend};
use crate::db::rtc::Object as Rtc;

use super::{agent::TestAgent, factory, SVC_AUDIENCE, USR_AUDIENCE};

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn insert_room(conn: &PgConnection) -> Room {
    let now = Utc::now().trunc_subsecs(0);

    factory::Room::new()
        .audience(USR_AUDIENCE)
        .time((Bound::Included(now), Bound::Unbounded))
        .backend(RoomBackend::Janus)
        .insert(conn)
}

pub(crate) fn insert_closed_room(conn: &PgConnection) -> Room {
    let now = Utc::now().trunc_subsecs(0);

    factory::Room::new()
        .audience(USR_AUDIENCE)
        .time((
            Bound::Included(now - Duration::hours(10)),
            Bound::Excluded(now - Duration::hours(8)),
        ))
        .backend(RoomBackend::Janus)
        .insert(conn)
}

pub(crate) fn insert_agent(conn: &PgConnection, agent_id: &AgentId, room_id: Uuid) -> Agent {
    factory::Agent::new()
        .agent_id(agent_id)
        .room_id(room_id)
        .status(AgentStatus::Ready)
        .insert(conn)
}

pub(crate) fn insert_janus_backend(conn: &PgConnection) -> JanusBackend {
    let mut rng = rand::thread_rng();
    let agent = TestAgent::new("alpha", "janus-gateway", SVC_AUDIENCE);
    factory::JanusBackend::new(agent.agent_id().to_owned(), rng.gen(), rng.gen()).insert(conn)
}

pub(crate) fn insert_rtc(conn: &PgConnection) -> Rtc {
    let room = insert_room(conn);
    factory::Rtc::new(room.id()).insert(conn)
}
