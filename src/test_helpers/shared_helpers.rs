use std::ops::Bound;

use chrono::{Duration, SubsecRound, Utc};
use diesel::pg::PgConnection;
use rand::Rng;
use svc_agent::AgentId;

use crate::{
    backend::janus::client::{
        create_handle::CreateHandleRequest,
        service_ping::{ServicePingRequest, ServicePingRequestBody},
        HandleId, JanusClient, SessionId,
    },
    db::{
        self,
        agent::{Object as Agent, Status as AgentStatus},
        agent_connection::Object as AgentConnection,
        janus_backend::Object as JanusBackend,
        recording::Object as Recording,
        room::Object as Room,
        rtc::{Object as Rtc, SharingPolicy as RtcSharingPolicy},
    },
    diesel::Identifiable,
};

use super::{agent::TestAgent, factory, SVC_AUDIENCE, USR_AUDIENCE};

///////////////////////////////////////////////////////////////////////////////

pub fn insert_room(conn: &PgConnection) -> Room {
    let now = Utc::now().trunc_subsecs(0);

    factory::Room::new()
        .audience(USR_AUDIENCE)
        .time((
            Bound::Included(now),
            Bound::Excluded(now + Duration::hours(1)),
        ))
        .rtc_sharing_policy(RtcSharingPolicy::Shared)
        .insert(conn)
}

pub fn insert_room_with_backend_id(conn: &PgConnection, backend_id: &AgentId) -> Room {
    let now = Utc::now().trunc_subsecs(0);

    factory::Room::new()
        .audience(USR_AUDIENCE)
        .time((
            Bound::Included(now),
            Bound::Excluded(now + Duration::hours(1)),
        ))
        .rtc_sharing_policy(RtcSharingPolicy::Shared)
        .backend_id(backend_id)
        .insert(conn)
}

pub fn insert_closed_room(conn: &PgConnection) -> Room {
    let now = Utc::now().trunc_subsecs(0);

    factory::Room::new()
        .audience(USR_AUDIENCE)
        .time((
            Bound::Included(now - Duration::hours(10)),
            Bound::Excluded(now - Duration::hours(8)),
        ))
        .rtc_sharing_policy(RtcSharingPolicy::Shared)
        .insert(conn)
}

pub fn insert_closed_room_with_backend_id(conn: &PgConnection, backend_id: &AgentId) -> Room {
    let now = Utc::now().trunc_subsecs(0);

    factory::Room::new()
        .audience(USR_AUDIENCE)
        .time((
            Bound::Included(now - Duration::hours(10)),
            Bound::Excluded(now - Duration::hours(8)),
        ))
        .rtc_sharing_policy(RtcSharingPolicy::Shared)
        .backend_id(backend_id)
        .insert(conn)
}

pub fn insert_room_with_owned(conn: &PgConnection) -> Room {
    let now = Utc::now().trunc_subsecs(0);

    factory::Room::new()
        .audience(USR_AUDIENCE)
        .time((Bound::Included(now), Bound::Unbounded))
        .rtc_sharing_policy(RtcSharingPolicy::Owned)
        .insert(conn)
}

pub fn insert_agent(conn: &PgConnection, agent_id: &AgentId, room_id: db::room::Id) -> Agent {
    factory::Agent::new()
        .agent_id(agent_id)
        .room_id(room_id)
        .status(AgentStatus::Ready)
        .insert(conn)
}

pub async fn insert_connected_agent(
    conn: &PgConnection,
    conn_sqlx: &mut sqlx::PgConnection,
    agent_id: &AgentId,
    room_id: db::room::Id,
    rtc_id: db::rtc::Id,
) -> (Agent, AgentConnection) {
    insert_connected_to_handle_agent(
        conn,
        conn_sqlx,
        agent_id,
        room_id,
        rtc_id,
        crate::backend::janus::client::HandleId::stub_id(),
    )
    .await
}

pub async fn create_handle(janus_url: &str, session_id: SessionId) -> HandleId {
    JanusClient::new(janus_url)
        .unwrap()
        .create_handle(CreateHandleRequest {
            session_id,
            opaque_id: None,
        })
        .await
        .unwrap()
        .id
}

pub async fn init_janus(janus_url: &str) -> (SessionId, HandleId) {
    let janus_client = JanusClient::new(janus_url).unwrap();
    let session_id = janus_client.create_session().await.unwrap().id;
    let handle_id = janus_client
        .create_handle(CreateHandleRequest {
            session_id,
            opaque_id: None,
        })
        .await
        .unwrap()
        .id;
    let _ = janus_client
        .service_ping(ServicePingRequest {
            session_id,
            handle_id,
            body: ServicePingRequestBody::new(),
        })
        .await;
    (session_id, handle_id)
}

pub async fn insert_connected_to_handle_agent(
    conn: &PgConnection,
    conn_sqlx: &mut sqlx::PgConnection,
    agent_id: &AgentId,
    room_id: db::room::Id,
    rtc_id: db::rtc::Id,
    handle_id: crate::backend::janus::client::HandleId,
) -> (Agent, AgentConnection) {
    let agent = insert_agent(conn, agent_id, room_id);
    let agent_connection = factory::AgentConnection::new(*agent.id(), rtc_id, handle_id)
        .insert(conn_sqlx)
        .await;
    (agent, agent_connection)
}

pub async fn insert_janus_backend(
    conn: &mut sqlx::PgConnection,
    url: &str,
    session_id: SessionId,
    handle_id: crate::backend::janus::client::HandleId,
) -> JanusBackend {
    let rng = rand::thread_rng();

    let label_suffix: String = rng
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();
    let label = format!("janus-gateway-{}", label_suffix);

    let agent = TestAgent::new("alpha", &label, SVC_AUDIENCE);
    factory::JanusBackend::new(
        agent.agent_id().to_owned(),
        handle_id,
        session_id,
        url.to_owned(),
    )
    .insert(conn)
    .await
}

pub async fn insert_janus_backend_with_group(
    conn: &mut sqlx::PgConnection,
    url: &str,
    session_id: SessionId,
    handle_id: crate::backend::janus::client::HandleId,
    group: &str,
) -> JanusBackend {
    let rng = rand::thread_rng();

    let label_suffix: String = rng
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();
    let label = format!("janus-gateway-{}", label_suffix);

    let agent = TestAgent::new("alpha", &label, SVC_AUDIENCE);
    factory::JanusBackend::new(
        agent.agent_id().to_owned(),
        handle_id,
        session_id,
        url.to_owned(),
    )
    .group(group)
    .insert(conn)
    .await
}

pub fn insert_rtc(conn: &PgConnection) -> Rtc {
    let room = insert_room(conn);
    factory::Rtc::new(room.id()).insert(conn)
}

pub fn insert_rtc_with_room(conn: &PgConnection, room: &Room) -> Rtc {
    factory::Rtc::new(room.id()).insert(conn)
}

pub fn insert_recording(conn: &PgConnection, rtc: &Rtc) -> Recording {
    factory::Recording::new().rtc(rtc).insert(conn)
}
