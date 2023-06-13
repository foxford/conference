use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use rand::Rng;
use svc_agent::{AccountId, AgentId};

use crate::{
    backend::janus::client::{HandleId, SessionId},
    db::{self, agent, group_agent::Groups, room::Id},
};

use super::{
    agent::TestAgent,
    shared_helpers::{insert_janus_backend, insert_room, insert_rtc},
};

///////////////////////////////////////////////////////////////////////////////

pub struct Room<'a> {
    audience: Option<String>,
    time: Option<db::room::Time>,
    rtc_sharing_policy: db::rtc::SharingPolicy,
    backend_id: Option<&'a AgentId>,
    reserve: Option<i32>,
    infinite: bool,
}

impl<'a> Room<'a> {
    pub fn new() -> Self {
        Self {
            audience: None,
            time: None,
            rtc_sharing_policy: db::rtc::SharingPolicy::None,
            backend_id: None,
            reserve: None,
            infinite: false,
        }
    }

    pub fn audience(self, audience: &str) -> Self {
        Self {
            audience: Some(audience.to_owned()),
            ..self
        }
    }

    pub fn time(self, time: db::room::Time) -> Self {
        Self {
            time: Some(time),
            ..self
        }
    }

    pub fn reserve(self, reserve: i32) -> Self {
        Self {
            reserve: Some(reserve),
            ..self
        }
    }

    pub fn rtc_sharing_policy(self, rtc_sharing_policy: db::rtc::SharingPolicy) -> Self {
        Self {
            rtc_sharing_policy,
            ..self
        }
    }

    pub fn backend_id(self, backend_id: &'a AgentId) -> Self {
        Self {
            backend_id: Some(backend_id),
            ..self
        }
    }

    pub fn infinite(self) -> Self {
        Self {
            infinite: true,
            ..self
        }
    }

    pub fn insert(self, conn: &PgConnection) -> db::room::Object {
        let audience = self.audience.expect("Audience not set");
        let time = self.time.expect("Time not set");

        let mut q = db::room::InsertQuery::new(
            time,
            &audience,
            self.rtc_sharing_policy,
            uuid::Uuid::new_v4(),
        );

        if let Some(backend_id) = self.backend_id {
            q = q.backend_id(backend_id);
        }

        if let Some(reserve) = self.reserve {
            q = q.reserve(reserve);
        }

        if self.infinite {
            q = q.infinite(true);
        }

        q.execute(conn).expect("Failed to insert room")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct Agent<'a> {
    audience: Option<&'a str>,
    agent_id: Option<&'a AgentId>,
    room_id: Option<db::room::Id>,
    status: db::agent::Status,
    created_at: Option<DateTime<Utc>>,
}

impl<'a> Agent<'a> {
    pub fn new() -> Self {
        Self {
            audience: None,
            agent_id: None,
            room_id: None,
            status: db::agent::Status::Ready,
            created_at: None,
        }
    }

    pub fn agent_id(self, agent_id: &'a AgentId) -> Self {
        Self {
            agent_id: Some(agent_id),
            ..self
        }
    }

    pub fn room_id(self, room_id: db::room::Id) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
        }
    }

    pub fn status(self, status: db::agent::Status) -> Self {
        Self { status, ..self }
    }

    pub fn created_at(self, created_at: DateTime<Utc>) -> Self {
        Self {
            created_at: Some(created_at),
            ..self
        }
    }

    pub async fn insert(
        &self,
        conn: &PgConnection,
        conn_sqlx: &mut sqlx::PgConnection,
    ) -> db::agent::Object {
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

        let mut q = db::agent::InsertQuery::new(&agent_id, room_id).status(self.status);
        if let Some(created_at) = self.created_at {
            q = q.created_at(created_at);
        }
        q.execute(conn_sqlx).await.expect("Failed to insert agent")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct AgentConnection {
    agent_id: agent::Id,
    rtc_id: db::rtc::Id,
    handle_id: HandleId,
    created_at: Option<DateTime<Utc>>,
}

impl AgentConnection {
    pub fn new(agent_id: agent::Id, rtc_id: db::rtc::Id, handle_id: HandleId) -> Self {
        Self {
            agent_id,
            rtc_id,
            handle_id,
            created_at: None,
        }
    }

    pub fn created_at(self, at: DateTime<Utc>) -> Self {
        Self {
            created_at: Some(at),
            ..self
        }
    }

    pub async fn insert(&self, conn: &mut sqlx::PgConnection) -> db::agent_connection::Object {
        let mut q =
            db::agent_connection::UpsertQuery::new(self.agent_id, self.rtc_id, self.handle_id);

        if let Some(created_at) = self.created_at {
            q = q.created_at(created_at);
        }

        q.execute(conn)
            .await
            .expect("Failed to insert agent_connection")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct Rtc {
    room_id: db::room::Id,
    created_by: AgentId,
}

impl Rtc {
    pub fn new(room_id: db::room::Id) -> Self {
        Self {
            room_id,
            created_by: AgentId::new("web", AccountId::new("nevermind", "example.com")),
        }
    }

    pub fn created_by(self, created_by: AgentId) -> Self {
        Self { created_by, ..self }
    }

    pub fn insert(&self, conn: &PgConnection) -> db::rtc::Object {
        db::rtc::InsertQuery::new(self.room_id, &self.created_by)
            .execute(conn)
            .expect("Failed to insert janus_backend")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct JanusBackend {
    id: AgentId,
    handle_id: HandleId,
    session_id: SessionId,
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
    group: Option<String>,
    janus_url: String,
}

impl JanusBackend {
    pub fn new(id: AgentId, handle_id: HandleId, session_id: SessionId, janus_url: String) -> Self {
        Self {
            id,
            handle_id,
            session_id,
            capacity: None,
            balancer_capacity: None,
            group: None,
            janus_url,
        }
    }

    pub fn capacity(self, capacity: i32) -> Self {
        Self {
            capacity: Some(capacity),
            ..self
        }
    }

    pub fn balancer_capacity(self, balancer_capacity: i32) -> Self {
        Self {
            balancer_capacity: Some(balancer_capacity),
            ..self
        }
    }

    pub fn group(self, group: &str) -> Self {
        Self {
            group: Some(group.to_owned()),
            ..self
        }
    }

    pub async fn insert(&self, conn: &mut sqlx::PgConnection) -> db::janus_backend::Object {
        let mut q = db::janus_backend::UpsertQuery::new(
            &self.id,
            self.handle_id,
            self.session_id,
            &self.janus_url,
        );

        if let Some(capacity) = self.capacity {
            q = q.capacity(capacity);
        }

        if let Some(balancer_capacity) = self.balancer_capacity {
            q = q.balancer_capacity(balancer_capacity);
        }

        if let Some(ref group) = self.group {
            q = q.group(group);
        }

        q.execute(conn)
            .await
            .expect("Failed to insert janus_backend")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct JanusRtcStream<'a> {
    audience: &'a str,
    backend: Option<&'a db::janus_backend::Object>,
    rtc: Option<&'a db::rtc::Object>,
    sent_by: Option<&'a AgentId>,
}

impl<'a> JanusRtcStream<'a> {
    pub fn new(audience: &'a str) -> Self {
        Self {
            audience,
            backend: None,
            rtc: None,
            sent_by: None,
        }
    }

    pub async fn insert(
        &self,
        conn: &PgConnection,
        conn_sqlx: &mut sqlx::PgConnection,
    ) -> db::janus_rtc_stream::Object {
        let default_backend;

        let backend = match self.backend {
            Some(value) => value,
            None => {
                default_backend = insert_janus_backend(
                    conn_sqlx,
                    "test",
                    SessionId::random(),
                    HandleId::random(),
                )
                .await;
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
            db::janus_rtc_stream::Id::random(),
            backend.handle_id(),
            rtc.id(),
            backend.id(),
            "alpha",
            sent_by,
        )
        .execute(conn_sqlx)
        .await
        .expect("Failed to insert janus_rtc_stream")
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct Recording<'a> {
    rtc: Option<&'a db::rtc::Object>,
}

impl<'a> Recording<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn rtc(self, rtc: &'a db::rtc::Object) -> Self {
        Self { rtc: Some(rtc) }
    }

    pub fn insert(&self, conn: &PgConnection) -> db::recording::Object {
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

///////////////////////////////////////////////////////////////////////////////

pub struct RtcReaderConfig<'a> {
    rtc: &'a db::rtc::Object,
    reader_id: &'a AgentId,
    receive_video: Option<bool>,
    receive_audio: Option<bool>,
}

impl<'a> RtcReaderConfig<'a> {
    pub fn new(rtc: &'a db::rtc::Object, reader_id: &'a AgentId) -> Self {
        Self {
            rtc,
            reader_id,
            receive_video: None,
            receive_audio: None,
        }
    }

    pub fn receive_video(self, receive_video: bool) -> Self {
        Self {
            receive_video: Some(receive_video),
            ..self
        }
    }

    pub fn receive_audio(self, receive_audio: bool) -> Self {
        Self {
            receive_audio: Some(receive_audio),
            ..self
        }
    }

    pub fn insert(&self, conn: &PgConnection) -> db::rtc_reader_config::Object {
        let mut q = db::rtc_reader_config::UpsertQuery::new(self.rtc.id(), self.reader_id);

        if let Some(receive_video) = self.receive_video {
            q = q.receive_video(receive_video);
        }

        if let Some(receive_audio) = self.receive_audio {
            q = q.receive_audio(receive_audio);
        }

        q.execute(conn).expect("Failed to insert RTC reader config")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct RtcWriterConfig<'a> {
    rtc: &'a db::rtc::Object,
    send_video: Option<bool>,
    send_audio: Option<bool>,
    video_remb: Option<i64>,
    send_audio_updated_by: Option<&'a AgentId>,
}

impl<'a> RtcWriterConfig<'a> {
    pub fn new(rtc: &'a db::rtc::Object) -> Self {
        Self {
            rtc,
            send_video: None,
            send_audio: None,
            video_remb: None,
            send_audio_updated_by: None,
        }
    }

    pub fn send_video(self, send_video: bool) -> Self {
        Self {
            send_video: Some(send_video),
            ..self
        }
    }

    pub fn send_audio(self, send_audio: bool) -> Self {
        Self {
            send_audio: Some(send_audio),
            ..self
        }
    }

    pub fn video_remb(self, video_remb: i64) -> Self {
        Self {
            video_remb: Some(video_remb),
            ..self
        }
    }

    pub fn send_audio_updated_by(self, send_audio_updated_by: &'a AgentId) -> Self {
        Self {
            send_audio_updated_by: Some(send_audio_updated_by),
            ..self
        }
    }

    pub fn insert(&self, conn: &PgConnection) -> db::rtc_writer_config::Object {
        let mut q = db::rtc_writer_config::UpsertQuery::new(self.rtc.id());

        if let Some(send_video) = self.send_video {
            q = q.send_video(send_video);
        }

        if let Some(send_audio) = self.send_audio {
            q = q.send_audio(send_audio);
        }

        if let Some(video_remb) = self.video_remb {
            q = q.video_remb(video_remb);
        }

        if let Some(send_audio_updated_by) = self.send_audio_updated_by {
            q = q.send_audio_updated_by(send_audio_updated_by);
        }

        q.execute(conn).expect("Failed to insert RTC writer config")
    }
}

pub struct RtcWriterConfigSnaphost<'a> {
    rtc: &'a db::rtc::Object,
    send_video: Option<bool>,
    send_audio: Option<bool>,
}

impl<'a> RtcWriterConfigSnaphost<'a> {
    pub fn new(
        rtc: &'a db::rtc::Object,
        send_video: Option<bool>,
        send_audio: Option<bool>,
    ) -> Self {
        Self {
            rtc,
            send_video,
            send_audio,
        }
    }

    pub fn insert(&self, conn: &PgConnection) -> db::rtc_writer_config_snapshot::Object {
        let q = db::rtc_writer_config_snapshot::InsertQuery::new(
            self.rtc.id(),
            self.send_video,
            self.send_audio,
        );

        q.execute(conn)
            .expect("Failed to insert RTC writer config snapshot")
    }
}

pub struct GroupAgent {
    room_id: Id,
    groups: Groups,
}

impl GroupAgent {
    pub fn new(room_id: Id, groups: Groups) -> Self {
        Self { room_id, groups }
    }

    pub fn upsert(self, conn: &PgConnection) -> db::group_agent::Object {
        db::group_agent::UpsertQuery::new(self.room_id, &self.groups)
            .execute(conn)
            .expect("failed to upsert group agent")
    }
}
