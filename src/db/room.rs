use std::{fmt, ops::Bound};

use chrono::{serde::ts_seconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::{
    backend::janus::{
        client::{HandleId, SessionId},
        JANUS_API_VERSION,
    },
    db::{
        self,
        janus_backend::Object as JanusBackend,
        recording::{Object as Recording, Status as RecordingStatus},
        rtc::SharingPolicy as RtcSharingPolicy,
    },
};

use super::recording::SegmentPg;

////////////////////////////////////////////////////////////////////////////////

pub type Time = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

#[derive(sqlx::Type, Debug, Clone)]
#[sqlx(transparent)]
pub struct TimePg(sqlx::postgres::types::PgRange<DateTime<Utc>>);

impl From<Time> for TimePg {
    fn from(value: Time) -> Self {
        Self(sqlx::postgres::types::PgRange::from(value))
    }
}

impl From<TimePg> for Time {
    fn from(value: TimePg) -> Self {
        (value.0.start, value.0.end)
    }
}

////////////////////////////////////////////////////////////////////////////////
pub type Id = db::id::Id;

// Deprecated in favor of `crate::db::rtc::SharingPolicy`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "room_backend")]
// This is not just `Backend` because of clash with `diesel::backend::Backend`.
pub enum RoomBackend {
    #[sqlx(rename = "none")]
    None,
    #[sqlx(rename = "janus")]
    Janus,
}

impl fmt::Display for RoomBackend {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let serialized = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        write!(f, "{serialized}")
    }
}

impl From<RtcSharingPolicy> for RoomBackend {
    fn from(rtc_sharing_policy: RtcSharingPolicy) -> Self {
        match rtc_sharing_policy {
            RtcSharingPolicy::Shared => Self::Janus,
            _ => Self::None,
        }
    }
}

impl From<RoomBackend> for RtcSharingPolicy {
    fn from(val: RoomBackend) -> Self {
        match val {
            RoomBackend::None => RtcSharingPolicy::None,
            RoomBackend::Janus => RtcSharingPolicy::Shared,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Object {
    pub id: Id,
    #[serde(with = "crate::serde::ts_seconds_bound_tuple_pg")]
    pub time: TimePg,
    pub audience: String,
    #[serde(with = "ts_seconds")]
    pub created_at: DateTime<Utc>,
    pub backend: RoomBackend,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reserve: Option<i32>,
    pub tags: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<AgentId>,
    pub rtc_sharing_policy: RtcSharingPolicy,
    pub classroom_id: Uuid,
    pub host: Option<AgentId>,
    pub timed_out: bool,
    pub closed_by: Option<AgentId>,
    #[serde(skip)]
    pub infinite: bool,
}

impl Object {
    pub fn audience(&self) -> &str {
        &self.audience
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn time(&self) -> Time {
        Time::from(self.time.clone())
    }

    pub fn reserve(&self) -> Option<i32> {
        self.reserve
    }

    pub fn is_closed(&self) -> bool {
        match self.time().1 {
            Bound::Included(t) => t < Utc::now(),
            Bound::Excluded(t) => t <= Utc::now(),
            Bound::Unbounded => false,
        }
    }

    #[cfg(test)]
    pub fn tags(&self) -> &JsonValue {
        &self.tags
    }

    pub fn backend_id(&self) -> Option<&AgentId> {
        self.backend_id.as_ref()
    }

    pub fn rtc_sharing_policy(&self) -> RtcSharingPolicy {
        self.rtc_sharing_policy
    }

    pub fn classroom_id(&self) -> Uuid {
        self.classroom_id
    }

    pub fn host(&self) -> Option<&AgentId> {
        self.host.as_ref()
    }

    #[cfg(test)]
    pub fn timed_out(&self) -> bool {
        self.timed_out
    }

    pub fn infinite(&self) -> bool {
        self.infinite
    }
}

////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FindQueryable {
    async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>>;
}

#[derive(Debug)]
pub struct FindQuery {
    id: Id,
}

impl FindQuery {
    pub fn new(id: Id) -> Self {
        Self { id }
    }
}

#[async_trait::async_trait]
impl FindQueryable for FindQuery {
    async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                id as "id: Id",
                backend_id as "backend_id: AgentId",
                time as "time: TimePg",
                reserve,
                tags,
                classroom_id,
                host as "host: AgentId",
                timed_out,
                audience,
                created_at,
                backend as "backend: RoomBackend",
                rtc_sharing_policy as "rtc_sharing_policy: RtcSharingPolicy",
                infinite,
                closed_by as "closed_by: AgentId"
            FROM room
            WHERE
                id = $1
            "#,
            self.id as Id,
        )
        .fetch_optional(conn)
        .await
    }
}

#[derive(Debug)]
pub struct FindByRtcIdQuery {
    rtc_id: db::rtc::Id,
}

impl FindByRtcIdQuery {
    pub fn new(rtc_id: db::rtc::Id) -> Self {
        Self { rtc_id }
    }
}

#[async_trait::async_trait]
impl FindQueryable for FindByRtcIdQuery {
    async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                r.id as "id: Id",
                r.backend_id as "backend_id: AgentId",
                r.time as "time: TimePg",
                r.reserve,
                r.tags,
                r.classroom_id,
                r.host as "host: AgentId",
                r.timed_out,
                r.audience,
                r.created_at,
                r.backend as "backend: RoomBackend",
                r.rtc_sharing_policy as "rtc_sharing_policy: RtcSharingPolicy",
                r.infinite,
                r.closed_by as "closed_by: AgentId"
            FROM room as r
            INNER JOIN rtc
            ON r.id = rtc.room_id
            WHERE
                rtc.id = $1
            "#,
            self.rtc_id as db::rtc::Id,
        )
        .fetch_optional(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

struct FinishedInProgressRecordingsRow {
    room_id: Id,
    time: TimePg,
    audience: String,
    room_created_at: DateTime<Utc>,
    backend: RoomBackend,
    reserve: Option<i32>,
    tags: JsonValue,
    backend_id: AgentId,
    rtc_sharing_policy: RtcSharingPolicy,
    classroom_id: sqlx::types::Uuid,
    host: Option<AgentId>,
    timed_out: bool,
    closed_by: Option<AgentId>,
    infinite: bool,
    rtc_id: db::rtc::Id,
    started_at: Option<DateTime<Utc>>,
    segments: Option<Vec<SegmentPg>>,
    status: RecordingStatus,
    mjr_dumps_uris: Option<Vec<String>>,
    handle_id: HandleId,
    session_id: SessionId,
    janus_backend_created_at: DateTime<Utc>,
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
    api_version: String,
    group: Option<String>,
    janus_url: String,
}

impl FinishedInProgressRecordingsRow {
    fn split(self) -> (Object, Recording, JanusBackend) {
        (
            Object {
                id: self.room_id,
                time: self.time,
                audience: self.audience,
                created_at: self.room_created_at,
                backend: self.backend,
                reserve: self.reserve,
                tags: self.tags,
                backend_id: Some(self.backend_id.clone()),
                rtc_sharing_policy: self.rtc_sharing_policy,
                classroom_id: self.classroom_id,
                host: self.host,
                timed_out: self.timed_out,
                closed_by: self.closed_by,
                infinite: self.infinite,
            },
            Recording {
                rtc_id: self.rtc_id,
                started_at: self.started_at,
                segments: self.segments,
                status: self.status,
                mjr_dumps_uris: self.mjr_dumps_uris,
            },
            JanusBackend {
                id: self.backend_id,
                handle_id: self.handle_id,
                session_id: self.session_id,
                created_at: self.janus_backend_created_at,
                capacity: self.capacity,
                balancer_capacity: self.balancer_capacity,
                api_version: self.api_version,
                group: self.group,
                janus_url: self.janus_url,
            },
        )
    }
}

// Filtering out rooms with every recording ready using left and inner joins
// and condition that recording.rtc_id is null. In diagram below room1
// and room3 will be selected (room1 - there's one recording that is not
// ready, room3 - there's no ready recordings at all).
// room1 | rtc1 | null           room1 | null | null
// room1 | rtc2 | recording1  -> room1 | rtc2 | recording1
// room2 | rtc3 | recording2     room2 | rtc3 | recording2
// room3 | rtc4 | null           room3 | null | null
pub async fn finished_with_in_progress_recordings(
    conn: &mut sqlx::PgConnection,
    maybe_group: Option<&str>,
) -> sqlx::Result<Vec<(Object, Recording, JanusBackend)>> {
    sqlx::query_as!(
        FinishedInProgressRecordingsRow,
        r#"
        SELECT
            room.id as "room_id: Id",
            room.time as "time: TimePg",
            room.audience,
            room.created_at "room_created_at: _",
            room.backend as "backend: RoomBackend",
            room.reserve,
            room.tags,
            room.backend_id as "backend_id!: AgentId",
            room.rtc_sharing_policy as "rtc_sharing_policy: RtcSharingPolicy",
            room.classroom_id,
            room.host as "host: AgentId",
            room.timed_out,
            room.closed_by as "closed_by: AgentId",
            room.infinite,
            recording.rtc_id as "rtc_id: db::rtc::Id",
            recording.started_at,
            recording.segments as "segments: Vec<SegmentPg>",
            recording.status as "status: RecordingStatus",
            recording.mjr_dumps_uris,
            janus_backend.handle_id as "handle_id: HandleId",
            janus_backend.session_id as "session_id: SessionId",
            janus_backend.created_at as "janus_backend_created_at: _",
            janus_backend.capacity,
            janus_backend.balancer_capacity,
            janus_backend.api_version,
            janus_backend.group,
            janus_backend.janus_url
        FROM room
        INNER JOIN rtc
        ON room.id = rtc.room_id
        INNER JOIN recording
        ON recording.rtc_id = rtc.id
        INNER JOIN janus_backend
        ON janus_backend.id = room.backend_id
        WHERE
            room.rtc_sharing_policy = ANY(ARRAY ['shared'::rtc_sharing_policy, 'owned']) AND
            janus_backend.api_version = $1 AND
            upper(room.time) < now() AND
            recording.status = 'in_progress' AND
            ($2::text IS NULL OR janus_backend.group = $2)
        "#,
        JANUS_API_VERSION,
        maybe_group
    )
    .fetch_all(conn)
    .await
    .map(|v| v.into_iter().map(|r| r.split()).collect())
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct InsertQuery<'a> {
    time: Time,
    audience: &'a str,
    backend: RoomBackend,
    reserve: Option<i32>,
    tags: Option<&'a JsonValue>,
    backend_id: Option<&'a AgentId>,
    rtc_sharing_policy: RtcSharingPolicy,
    classroom_id: Uuid,
    infinite: bool,
}

impl<'a> InsertQuery<'a> {
    pub fn new(
        time: Time,
        audience: &'a str,
        rtc_sharing_policy: RtcSharingPolicy,
        classroom_id: Uuid,
    ) -> Self {
        Self {
            time,
            audience,
            backend: rtc_sharing_policy.into(),
            reserve: None,
            tags: None,
            backend_id: None,
            rtc_sharing_policy,
            classroom_id,
            infinite: false,
        }
    }

    pub fn reserve(self, value: i32) -> Self {
        Self {
            reserve: Some(value),
            ..self
        }
    }

    pub fn tags(self, value: &'a JsonValue) -> Self {
        Self {
            tags: Some(value),
            ..self
        }
    }

    #[cfg(test)]
    pub fn backend_id(self, backend_id: &'a AgentId) -> Self {
        Self {
            backend_id: Some(backend_id),
            ..self
        }
    }

    #[cfg(test)]
    pub fn infinite(self, infinite: bool) -> Self {
        Self { infinite, ..self }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO room (
                time, audience, backend, reserve, tags,
                backend_id, rtc_sharing_policy, classroom_id, infinite
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING
                id as "id: Id",
                backend_id as "backend_id: AgentId",
                time as "time: TimePg",
                reserve,
                tags,
                classroom_id,
                host as "host: AgentId",
                timed_out,
                audience,
                created_at,
                backend as "backend: RoomBackend",
                rtc_sharing_policy as "rtc_sharing_policy: RtcSharingPolicy",
                infinite,
                closed_by as "closed_by: AgentId"
            "#,
            TimePg::from(self.time) as TimePg,
            self.audience,
            self.backend as RoomBackend,
            self.reserve,
            self.tags,
            self.backend_id as Option<&AgentId>,
            self.rtc_sharing_policy as RtcSharingPolicy,
            self.classroom_id,
            self.infinite,
        )
        .fetch_one(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UpdateQuery<'a> {
    id: Id,
    time: Option<Time>,
    reserve: Option<Option<i32>>,
    tags: Option<JsonValue>,
    backend_id: Option<&'a AgentId>,
    classroom_id: Option<Uuid>,
    host: Option<&'a AgentId>,
    timed_out: Option<bool>,
}

impl<'a> UpdateQuery<'a> {
    pub fn new(id: Id) -> Self {
        Self {
            id,
            backend_id: Default::default(),
            time: Default::default(),
            reserve: Default::default(),
            tags: Default::default(),
            classroom_id: Default::default(),
            host: Default::default(),
            timed_out: Default::default(),
        }
    }

    pub fn time(self, time: Option<Time>) -> Self {
        Self { time, ..self }
    }

    pub fn timed_out(self) -> Self {
        Self {
            timed_out: Some(true),
            ..self
        }
    }

    pub fn reserve(self, reserve: Option<Option<i32>>) -> Self {
        Self { reserve, ..self }
    }

    pub fn tags(self, tags: Option<JsonValue>) -> Self {
        Self { tags, ..self }
    }

    pub fn backend_id(self, backend_id: Option<&'a AgentId>) -> Self {
        Self { backend_id, ..self }
    }

    pub fn classroom_id(self, classroom_id: Option<Uuid>) -> Self {
        Self {
            classroom_id,
            ..self
        }
    }

    pub fn host(self, host: Option<&'a AgentId>) -> Self {
        Self { host, ..self }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            UPDATE room
            SET
                backend_id   = COALESCE($2, backend_id),
                time         = COALESCE($3, time),
                reserve      = COALESCE($4, reserve),
                tags         = COALESCE($5, tags),
                classroom_id = COALESCE($6, classroom_id),
                host         = COALESCE($7, host),
                timed_out    = COALESCE($8, timed_out)
            WHERE
                id = $1
            RETURNING
                id as "id: Id",
                backend_id as "backend_id: AgentId",
                time as "time: TimePg",
                reserve,
                tags,
                classroom_id,
                host as "host: AgentId",
                timed_out,
                audience,
                created_at,
                backend as "backend: RoomBackend",
                rtc_sharing_policy as "rtc_sharing_policy: RtcSharingPolicy",
                infinite,
                closed_by as "closed_by: AgentId"
            "#,
            self.id as db::room::Id,
            self.backend_id as Option<&AgentId>,
            self.time.map(TimePg::from) as Option<TimePg>,
            self.reserve.flatten(),
            self.tags,
            self.classroom_id,
            self.host as Option<&AgentId>,
            self.timed_out
        )
        .fetch_one(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

pub async fn set_closed_by(
    room_id: Id,
    agent: &AgentId,
    conn: &mut sqlx::PgConnection,
) -> sqlx::Result<Object> {
    sqlx::query_as!(
        Object,
        r#"
        UPDATE room
        SET
            closed_by = $2,
            time = TSTZRANGE(LOWER(time), NOW())
        WHERE
            id = $1
        RETURNING
            id as "id: Id",
            backend_id as "backend_id: AgentId",
            time as "time: TimePg",
            reserve,
            tags,
            classroom_id,
            host as "host: AgentId",
            timed_out,
            audience,
            created_at,
            backend as "backend: RoomBackend",
            rtc_sharing_policy as "rtc_sharing_policy: RtcSharingPolicy",
            infinite,
            closed_by as "closed_by: AgentId"
        "#,
        room_id as Id,
        agent as &AgentId,
    )
    .fetch_one(conn)
    .await
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod finished_with_in_progress_recordings {
        use super::super::*;
        use crate::{
            backend::janus::client::{HandleId, SessionId},
            test_helpers::{db, prelude::*},
        };

        #[tokio::test]
        async fn selects_appropriate_backend() {
            let db = db::TestDb::new().await;
            let mut conn = db.get_conn().await;

            let backend1 = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                HandleId::random(),
            )
            .await;
            let backend2 = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                HandleId::random(),
            )
            .await;

            let room1 =
                shared_helpers::insert_closed_room_with_backend_id(&mut conn, backend1.id()).await;
            let room2 =
                shared_helpers::insert_closed_room_with_backend_id(&mut conn, backend2.id()).await;

            // this room will have rtc but no rtc_stream simulating the case when janus_backend was removed
            // (for example crashed) and via cascade removed all streams hosted on it
            //
            // it should not appear in query result and it should not result in query Err
            let backend3_id = TestAgent::new("alpha", "janus3", SVC_AUDIENCE);

            let room3 = shared_helpers::insert_closed_room_with_backend_id(
                &mut conn,
                backend3_id.agent_id(),
            )
            .await;

            let rtc1 = shared_helpers::insert_rtc_with_room(&mut conn, &room1).await;
            let rtc2 = shared_helpers::insert_rtc_with_room(&mut conn, &room2).await;
            let _rtc3 = shared_helpers::insert_rtc_with_room(&mut conn, &room3).await;

            shared_helpers::insert_recording(&mut conn, &rtc1).await;
            shared_helpers::insert_recording(&mut conn, &rtc2).await;

            let rooms = finished_with_in_progress_recordings(&mut conn, None)
                .await
                .expect("finished_with_in_progress_recordings call failed");

            assert_eq!(rooms.len(), 2);

            // order of rooms is not specified so we check that its [(room1, _, backend1), (room2, _, backend2)] in any order
            if rooms[0].0.id() == room1.id() {
                assert_eq!(rooms[0].0.id(), room1.id());
                assert_eq!(rooms[0].2.id(), backend1.id());
                assert_eq!(rooms[1].0.id(), room2.id());
                assert_eq!(rooms[1].2.id(), backend2.id());
            } else {
                assert_eq!(rooms[1].0.id(), room1.id());
                assert_eq!(rooms[1].2.id(), backend1.id());
                assert_eq!(rooms[0].0.id(), room2.id());
                assert_eq!(rooms[0].2.id(), backend2.id());
            }
        }

        #[tokio::test]
        async fn selects_appropriate_backend_by_group() {
            let db = db::TestDb::new().await;
            let mut conn = db.get_conn().await;

            let backend1 = shared_helpers::insert_janus_backend_with_group(
                &mut conn,
                "test",
                SessionId::random(),
                HandleId::random(),
                "webinar",
            )
            .await;
            let backend2 = shared_helpers::insert_janus_backend_with_group(
                &mut conn,
                "test",
                SessionId::random(),
                HandleId::random(),
                "minigroup",
            )
            .await;

            let room1 =
                shared_helpers::insert_closed_room_with_backend_id(&mut conn, backend1.id()).await;
            let room2 =
                shared_helpers::insert_closed_room_with_backend_id(&mut conn, backend2.id()).await;

            let rtc1 = shared_helpers::insert_rtc_with_room(&mut conn, &room1).await;
            let rtc2 = shared_helpers::insert_rtc_with_room(&mut conn, &room2).await;

            shared_helpers::insert_recording(&mut conn, &rtc1).await;
            shared_helpers::insert_recording(&mut conn, &rtc2).await;

            let rooms = finished_with_in_progress_recordings(&mut conn, Some("minigroup"))
                .await
                .expect("finished_with_in_progress_recordings call failed");

            assert_eq!(rooms.len(), 1);
            assert_eq!(rooms[0].0.id(), room2.id());
        }
    }
}
