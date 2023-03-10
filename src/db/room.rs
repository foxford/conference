// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use std::{fmt, ops::Bound};

use chrono::{serde::ts_seconds, DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use diesel_derive_enum::DbEnum;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::{
    backend::janus::JANUS_API_VERSION,
    db::{
        self,
        janus_backend::Object as JanusBackend,
        recording::{Object as Recording, Status as RecordingStatus},
        rtc::SharingPolicy as RtcSharingPolicy,
    },
    schema::{janus_backend, recording, room, rtc},
};

////////////////////////////////////////////////////////////////////////////////

pub type Time = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

type AllColumns = (
    room::id,
    room::time,
    room::audience,
    room::created_at,
    room::backend,
    room::reserve,
    room::tags,
    room::backend_id,
    room::rtc_sharing_policy,
    room::classroom_id,
    room::host,
    room::timed_out,
    room::closed_by,
    room::infinite,
);

const ALL_COLUMNS: AllColumns = (
    room::id,
    room::time,
    room::audience,
    room::created_at,
    room::backend,
    room::reserve,
    room::tags,
    room::backend_id,
    room::rtc_sharing_policy,
    room::classroom_id,
    room::host,
    room::timed_out,
    room::closed_by,
    room::infinite,
);

////////////////////////////////////////////////////////////////////////////////
pub type Id = db::id::Id;

// Deprecated in favor of `crate::db::rtc::SharingPolicy`.
#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[DieselType = "Room_backend"]
// This is not just `Backend` because of clash with `diesel::backend::Backend`.
pub enum RoomBackend {
    None,
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

#[derive(
    Clone, Debug, Deserialize, Serialize, Identifiable, Queryable, QueryableByName, Associations,
)]
#[belongs_to(JanusBackend, foreign_key = "backend_id")]
#[table_name = "room"]
pub struct Object {
    id: Id,
    #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
    time: Time,
    audience: String,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
    backend: RoomBackend,
    #[serde(skip_serializing_if = "Option::is_none")]
    reserve: Option<i32>,
    tags: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend_id: Option<AgentId>,
    rtc_sharing_policy: RtcSharingPolicy,
    classroom_id: Uuid,
    host: Option<AgentId>,
    timed_out: bool,
    closed_by: Option<AgentId>,
    #[serde(skip)]
    infinite: bool,
}

impl Object {
    pub fn audience(&self) -> &str {
        &self.audience
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn time(&self) -> &Time {
        &self.time
    }

    pub fn reserve(&self) -> Option<i32> {
        self.reserve
    }

    pub fn is_closed(&self) -> bool {
        match self.time.1 {
            Bound::Included(t) => t < Utc::now(),
            Bound::Excluded(t) => t <= Utc::now(),
            Bound::Unbounded => false,
        }
    }

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

    pub fn timed_out(&self) -> bool {
        self.timed_out
    }

    pub fn infinite(&self) -> bool {
        self.infinite
    }
}

////////////////////////////////////////////////////////////////////////////////

pub trait FindQueryable {
    fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error>;
}

#[derive(Debug, Default)]
pub struct FindQuery {
    id: Option<Id>,
}

impl FindQuery {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn by_id(self, id: Id) -> Self {
        Self { id: Some(id) }
    }
}

impl FindQueryable for FindQuery {
    fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        let mut query = room::table.into_boxed();

        if let Some(id) = self.id {
            query = query.filter(room::id.eq(id))
        }

        query.get_result(conn).optional()
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

impl FindQueryable for FindByRtcIdQuery {
    fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        room::table
            .inner_join(rtc::table)
            .filter(rtc::id.eq(self.rtc_id))
            .select(ALL_COLUMNS)
            .get_result(conn)
            .optional()
    }
}

////////////////////////////////////////////////////////////////////////////////

// Filtering out rooms with every recording ready using left and inner joins
// and condition that recording.rtc_id is null. In diagram below room1
// and room3 will be selected (room1 - there's one recording that is not
// ready, room3 - there's no ready recordings at all).
// room1 | rtc1 | null           room1 | null | null
// room1 | rtc2 | recording1  -> room1 | rtc2 | recording1
// room2 | rtc3 | recording2     room2 | rtc3 | recording2
// room3 | rtc4 | null           room3 | null | null
pub fn finished_with_in_progress_recordings(
    conn: &PgConnection,
    maybe_group: Option<&str>,
) -> Result<Vec<(Object, Recording, JanusBackend)>, Error> {
    use diesel::{dsl::sql, prelude::*};

    let query = room::table
        .inner_join(rtc::table.inner_join(recording::table))
        .inner_join(janus_backend::table.on(janus_backend::id.nullable().eq(room::backend_id)))
        .filter(
            room::rtc_sharing_policy.eq_any(&[RtcSharingPolicy::Shared, RtcSharingPolicy::Owned]),
        )
        .filter(janus_backend::api_version.eq(JANUS_API_VERSION))
        .filter(sql("upper(\"room\".\"time\") < now()"))
        .filter(recording::status.eq(RecordingStatus::InProgress))
        .select((
            self::ALL_COLUMNS,
            super::recording::ALL_COLUMNS,
            super::janus_backend::ALL_COLUMNS,
        ));

    if let Some(group) = maybe_group {
        query
            .filter(
                janus_backend::group
                    .eq(group)
                    .or(janus_backend::group.is_null()),
            )
            .load(conn)
    } else {
        query.load(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "room"]
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

    pub fn classroom_id(self, classroom_id: Uuid) -> Self {
        Self {
            classroom_id,
            ..self
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::room::dsl::room;
        use diesel::RunQueryDsl;

        diesel::insert_into(room).values(self).get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, AsChangeset)]
#[table_name = "room"]
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

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        diesel::update(self).set(self).get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub fn set_closed_by(room_id: Id, agent: &AgentId, conn: &PgConnection) -> Result<Object, Error> {
    use diesel::dsl::sql;
    use diesel::prelude::*;

    diesel::update(room::table.filter(room::id.eq(room_id)))
        .set((
            room::closed_by.eq(agent),
            room::time.eq(sql("TSTZRANGE(LOWER(time), NOW())")),
        ))
        .get_result(conn)
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod finished_with_in_progress_recordings {
        use super::super::*;
        use crate::{
            backend::janus::client::{HandleId, SessionId},
            test_helpers::{prelude::*, test_deps::LocalDeps},
        };

        #[test]
        fn selects_appropriate_backend() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let pool = db.connection_pool();
            let conn = pool.get().expect("Failed to get db connection");

            let backend1 = shared_helpers::insert_janus_backend(
                &conn,
                "test",
                SessionId::random(),
                HandleId::random(),
            );
            let backend2 = shared_helpers::insert_janus_backend(
                &conn,
                "test",
                SessionId::random(),
                HandleId::random(),
            );

            let room1 = shared_helpers::insert_closed_room_with_backend_id(&conn, backend1.id());
            let room2 = shared_helpers::insert_closed_room_with_backend_id(&conn, backend2.id());

            // this room will have rtc but no rtc_stream simulating the case when janus_backend was removed
            // (for example crashed) and via cascade removed all streams hosted on it
            //
            // it should not appear in query result and it should not result in query Err
            let backend3_id = TestAgent::new("alpha", "janus3", SVC_AUDIENCE);

            let room3 =
                shared_helpers::insert_closed_room_with_backend_id(&conn, backend3_id.agent_id());

            let rtc1 = shared_helpers::insert_rtc_with_room(&conn, &room1);
            let rtc2 = shared_helpers::insert_rtc_with_room(&conn, &room2);
            let _rtc3 = shared_helpers::insert_rtc_with_room(&conn, &room3);

            shared_helpers::insert_recording(&conn, &rtc1);
            shared_helpers::insert_recording(&conn, &rtc2);

            let rooms = finished_with_in_progress_recordings(&conn, None)
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

        #[test]
        fn selects_appropriate_backend_by_group() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let pool = db.connection_pool();
            let conn = pool.get().expect("Failed to get db connection");

            let backend1 = shared_helpers::insert_janus_backend_with_group(
                &conn,
                "test",
                SessionId::random(),
                HandleId::random(),
                "webinar",
            );
            let backend2 = shared_helpers::insert_janus_backend_with_group(
                &conn,
                "test",
                SessionId::random(),
                HandleId::random(),
                "minigroup",
            );

            let room1 = shared_helpers::insert_closed_room_with_backend_id(&conn, backend1.id());
            let room2 = shared_helpers::insert_closed_room_with_backend_id(&conn, backend2.id());

            let rtc1 = shared_helpers::insert_rtc_with_room(&conn, &room1);
            let rtc2 = shared_helpers::insert_rtc_with_room(&conn, &room2);

            shared_helpers::insert_recording(&conn, &rtc1);
            shared_helpers::insert_recording(&conn, &rtc2);

            let rooms = finished_with_in_progress_recordings(&conn, Some("minigroup"))
                .expect("finished_with_in_progress_recordings call failed");

            assert_eq!(rooms.len(), 1);
            assert_eq!(rooms[0].0.id(), room2.id());
        }
    }
}
