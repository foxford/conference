use std::fmt;
use std::ops::Bound;

use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::backend::janus::JANUS_API_VERSION;
use crate::db::janus_backend::Object as JanusBackend;
use crate::db::recording::{Object as Recording, Status as RecordingStatus};
use crate::db::rtc::SharingPolicy as RtcSharingPolicy;
use crate::schema::{janus_backend, recording, room, rtc};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type Time = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

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
);

////////////////////////////////////////////////////////////////////////////////

// Deprecated in favor of `crate::db::rtc::SharingPolicy`.
#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[DieselType = "Room_backend"]
// This is not just `Backend` because of clash with `diesel::backend::Backend`.
pub(crate) enum RoomBackend {
    None,
    Janus,
}

impl fmt::Display for RoomBackend {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let serialized = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        write!(f, "{}", serialized)
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

impl Into<RtcSharingPolicy> for RoomBackend {
    fn into(self) -> RtcSharingPolicy {
        match self {
            Self::None => RtcSharingPolicy::None,
            Self::Janus => RtcSharingPolicy::Shared,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(
    Clone, Debug, Deserialize, Serialize, Identifiable, Queryable, QueryableByName, Associations,
)]
#[belongs_to(JanusBackend, foreign_key = "backend_id")]
#[table_name = "room"]
pub(crate) struct Object {
    id: Uuid,
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
}

impl Object {
    pub(crate) fn audience(&self) -> &str {
        &self.audience
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn time(&self) -> &Time {
        &self.time
    }

    pub(crate) fn reserve(&self) -> Option<i32> {
        self.reserve
    }

    pub(crate) fn is_closed(&self) -> bool {
        match self.time.1 {
            Bound::Included(t) => t < Utc::now(),
            Bound::Excluded(t) => t <= Utc::now(),
            Bound::Unbounded => false,
        }
    }

    pub(crate) fn tags(&self) -> &JsonValue {
        &self.tags
    }

    pub(crate) fn backend_id(&self) -> Option<&AgentId> {
        self.backend_id.as_ref()
    }

    pub(crate) fn rtc_sharing_policy(&self) -> RtcSharingPolicy {
        self.rtc_sharing_policy
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait FindQueryable {
    fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error>;
}

#[derive(Debug)]
pub(crate) struct FindQuery {
    id: Uuid,
}

impl FindQuery {
    pub(crate) fn new(id: Uuid) -> Self {
        Self { id }
    }
}

impl FindQueryable for FindQuery {
    fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        room::table
            .filter(room::id.eq(self.id))
            .get_result(conn)
            .optional()
    }
}

#[derive(Debug)]
pub(crate) struct FindByRtcIdQuery {
    rtc_id: Uuid,
}

impl FindByRtcIdQuery {
    pub(crate) fn new(rtc_id: Uuid) -> Self {
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
pub(crate) fn finished_with_in_progress_recordings(
    conn: &PgConnection,
) -> Result<Vec<(Object, Recording, JanusBackend)>, Error> {
    use diesel::{dsl::sql, prelude::*};

    room::table
        .inner_join(rtc::table.inner_join(recording::table))
        .inner_join(janus_backend::table.on(janus_backend::id.nullable().eq(room::backend_id)))
        .filter(room::backend.eq(RoomBackend::Janus))
        .filter(janus_backend::api_version.eq(JANUS_API_VERSION))
        .filter(sql("upper(\"room\".\"time\") < now()"))
        .filter(recording::status.eq(RecordingStatus::InProgress))
        .select((
            self::ALL_COLUMNS,
            super::recording::ALL_COLUMNS,
            super::janus_backend::ALL_COLUMNS,
        ))
        .load(conn)
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "room"]
pub(crate) struct InsertQuery<'a> {
    time: Time,
    audience: &'a str,
    backend: RoomBackend,
    reserve: Option<i32>,
    tags: Option<&'a JsonValue>,
    backend_id: Option<&'a AgentId>,
    rtc_sharing_policy: RtcSharingPolicy,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(time: Time, audience: &'a str, rtc_sharing_policy: RtcSharingPolicy) -> Self {
        Self {
            time,
            audience,
            backend: rtc_sharing_policy.into(),
            reserve: None,
            tags: None,
            backend_id: None,
            rtc_sharing_policy,
        }
    }

    pub(crate) fn reserve(self, value: i32) -> Self {
        Self {
            reserve: Some(value),
            ..self
        }
    }

    pub(crate) fn tags(self, value: &'a JsonValue) -> Self {
        Self {
            tags: Some(value),
            ..self
        }
    }

    #[cfg(test)]
    pub(crate) fn backend_id(self, backend_id: &'a AgentId) -> Self {
        Self {
            backend_id: Some(backend_id),
            ..self
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::room::dsl::room;
        use diesel::RunQueryDsl;

        diesel::insert_into(room).values(self).get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct DeleteQuery {
    id: Uuid,
}

impl DeleteQuery {
    pub(crate) fn new(id: Uuid) -> Self {
        Self { id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use diesel::prelude::*;

        diesel::delete(room::table.filter(room::id.eq(self.id))).execute(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default, Identifiable, AsChangeset)]
#[table_name = "room"]
pub(crate) struct UpdateQuery<'a> {
    id: Uuid,
    time: Option<Time>,
    audience: Option<String>,
    reserve: Option<Option<i32>>,
    tags: Option<JsonValue>,
    backend_id: Option<&'a AgentId>,
}

impl<'a> UpdateQuery<'a> {
    pub(crate) fn new(id: Uuid) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    pub(crate) fn time(self, time: Option<Time>) -> Self {
        Self { time, ..self }
    }

    pub(crate) fn audience(self, audience: Option<String>) -> Self {
        Self { audience, ..self }
    }

    pub(crate) fn reserve(self, reserve: Option<Option<i32>>) -> Self {
        Self { reserve, ..self }
    }

    pub(crate) fn tags(self, tags: Option<JsonValue>) -> Self {
        Self { tags, ..self }
    }

    pub(crate) fn backend_id(self, backend_id: Option<&'a AgentId>) -> Self {
        Self { backend_id, ..self }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        diesel::update(self).set(self).get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod finished_with_in_progress_recordings {
        use super::super::*;
        use crate::test_helpers::prelude::*;

        #[test]
        fn selects_appropriate_backend() {
            let db = TestDb::new();
            let pool = db.connection_pool();
            let conn = pool.get().expect("Failed to get db connection");

            let backend1 = shared_helpers::insert_janus_backend(&conn);
            let backend2 = shared_helpers::insert_janus_backend(&conn);

            let room1 = shared_helpers::insert_closed_room_with_backend(&conn, backend1.id());
            let room2 = shared_helpers::insert_closed_room_with_backend(&conn, backend2.id());

            // this room will have rtc but no rtc_stream simulating the case when janus_backend was removed
            // (for example crashed) and via cascade removed all streams hosted on it
            //
            // it should not appear in query result and it should not result in query Err
            let backend3_id = TestAgent::new("alpha", "janus3", SVC_AUDIENCE);

            let room3 =
                shared_helpers::insert_closed_room_with_backend(&conn, backend3_id.agent_id());

            let rtc1 = shared_helpers::insert_rtc_with_room(&conn, &room1);
            let rtc2 = shared_helpers::insert_rtc_with_room(&conn, &room2);
            let _rtc3 = shared_helpers::insert_rtc_with_room(&conn, &room3);

            shared_helpers::insert_recording(&conn, &rtc1);
            shared_helpers::insert_recording(&conn, &rtc2);

            let rooms = finished_with_in_progress_recordings(&conn)
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
    }
}
