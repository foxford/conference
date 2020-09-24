use std::fmt;
use std::ops::Bound;

use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::db::janus_backend::Object as JanusBackend;
use crate::db::recording::{Object as Recording, Status as RecordingStatus};
use crate::schema::{room, rtc};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type Time = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

/// Use to filter by not expired room allowing time before room opening.
///
///    [-----room.time-----]
/// [---------------------------- OK
///              [--------------- OK
///                           [-- NOT OK
pub(crate) fn since_now() -> Time {
    (Bound::Included(Utc::now()), Bound::Unbounded)
}

/// Use to filter strictly by room time range.
///
///    [-----room.time-----]
///  |                            NOT OK
///              |                OK
///                          |    NOT OK
pub(crate) fn now() -> Time {
    let now = Utc::now();
    (Bound::Included(now), Bound::Included(now))
}

////////////////////////////////////////////////////////////////////////////////

type AllColumns = (
    room::id,
    room::time,
    room::audience,
    room::created_at,
    room::backend,
    room::reserve,
    room::tags,
);

const ALL_COLUMNS: AllColumns = (
    room::id,
    room::time,
    room::audience,
    room::created_at,
    room::backend,
    room::reserve,
    room::tags,
);

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, Serialize, Identifiable, Queryable, QueryableByName)]
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
}

impl Object {
    pub(crate) fn audience(&self) -> &str {
        &self.audience
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }

    #[cfg(test)]
    pub(crate) fn time(&self) -> &Time {
        &self.time
    }

    pub(crate) fn backend(&self) -> RoomBackend {
        self.backend
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub(crate) fn tags(&self) -> &JsonValue {
        &self.tags
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FindQuery {
    id: Option<Uuid>,
    rtc_id: Option<Uuid>,
    time: Option<Time>,
}

impl FindQuery {
    pub(crate) fn new() -> Self {
        Self {
            id: None,
            rtc_id: None,
            time: None,
        }
    }

    pub(crate) fn id(self, id: Uuid) -> Self {
        Self {
            id: Some(id),
            ..self
        }
    }

    pub(crate) fn rtc_id(self, rtc_id: Uuid) -> Self {
        Self {
            rtc_id: Some(rtc_id),
            ..self
        }
    }

    pub(crate) fn time(self, time: Time) -> Self {
        Self {
            time: Some(time),
            ..self
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;
        use diesel::{dsl::sql, sql_types::Tstzrange};

        let mut q = room::table.into_boxed();

        if let Some(time) = self.time {
            q = q.filter(sql("room.time && ").bind::<Tstzrange, _>(time));
        }

        match (self.id, self.rtc_id) {
            (Some(id), None) => q.filter(room::id.eq(id)).get_result(conn).optional(),
            (None, Some(rtc_id)) => q
                .inner_join(rtc::table)
                .filter(rtc::id.eq(rtc_id))
                .select(ALL_COLUMNS)
                .get_result(conn)
                .optional(),
            _ => Err(Error::QueryBuilderError("id either rtc_id required".into())),
        }
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
    use crate::schema;
    use diesel::{dsl::sql, prelude::*};

    schema::room::table
        .inner_join(
            schema::rtc::table.inner_join(
                schema::recording::table.inner_join(
                    schema::janus_backend::table
                        .on(schema::janus_backend::id.eq(schema::recording::backend_id)),
                ),
            ),
        )
        .filter(room::backend.eq(RoomBackend::Janus))
        .filter(sql("upper(\"room\".\"time\") < now()"))
        .filter(schema::recording::status.eq(RecordingStatus::InProgress))
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
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(time: Time, audience: &'a str, backend: RoomBackend) -> Self {
        Self {
            time,
            audience,
            backend,
            reserve: None,
            tags: None,
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

#[derive(Debug, Identifiable, AsChangeset)]
#[table_name = "room"]
pub(crate) struct UpdateQuery {
    id: Uuid,
    time: Option<Time>,
    audience: Option<String>,
    backend: Option<RoomBackend>,
    reserve: Option<Option<i32>>,
    tags: Option<JsonValue>,
}

impl UpdateQuery {
    pub(crate) fn new(id: Uuid) -> Self {
        Self {
            id,
            time: None,
            audience: None,
            backend: None,
            reserve: None,
            tags: None,
        }
    }

    pub(crate) fn time(self, time: Option<Time>) -> Self {
        Self { time, ..self }
    }

    pub(crate) fn audience(self, audience: Option<String>) -> Self {
        Self { audience, ..self }
    }

    pub(crate) fn backend(self, backend: Option<RoomBackend>) -> Self {
        Self { backend, ..self }
    }

    pub(crate) fn reserve(self, reserve: Option<Option<i32>>) -> Self {
        Self { reserve, ..self }
    }

    pub(crate) fn tags(self, tags: Option<JsonValue>) -> Self {
        Self { tags, ..self }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        diesel::update(self).set(self).get_result(conn)
    }
}

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

            let room1 = shared_helpers::insert_closed_room(&conn);
            let room2 = shared_helpers::insert_closed_room(&conn);

            // this room will have rtc but no rtc_stream simulating the case when janus_backend was removed
            // (for example crashed) and via cascade removed all streams hosted on it
            //
            // it should not appear in query result and it should not result in query Err
            let room3 = shared_helpers::insert_closed_room(&conn);

            let backend1 = shared_helpers::insert_janus_backend(&conn);
            let backend2 = shared_helpers::insert_janus_backend(&conn);

            let rtc1 = shared_helpers::insert_rtc_with_room(&conn, &room1);
            let rtc2 = shared_helpers::insert_rtc_with_room(&conn, &room2);
            let _rtc3 = shared_helpers::insert_rtc_with_room(&conn, &room3);

            shared_helpers::insert_recording(&conn, &rtc1, &backend1);
            shared_helpers::insert_recording(&conn, &rtc2, &backend2);

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
