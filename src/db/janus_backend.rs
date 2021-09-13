use crate::db;
use crate::{
    backend::janus::{
        client::{HandleId, SessionId},
        JANUS_API_VERSION,
    },
    schema::janus_backend,
};
use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use svc_agent::{sql::Agent_id, AgentId};

pub type AllColumns = (
    janus_backend::id,
    janus_backend::handle_id,
    janus_backend::session_id,
    janus_backend::created_at,
    janus_backend::capacity,
    janus_backend::balancer_capacity,
    janus_backend::api_version,
    janus_backend::group,
    janus_backend::janus_url,
);

pub const ALL_COLUMNS: AllColumns = (
    janus_backend::id,
    janus_backend::handle_id,
    janus_backend::session_id,
    janus_backend::created_at,
    janus_backend::capacity,
    janus_backend::balancer_capacity,
    janus_backend::api_version,
    janus_backend::group,
    janus_backend::janus_url,
);

////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug, Identifiable, Queryable, QueryableByName, Associations, PartialEq, Eq, Hash, Clone,
)]
#[table_name = "janus_backend"]
pub struct Object {
    id: AgentId,
    handle_id: HandleId,
    session_id: SessionId,
    created_at: DateTime<Utc>,
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
    api_version: String,
    group: Option<String>,
    janus_url: String,
}

impl Object {
    pub fn id(&self) -> &AgentId {
        &self.id
    }

    pub fn handle_id(&self) -> HandleId {
        self.handle_id
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn janus_url(&self) -> &str {
        &self.janus_url
    }

    /// Get a reference to the object's group.
    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct ListQuery<'a> {
    ids: Option<&'a [&'a AgentId]>,
    offset: Option<i64>,
    limit: Option<i64>,
}

impl<'a> ListQuery<'a> {
    pub fn new() -> Self {
        Self {
            ids: None,
            offset: None,
            limit: None,
        }
    }

    pub fn ids(self, ids: &'a [&'a AgentId]) -> Self {
        Self {
            ids: Some(ids),
            ..self
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::prelude::*;

        let mut q = janus_backend::table.into_boxed();
        if let Some(ids) = self.ids {
            q = q.filter(janus_backend::id.eq_any(ids))
        }
        if let Some(offset) = self.offset {
            q = q.offset(offset);
        }
        if let Some(limit) = self.limit {
            q = q.limit(limit);
        }
        q.order_by(janus_backend::created_at).get_results(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct FindQuery<'a> {
    id: Option<&'a AgentId>,
}

impl<'a> FindQuery<'a> {
    pub fn new() -> Self {
        Self { id: None }
    }

    pub fn id(self, id: &'a AgentId) -> Self {
        Self { id: Some(id) }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        match self.id {
            Some(ref id) => janus_backend::table.find(id).get_result(conn).optional(),
            None => Err(Error::QueryBuilderError(
                "id parameter is required parameter of the query".into(),
            )),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "janus_backend"]
pub struct UpsertQuery<'a> {
    id: &'a AgentId,
    handle_id: HandleId,
    session_id: SessionId,
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
    api_version: String,
    group: Option<&'a str>,
    janus_url: &'a str,
}

impl<'a> UpsertQuery<'a> {
    pub fn new(
        id: &'a AgentId,
        handle_id: HandleId,
        session_id: SessionId,
        janus_url: &'a str,
    ) -> Self {
        Self {
            id,
            handle_id,
            session_id,
            capacity: None,
            balancer_capacity: None,
            api_version: JANUS_API_VERSION.to_string(),
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

    pub fn group(self, group: &'a str) -> Self {
        Self {
            group: Some(group),
            ..self
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::janus_backend::dsl::janus_backend;
        use diesel::RunQueryDsl;

        diesel::insert_into(janus_backend)
            .values(self)
            .on_conflict(crate::schema::janus_backend::id)
            .do_update()
            .set(self)
            .get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct DeleteQuery<'a> {
    id: &'a AgentId,
}

impl<'a> DeleteQuery<'a> {
    pub fn new(id: &'a AgentId) -> Self {
        Self { id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use diesel::prelude::*;

        diesel::delete(janus_backend::table.filter(janus_backend::id.eq(self.id))).execute(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

// Returns the most loaded backend capable to host the room with its reserve considering:
// - room opening period;
// - actual number of online agents;
// - optional backend capacity;
// - optional room reserve;
// - writer's bitrate;
// - possible multiple RTCs in each room.
const MOST_LOADED_SQL: &str = r#"
    WITH
        room_load AS (
            SELECT
                a.room_id,
                SUM(COALESCE(rwc.video_remb, 1000000) / 1000000.0) AS taken
            FROM agent AS a
            INNER JOIN agent_connection AS ac
            ON ac.agent_id = a.id
            LEFT JOIN rtc
            ON rtc.id = ac.rtc_id
            LEFT JOIN rtc_writer_config AS rwc
            ON rwc.rtc_id = rtc.id
            GROUP BY a.room_id
        ),
        active_room AS (
            SELECT *
            FROM room
            WHERE backend_id IS NOT NULL
            AND   LOWER(time) <= NOW()
            AND   (UPPER(time) IS NULL OR UPPER(time) > NOW())
        ),
        janus_backend_load AS (
            SELECT
                backend_id,
                SUM(GREATEST(taken, reserve)) AS load
            FROM (
                SELECT DISTINCT ON(backend_id, room_id)
                    ar.backend_id,
                    ar.id                   AS room_id,
                    COALESCE(rl.taken, 0)   AS taken,
                    COALESCE(ar.reserve, 0) AS reserve
                FROM active_room AS ar
                LEFT JOIN room_load AS rl
                ON rl.room_id = ar.id
            ) AS sub
            GROUP BY backend_id
        )
    SELECT jb.*
    FROM janus_backend AS jb
    LEFT JOIN janus_backend_load AS jbl
    ON jbl.backend_id = jb.id
    LEFT JOIN room AS r2
    ON 1 = 1
    WHERE r2.id = $1
    AND   COALESCE(jb.balancer_capacity, jb.capacity, 2147483647) - COALESCE(jbl.load, 0) >= COALESCE(r2.reserve, 1)
    AND   jb.api_version = $2
    AND   ($3 IS NULL OR jb."group" = $3)
    ORDER BY COALESCE(jbl.load, 0) DESC, RANDOM()
    LIMIT 1
"#;

pub fn most_loaded(
    room_id: db::room::Id,
    group: Option<&str>,
    conn: &PgConnection,
) -> Result<Option<Object>, Error> {
    use diesel::{
        prelude::*,
        sql_types::{Nullable, Text, Uuid},
    };

    diesel::sql_query(MOST_LOADED_SQL)
        .bind::<Uuid, _>(room_id)
        .bind::<Text, _>(JANUS_API_VERSION)
        .bind::<Nullable<Text>, _>(group)
        .get_result(conn)
        .optional()
}

// The same as above but finds the least loaded backend instead without considering the reserve.
const LEAST_LOADED_SQL: &str = r#"
    WITH
        room_load AS (
            SELECT
                a.room_id,
                SUM(COALESCE(rwc.video_remb, 1000000) / 1000000.0) AS taken
            FROM agent AS a
            INNER JOIN agent_connection AS ac
            ON ac.agent_id = a.id
            LEFT JOIN rtc
            ON rtc.id = ac.rtc_id
            LEFT JOIN rtc_writer_config AS rwc
            ON rwc.rtc_id = rtc.id
            GROUP BY a.room_id
        ),
        active_room AS (
            SELECT *
            FROM room
            WHERE backend_id IS NOT NULL
            AND   LOWER(time) <= NOW()
            AND   (UPPER(time) IS NULL OR UPPER(time) > NOW())
        ),
        janus_backend_load AS (
            SELECT
                backend_id,
                SUM(taken) AS load
            FROM (
                SELECT DISTINCT ON(backend_id, room_id)
                    ar.backend_id,
                    ar.id                 AS room_id,
                    COALESCE(rl.taken, 0) AS taken
                FROM active_room AS ar
                LEFT JOIN room_load AS rl
                ON rl.room_id = ar.id
            ) AS sub
            GROUP BY backend_id
        )
    SELECT jb.*
    FROM janus_backend AS jb
    LEFT JOIN janus_backend_load AS jbl
    ON jbl.backend_id = jb.id
    LEFT JOIN room AS r2
    ON 1 = 1
    WHERE r2.id = $1
    AND   jb.api_version = $2
    AND   ($3 IS NULL OR jb."group" = $3)
    ORDER BY
        COALESCE(jb.balancer_capacity, jb.capacity, 2147483647) - COALESCE(jbl.load, 0) DESC,
        RANDOM()
    LIMIT 1
"#;

pub fn least_loaded(
    room_id: db::room::Id,
    group: Option<&str>,
    conn: &PgConnection,
) -> Result<Option<Object>, Error> {
    use diesel::{
        prelude::*,
        sql_types::{Nullable, Text, Uuid},
    };

    diesel::sql_query(LEAST_LOADED_SQL)
        .bind::<Uuid, _>(room_id)
        .bind::<Text, _>(JANUS_API_VERSION)
        .bind::<Nullable<Text>, _>(group)
        .get_result(conn)
        .optional()
}

////////////////////////////////////////////////////////////////////////////////

// Similar to the previous one but returns the number of free slots for the room on the backend
// that hosts the active stream for the given RTC.
const FREE_CAPACITY_SQL: &str = r#"
    WITH
        room_load AS (
            SELECT
                a.room_id,
                SUM(COALESCE(rwc.video_remb, 1000000) / 1000000.0) AS taken
            FROM agent AS a
            INNER JOIN agent_connection AS ac
            ON ac.agent_id = a.id
            LEFT JOIN rtc
            ON rtc.id = ac.rtc_id
            LEFT JOIN rtc_writer_config AS rwc
            ON rwc.rtc_id = rtc.id
            GROUP BY a.room_id
        ),
        active_room AS (
            SELECT *
            FROM room
            WHERE backend_id = $1
            AND   LOWER(time) <= NOW()
            AND   (UPPER(time) IS NULL OR UPPER(time) > NOW())
        ),
        janus_backend_load AS (
            SELECT
                backend_id,
                SUM(taken) AS total_taken,
                SUM(reserve) AS total_reserve,
                SUM(GREATEST(taken, reserve)) AS load
            FROM (
                SELECT DISTINCT ON(backend_id, room_id)
                    ar.backend_id,
                    ar.id                   AS room_id,
                    COALESCE(rl.taken, 0)   AS taken,
                    COALESCE(ar.reserve, 0) AS reserve
                FROM active_room AS ar
                LEFT JOIN room_load AS rl
                ON rl.room_id = ar.id
            ) AS sub
            GROUP BY backend_id
        )
    SELECT
        (
            CASE
                WHEN COALESCE(jb.capacity, 2147483647) <= COALESCE(jbl.total_taken, 0) THEN 0
                ELSE (
                    GREATEST(
                        (
                            CASE
                                WHEN COALESCE(ar.reserve, 0) > COALESCE(rl.taken, 0)
                                    THEN LEAST(
                                        COALESCE(ar.reserve, 0) - COALESCE(rl.taken, 0),
                                        COALESCE(jb.capacity, 2147483647) - COALESCE(jbl.total_taken, 0)
                                    )
                                ELSE
                                    GREATEST(COALESCE(jb.capacity, 2147483647) - COALESCE(jbl.load, 0), 0)
                            END
                        ),
                    1)
                )
            END
        )::INT AS free_capacity
    FROM rtc
    LEFT JOIN active_room AS ar
    ON ar.id = rtc.room_id
    LEFT JOIN room_load as rl
    ON rl.room_id = rtc.room_id
    LEFT JOIN janus_backend AS jb
    ON jb.id = ar.backend_id
    LEFT JOIN janus_backend_load AS jbl
    ON jbl.backend_id = jb.id
    WHERE rtc.id = $2
"#;

#[derive(QueryableByName)]
struct FreeCapacityQueryRow {
    #[sql_type = "diesel::sql_types::Integer"]
    free_capacity: i32,
}

pub fn free_capacity(
    backend_id: &AgentId,
    rtc_id: db::rtc::Id,
    conn: &PgConnection,
) -> Result<i32, Error> {
    use diesel::{prelude::*, sql_types::Uuid};

    diesel::sql_query(FREE_CAPACITY_SQL)
        .bind::<Agent_id, _>(backend_id)
        .bind::<Uuid, _>(rtc_id)
        .get_result::<FreeCapacityQueryRow>(conn)
        .map(|row| row.free_capacity)
}

////////////////////////////////////////////////////////////////////////////////

pub fn total_capacity(conn: &PgConnection) -> Result<i64, Error> {
    use diesel::{dsl::sum, prelude::*};

    janus_backend::table
        .select(sum(janus_backend::capacity))
        .get_result::<Option<i64>>(conn)
        .map(|v| v.unwrap_or(0))
}

pub fn count(conn: &PgConnection) -> Result<i64, Error> {
    use diesel::{dsl::count, prelude::*};

    janus_backend::table
        .select(count(janus_backend::id))
        .get_result(conn)
}

#[derive(QueryableByName, Debug)]
pub struct ReserveLoadQueryLoad {
    #[sql_type = "svc_agent::sql::Agent_id"]
    pub backend_id: AgentId,
    #[sql_type = "diesel::sql_types::BigInt"]
    pub load: i64,
    #[sql_type = "diesel::sql_types::BigInt"]
    pub taken: i64,
}

pub fn reserve_load_for_each_backend(
    conn: &PgConnection,
) -> Result<Vec<ReserveLoadQueryLoad>, Error> {
    use diesel::prelude::*;

    diesel::sql_query(LOAD_FOR_EACH_BACKEND).get_results(conn)
}

const LOAD_FOR_EACH_BACKEND: &str = r#"
WITH
    room_load AS (
        SELECT
            a.room_id,
            SUM(COALESCE(rwc.video_remb, 1000000) / 1000000.0) AS taken
        FROM agent AS a
        INNER JOIN agent_connection AS ac
        ON ac.agent_id = a.id
        LEFT JOIN rtc
        ON rtc.id = ac.rtc_id
        LEFT JOIN rtc_writer_config AS rwc
        ON rwc.rtc_id = rtc.id
        GROUP BY a.room_id
    ),
    active_room AS (
        SELECT *
        FROM room
        WHERE backend_id IS NOT NULL
        AND   LOWER(time) <= NOW()
        AND   (UPPER(time) IS NULL OR UPPER(time) > NOW())
    ),
    janus_backend_load AS (
        SELECT
            backend_id,
            SUM(reserve) AS load,
            SUM(taken) AS taken
        FROM (
            SELECT DISTINCT ON(backend_id, room_id)
                ar.backend_id,
                ar.id                   AS room_id,
                COALESCE(rl.taken, 0)   AS taken,
                COALESCE(ar.reserve, 0) AS reserve
            FROM active_room AS ar
            LEFT JOIN room_load AS rl
            ON rl.room_id = ar.id
        ) AS sub
        GROUP BY backend_id
    )
SELECT
    jb.id AS backend_id,
    COALESCE(jbl.load, 0)::BIGINT as load,
    COALESCE(jbl.taken, 0)::BIGINT as taken
FROM janus_backend jb
LEFT OUTER JOIN janus_backend_load jbl
ON jb.id = jbl.backend_id;
"#;

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use std::ops::Bound;

    use crate::{
        backend::janus::client::{HandleId, SessionId},
        db::rtc::SharingPolicy as RtcSharingPolicy,
        test_helpers::{prelude::*, test_deps::LocalDeps},
    };

    #[async_std::test]
    async fn reserve_load_for_each_backend() {
        // Insert an rtc and janus backend.
        let now = Utc::now();

        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let conn = TestDb::with_local_postgres(&postgres)
            .connection_pool()
            .get()
            .expect("Failed to get db conn");

        // Insert janus backends.
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
        let backend3 = shared_helpers::insert_janus_backend(
            &conn,
            "test",
            SessionId::random(),
            HandleId::random(),
        );

        let room1 = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((
                Bound::Included(now),
                Bound::Excluded(now + Duration::hours(1)),
            ))
            .rtc_sharing_policy(RtcSharingPolicy::Shared)
            .backend_id(backend1.id())
            .reserve(200)
            .insert(&conn);

        let room2 = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((
                Bound::Included(now),
                Bound::Excluded(now + Duration::hours(1)),
            ))
            .reserve(300)
            .rtc_sharing_policy(RtcSharingPolicy::Shared)
            .backend_id(backend1.id())
            .insert(&conn);

        let room3 = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((
                Bound::Included(now),
                Bound::Excluded(now + Duration::hours(1)),
            ))
            .reserve(400)
            .rtc_sharing_policy(RtcSharingPolicy::Shared)
            .backend_id(backend2.id())
            .insert(&conn);

        shared_helpers::insert_rtc_with_room(&conn, &room1);
        shared_helpers::insert_rtc_with_room(&conn, &room2);
        shared_helpers::insert_rtc_with_room(&conn, &room3);

        let loads = super::reserve_load_for_each_backend(&conn).expect("Db query failed");
        assert_eq!(loads.len(), 3);

        [backend1, backend2, backend3]
            .iter()
            .zip([500, 400, 0].iter())
            .for_each(|(backend, expected_load)| {
                let b = loads
                    .iter()
                    .find(|load| load.backend_id == *backend.id())
                    .expect("Failed to find backend in query results");

                assert_eq!(b.load, *expected_load as i64);
            });
    }
}
