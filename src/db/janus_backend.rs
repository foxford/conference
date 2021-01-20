use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::schema::janus_backend;

pub(crate) type AllColumns = (
    janus_backend::id,
    janus_backend::handle_id,
    janus_backend::session_id,
    janus_backend::created_at,
    janus_backend::capacity,
    janus_backend::balancer_capacity,
);

pub(crate) const ALL_COLUMNS: AllColumns = (
    janus_backend::id,
    janus_backend::handle_id,
    janus_backend::session_id,
    janus_backend::created_at,
    janus_backend::capacity,
    janus_backend::balancer_capacity,
);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, QueryableByName, Associations)]
#[table_name = "janus_backend"]
pub(crate) struct Object {
    id: AgentId,
    handle_id: i64,
    session_id: i64,
    created_at: DateTime<Utc>,
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
}

impl Object {
    pub(crate) fn id(&self) -> &AgentId {
        &self.id
    }

    pub(crate) fn handle_id(&self) -> i64 {
        self.handle_id
    }

    pub(crate) fn session_id(&self) -> i64 {
        self.session_id
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FindQuery<'a> {
    id: Option<&'a AgentId>,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new() -> Self {
        Self { id: None }
    }

    pub(crate) fn id(self, id: &'a AgentId) -> Self {
        Self { id: Some(id) }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
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
pub(crate) struct UpsertQuery<'a> {
    id: &'a AgentId,
    handle_id: i64,
    session_id: i64,
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
}

impl<'a> UpsertQuery<'a> {
    pub(crate) fn new(id: &'a AgentId, handle_id: i64, session_id: i64) -> Self {
        Self {
            id,
            handle_id,
            session_id,
            capacity: None,
            balancer_capacity: None,
        }
    }

    pub(crate) fn capacity(self, capacity: i32) -> Self {
        Self {
            capacity: Some(capacity),
            ..self
        }
    }

    pub(crate) fn balancer_capacity(self, balancer_capacity: i32) -> Self {
        Self {
            balancer_capacity: Some(balancer_capacity),
            ..self
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
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

pub(crate) struct DeleteQuery<'a> {
    id: &'a AgentId,
}

impl<'a> DeleteQuery<'a> {
    pub(crate) fn new(id: &'a AgentId) -> Self {
        Self { id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use diesel::prelude::*;

        diesel::delete(janus_backend::table.filter(janus_backend::id.eq(self.id))).execute(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

// Returns the most loaded backend capable to host the room with its reserve considering:
// - room opening period;
// - actual number of online agents;
// - optional backend capacity;
// - optional room reserve.
const MOST_LOADED_SQL: &str = r#"
    WITH
        room_load AS (
            SELECT
                a.room_id,
                COUNT(a.id) AS taken
            FROM agent AS a
            INNER JOIN agent_connection AS ac
            ON ac.agent_id = a.id
            GROUP BY a.room_id
        ),
        active_room AS (
            SELECT *
            FROM room
            WHERE backend = 'janus'
            AND   backend_id IS NOT NULL
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
    AND   COALESCE(jb.balancer_capacity, jb.capacity, 2147483647) - COALESCE(jbl.load, 0) >= COALESCE(r2.reserve, 0)
    ORDER BY COALESCE(jbl.load, 0) DESC
    LIMIT 1
"#;

pub(crate) fn most_loaded(room_id: Uuid, conn: &PgConnection) -> Result<Option<Object>, Error> {
    use diesel::prelude::*;
    use diesel::sql_types::Uuid;

    diesel::sql_query(MOST_LOADED_SQL)
        .bind::<Uuid, _>(room_id)
        .get_result(conn)
        .optional()
}

// The same as above but finds the least loaded backend instead without considering the reserve.
const LEAST_LOADED_SQL: &str = r#"
    WITH
        room_load AS (
            SELECT
                a.room_id,
                COUNT(a.id) AS taken
            FROM agent AS a
            INNER JOIN agent_connection AS ac
            ON ac.agent_id = a.id
            GROUP BY a.room_id
        ),
        active_room AS (
            SELECT *
            FROM room
            WHERE backend = 'janus'
            AND   backend_id IS NOT NULL
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
    ORDER BY COALESCE(jb.balancer_capacity, jb.capacity, 2147483647) - COALESCE(jbl.load, 0) DESC
    LIMIT 1
"#;

pub(crate) fn least_loaded(room_id: Uuid, conn: &PgConnection) -> Result<Option<Object>, Error> {
    use diesel::prelude::*;
    use diesel::sql_types::Uuid;

    diesel::sql_query(LEAST_LOADED_SQL)
        .bind::<Uuid, _>(room_id)
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
                r.id        AS room_id,
                r.reserve,
                COUNT(a.id) AS taken
            FROM agent AS a
            INNER JOIN room AS r
            ON r.id = a.room_id
            INNER JOIN agent_connection AS ac
            ON ac.agent_id = a.id
            GROUP BY r.id
        ),
        active_room AS (
            SELECT *
            FROM room
            WHERE backend = 'janus'
            AND   backend_id IS NOT NULL
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
    FROM active_room AS ar
    LEFT JOIN room_load as rl
    ON rl.room_id = ar.id
    LEFT JOIN janus_backend AS jb
    ON jb.id = ar.backend_id
    LEFT JOIN janus_backend_load AS jbl
    ON jbl.backend_id = jb.id
    WHERE ar.id = $1
"#;

#[derive(QueryableByName)]
struct FreeCapacityQueryRow {
    #[sql_type = "diesel::sql_types::Integer"]
    free_capacity: i32,
}

pub(crate) fn free_capacity(room_id: Uuid, conn: &PgConnection) -> Result<i32, Error> {
    use diesel::prelude::*;
    use diesel::sql_types::Uuid;

    diesel::sql_query(FREE_CAPACITY_SQL)
        .bind::<Uuid, _>(room_id)
        .get_result::<FreeCapacityQueryRow>(conn)
        .map(|row| row.free_capacity)
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn total_capacity(conn: &PgConnection) -> Result<i64, Error> {
    use diesel::dsl::sum;
    use diesel::prelude::*;

    janus_backend::table
        .select(sum(janus_backend::capacity))
        .get_result::<Option<i64>>(conn)
        .map(|v| v.unwrap_or(0))
}

pub(crate) fn count(conn: &PgConnection) -> Result<i64, Error> {
    use diesel::dsl::count;
    use diesel::prelude::*;

    janus_backend::table
        .select(count(janus_backend::id))
        .get_result(conn)
}

#[derive(QueryableByName, Debug)]
pub(crate) struct ReserveLoadQueryLoad {
    #[sql_type = "svc_agent::sql::Agent_id"]
    pub backend_id: AgentId,
    #[sql_type = "diesel::sql_types::BigInt"]
    pub load: i64,
    #[sql_type = "diesel::sql_types::BigInt"]
    pub taken: i64,
}

pub(crate) fn reserve_load_for_each_backend(
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
            COUNT(a.id) AS taken
        FROM agent AS a
        INNER JOIN agent_connection AS ac
        ON ac.agent_id = a.id
        GROUP BY a.room_id
    ),
    active_room AS (
        SELECT *
        FROM room
        WHERE backend = 'janus'
        AND   backend_id IS NOT NULL
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

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use std::ops::Bound;

    use crate::db::room::RoomBackend;
    use crate::test_helpers::prelude::*;

    #[test]
    fn reserve_load_for_each_backend() {
        async_std::task::block_on(async {
            // Insert an rtc and janus backend.
            let now = Utc::now();

            let conn = TestDb::new()
                .connection_pool()
                .get()
                .expect("Failed to get db conn");
            // Insert janus backends.
            let backend1 = shared_helpers::insert_janus_backend(&conn);
            let backend2 = shared_helpers::insert_janus_backend(&conn);
            let backend3 = shared_helpers::insert_janus_backend(&conn);

            let room1 = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(now),
                    Bound::Excluded(now + Duration::hours(1)),
                ))
                .backend(RoomBackend::Janus)
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
                .backend(RoomBackend::Janus)
                .backend_id(backend1.id())
                .insert(&conn);

            let room3 = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(now),
                    Bound::Excluded(now + Duration::hours(1)),
                ))
                .reserve(400)
                .backend(RoomBackend::Janus)
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
        });
    }
}
