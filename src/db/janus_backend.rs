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

pub(crate) struct ListQuery<'a> {
    ids: Option<&'a [&'a AgentId]>,
    offset: Option<i64>,
    limit: Option<i64>,
}

impl<'a> ListQuery<'a> {
    pub(crate) fn new() -> Self {
        Self {
            ids: None,
            offset: None,
            limit: None,
        }
    }

    pub(crate) fn ids(self, ids: &'a [&'a AgentId]) -> Self {
        Self {
            ids: Some(ids),
            ..self
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
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

pub(crate) struct FindQuery {
    id: Option<AgentId>,
}

impl FindQuery {
    pub(crate) fn new() -> Self {
        Self { id: None }
    }

    pub(crate) fn id(self, id: AgentId) -> Self {
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
                room_id,
                COUNT(id) AS taken
            FROM agent
            WHERE status = 'connected'
            GROUP BY room_id
        ),
        active_room AS (
            SELECT *
            FROM room
            WHERE backend = 'janus'
            AND   UPPER(time) BETWEEN NOW() AND NOW() + INTERVAL '1 day'
        ),
        janus_backend_load AS (
            SELECT
                backend_id,
                SUM(GREATEST(taken, reserve)) AS load
            FROM (
                SELECT DISTINCT ON(backend_id, room_id)
                    rec.backend_id,
                    rtc.room_id,
                    COALESCE(rl.taken, 0)   AS taken,
                    COALESCE(ar.reserve, 0) AS reserve
                FROM recording AS rec
                INNER JOIN rtc
                ON rtc.id = rec.rtc_id
                LEFT JOIN active_room AS ar
                ON ar.id = rtc.room_id
                LEFT JOIN room_load AS rl
                ON rl.room_id = rtc.room_id
                WHERE rec.status = 'in_progress'
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
                room_id,
                COUNT(id) AS taken
            FROM agent
            WHERE status = 'connected'
            GROUP BY room_id
        ),
        active_room AS (
            SELECT *
            FROM room
            WHERE backend = 'janus'
            AND   UPPER(time) BETWEEN NOW() AND NOW() + INTERVAL '1 day'
        ),
        janus_backend_load AS (
            SELECT
                backend_id,
                SUM(taken) AS load
            FROM (
                SELECT DISTINCT ON(backend_id, room_id)
                    rec.backend_id,
                    rtc.room_id,
                    COALESCE(rl.taken, 0) AS taken
                FROM recording AS rec
                INNER JOIN rtc
                ON rtc.id = rec.rtc_id
                LEFT JOIN active_room AS ar
                ON ar.id = rtc.room_id
                LEFT JOIN room_load AS rl
                ON rl.room_id = rtc.room_id
                WHERE rec.status = 'in_progress'
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
            WHERE a.status = 'connected'
            GROUP BY r.id
        ),
        active_room AS (
            SELECT *
            FROM room
            WHERE backend = 'janus'
            AND   UPPER(time) BETWEEN NOW() AND NOW() + INTERVAL '1 day'
        ),
        janus_backend_load AS (
            SELECT
                backend_id,
                SUM(taken) AS total_taken,
                SUM(reserve) AS total_reserve,
                SUM(GREATEST(taken, reserve)) AS load
            FROM (
                SELECT DISTINCT ON(backend_id, room_id)
                    rec.backend_id,
                    rtc.room_id,
                    COALESCE(rl.taken, 0)   AS taken,
                    COALESCE(ar.reserve, 0) AS reserve
                FROM recording AS rec
                INNER JOIN rtc
                ON rtc.id = rec.rtc_id
                LEFT JOIN active_room AS ar
                ON ar.id = rtc.room_id
                LEFT JOIN room_load AS rl
                ON rl.room_id = rtc.room_id
                WHERE rec.status = 'in_progress'
            ) AS sub
            GROUP BY backend_id
        )
    SELECT
        (
            CASE
                WHEN COALESCE(jb.capacity, 2147483647) <= COALESCE(jbl.total_taken, 0) THEN 0
                ELSE (
                    CASE
                        WHEN COALESCE(ar.reserve, 0) > COALESCE(rl.taken, 0)
                            THEN LEAST(
                                COALESCE(ar.reserve, 0) - COALESCE(rl.taken, 0),
                                COALESCE(jb.capacity, 2147483647) - COALESCE(jbl.total_taken, 0)
                            )
                        ELSE
                            GREATEST(COALESCE(jb.capacity, 2147483647) - COALESCE(jbl.load, 0), 0)
                    END
                )
            END
        )::INT AS free_capacity
    FROM rtc
    LEFT JOIN active_room AS ar
    ON ar.id = rtc.room_id
    LEFT JOIN room_load as rl
    ON rl.room_id = rtc.room_id
    LEFT JOIN recording AS rec
    ON  rec.rtc_id = rtc.id
    AND rec.status = 'in_progress'
    LEFT JOIN janus_backend AS jb
    ON jb.id = rec.backend_id
    LEFT JOIN janus_backend_load AS jbl
    ON jbl.backend_id = jb.id
    WHERE rtc.id = $1
"#;

#[derive(QueryableByName)]
struct FreeCapacityQueryRow {
    #[sql_type = "diesel::sql_types::Integer"]
    free_capacity: i32,
}

pub(crate) fn free_capacity(rtc_id: Uuid, conn: &PgConnection) -> Result<i32, Error> {
    use diesel::prelude::*;
    use diesel::sql_types::Uuid;

    diesel::sql_query(FREE_CAPACITY_SQL)
        .bind::<Uuid, _>(rtc_id)
        .get_result::<FreeCapacityQueryRow>(conn)
        .map(|row| row.free_capacity)
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn count(conn: &PgConnection) -> Result<i64, Error> {
    use diesel::dsl::sum;
    use diesel::prelude::*;

    janus_backend::table
        .select(sum(janus_backend::capacity))
        .get_result::<Option<i64>>(conn)
        .map(|v| v.unwrap_or(0))
}

pub(crate) fn total_capacity(conn: &PgConnection) -> Result<i64, Error> {
    use diesel::dsl::count;
    use diesel::prelude::*;

    janus_backend::table
        .select(count(janus_backend::id))
        .get_result(conn)
}
