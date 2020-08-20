use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::db::agent::Status as AgentStatus;
use crate::schema::{agent, janus_backend, janus_rtc_stream, rtc};

type AllColumns = (
    janus_backend::id,
    janus_backend::handle_id,
    janus_backend::session_id,
    janus_backend::created_at,
    janus_backend::capacity,
);
pub const ALL_COLUMNS: AllColumns = (
    janus_backend::id,
    janus_backend::handle_id,
    janus_backend::session_id,
    janus_backend::created_at,
    janus_backend::capacity,
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

    pub(crate) fn capacity(&self) -> Option<i32> {
        self.capacity
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
}

impl<'a> UpsertQuery<'a> {
    pub(crate) fn new(id: &'a AgentId, handle_id: i64, session_id: i64) -> Self {
        Self {
            id,
            handle_id,
            session_id,
            capacity: None,
        }
    }

    pub(crate) fn capacity(self, capacity: i32) -> Self {
        Self {
            capacity: Some(capacity),
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

// Returns the least loaded backend capable to host the room with its reserve considering:
// - actual number of online agents;
// - optional backend subscribers limit;
// - optional reserved capacity.
const LEAST_LOADED_SQL: &str = r#"
    WITH
        room_load AS (
            SELECT
                r.id AS room_id,
                GREATEST(COALESCE(r.reserve, 0), COUNT(a.id)) AS taken
            FROM room AS r
            LEFT JOIN agent AS a
            ON a.room_id = r.id
            WHERE a.status = 'connected'
            GROUP BY r.id
        ),
        janus_backend_load AS (
            SELECT
                jrs.backend_id,
                SUM(taken) AS taken
            FROM room_load as rl
            LEFT JOIN rtc
            ON rtc.room_id = rl.room_id
            LEFT JOIN janus_rtc_stream AS jrs
            ON jrs.rtc_id = rtc.id
            WHERE LOWER(jrs.time) IS NOT NULL AND UPPER(jrs.time) IS NULL
            GROUP BY jrs.backend_id
        )
    SELECT jb.*
    FROM janus_backend AS jb
    LEFT JOIN janus_backend_load AS jbl
    ON jbl.backend_id = jb.id
    LEFT JOIN room AS r2
    ON 1 = 1
    WHERE r2.id = $1
    AND   COALESCE(jb.capacity, 2147483647) - COALESCE(jbl.taken, 0) > COALESCE(r2.reserve, 0)
    ORDER BY COALESCE(jb.capacity, 2147483647) - COALESCE(jbl.taken, 0) DESC
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

pub(crate) fn agents_count(backend_id: &AgentId, conn: &PgConnection) -> Result<i64, Error> {
    use diesel::dsl::{count, sql};
    use diesel::prelude::*;

    agent::table
        .inner_join(rtc::table.on(rtc::room_id.eq(agent::room_id)))
        .inner_join(janus_rtc_stream::table.on(janus_rtc_stream::rtc_id.eq(rtc::id)))
        .filter(janus_rtc_stream::backend_id.eq(backend_id))
        .filter(sql("LOWER(\"janus_rtc_stream\".\"time\") IS NOT NULL AND UPPER(\"janus_rtc_stream\".\"time\") IS NULL"))
        .filter(agent::status.eq(AgentStatus::Connected))
        .select(count(agent::id))
        .get_result(conn)
}
