use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::db::janus_backend::Object as JanusBackend;
use crate::schema::janus_backend_handle;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(JanusBackend, foreign_key = "backend_id")]
#[table_name = "janus_backend_handle"]
pub(crate) struct Object {
    id: Uuid,
    backend_id: AgentId,
    handle_id: i64,
    created_at: DateTime<Utc>,
}

impl Object {
    pub(crate) fn handle_id(&self) -> i64 {
        self.handle_id
    }
}

////////////////////////////////////////////////////////////////////////////////

const FIND_FREE_SQL: &str = r#"
SELECT *
FROM janus_backend_handle
WHERE id = (
    SELECT jbh.id
    FROM janus_backend_handle AS jbh
    LEFT JOIN agent_connection AS ac
    ON ac.janus_backend_handle_id = jbh.id
    WHERE ac.agent_id IS NULL
    AND jbh.backend_id = $1
    LIMIT 1
)
FOR UPDATE SKIP LOCKED
"#;

pub(crate) struct FindFreeQuery<'a> {
    backend_id: &'a AgentId,
}

impl<'a> FindFreeQuery<'a> {
    pub(crate) fn new(backend_id: &'a AgentId) -> Self {
        Self { backend_id }
    }

    pub(crate) fn execute(self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use crate::db::sql::Agent_id;
        use diesel::prelude::*;

        diesel::sql_query(FIND_FREE_SQL)
            .bind::<Agent_id, _>(self.backend_id)
            .get_result(conn)
            .optional()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct FindQuery<'a> {
    backend_id: &'a AgentId,
    handle_id: i64,
}

#[cfg(test)]
impl<'a> FindQuery<'a> {
    pub(crate) fn new(backend_id: &'a AgentId, handle_id: i64) -> Self {
        Self {
            backend_id,
            handle_id,
        }
    }

    pub(crate) fn execute(self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        janus_backend_handle::table
            .filter(janus_backend_handle::backend_id.eq(self.backend_id))
            .filter(janus_backend_handle::handle_id.eq(self.handle_id))
            .get_result(conn)
            .optional()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct BulkInsertQuery<'a> {
    backend_id: &'a AgentId,
    handle_ids: &'a [i64],
}

impl<'a> BulkInsertQuery<'a> {
    pub(crate) fn new(backend_id: &'a AgentId, handle_ids: &'a [i64]) -> Self {
        Self {
            backend_id,
            handle_ids,
        }
    }

    pub(crate) fn execute(self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::prelude::*;

        let values = self
            .handle_ids
            .iter()
            .map(|handle_id| {
                (
                    janus_backend_handle::backend_id.eq(self.backend_id),
                    janus_backend_handle::handle_id.eq(handle_id),
                )
            })
            .collect::<Vec<_>>();

        diesel::insert_into(janus_backend_handle::table)
            .values(values)
            .get_results(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct DeleteQuery<'a> {
    backend_id: &'a AgentId,
    handle_id: i64,
}

impl<'a> DeleteQuery<'a> {
    pub(crate) fn new(backend_id: &'a AgentId, handle_id: i64) -> Self {
        Self {
            backend_id,
            handle_id,
        }
    }

    pub(crate) fn execute(self, conn: &PgConnection) -> Result<usize, Error> {
        use diesel::prelude::*;

        diesel::delete(janus_backend_handle::table)
            .filter(janus_backend_handle::backend_id.eq(self.backend_id))
            .filter(janus_backend_handle::handle_id.eq(self.handle_id))
            .execute(conn)
    }
}
