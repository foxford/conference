use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::db::janus_backend::Object as JanusBackend;
use crate::schema::{agent_connection, janus_backend_handle};

type AllColumns = (
    janus_backend_handle::id,
    janus_backend_handle::backend_id,
    janus_backend_handle::handle_id,
    janus_backend_handle::created_at,
);

const ALL_COLUMNS: AllColumns = (
    janus_backend_handle::id,
    janus_backend_handle::backend_id,
    janus_backend_handle::handle_id,
    janus_backend_handle::created_at,
);

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

pub(crate) struct FindFreeQuery<'a> {
    backend_id: &'a AgentId,
}

impl<'a> FindFreeQuery<'a> {
    pub(crate) fn new(backend_id: &'a AgentId) -> Self {
        Self { backend_id }
    }

    pub(crate) fn execute(self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        janus_backend_handle::table
            .left_join(agent_connection::table)
            .filter(agent_connection::agent_id.is_null())
            .filter(janus_backend_handle::backend_id.eq(self.backend_id))
            .select(ALL_COLUMNS)
            .get_result(conn)
            .optional()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct FindQuery<'a> {
    backend_id: &'a AgentId,
    handle_id: i64,
}

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

    pub(crate) fn execute(self, conn: &PgConnection) -> Result<(), Error> {
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
            .execute(conn)?;

        Ok(())
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
