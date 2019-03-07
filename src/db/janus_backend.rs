use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use svc_agent::AgentId;

use crate::schema::janus_backend;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, QueryableByName, Associations)]
#[table_name = "janus_backend"]
pub(crate) struct Object {
    id: AgentId,
    session_id: i64,
    created_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "janus_backend"]
pub(crate) struct InsertQuery<'a> {
    id: &'a AgentId,
    session_id: i64,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(id: &'a AgentId, session_id: i64) -> Self {
        Self { id, session_id }
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
