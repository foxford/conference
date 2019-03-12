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
    handle_id: i64,
    session_id: i64,
    created_at: DateTime<Utc>,
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

pub(crate) struct ListQuery {
    offset: Option<i64>,
    limit: Option<i64>,
}

impl ListQuery {
    pub(crate) fn new() -> Self {
        Self {
            offset: None,
            limit: None,
        }
    }

    pub(crate) fn offset(self, offset: i64) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    pub(crate) fn limit(self, limit: i64) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::prelude::*;

        let mut q = janus_backend::table.into_boxed();
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

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "janus_backend"]
pub(crate) struct InsertQuery<'a> {
    id: &'a AgentId,
    handle_id: i64,
    session_id: i64,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(id: &'a AgentId, handle_id: i64, session_id: i64) -> Self {
        Self { id, handle_id, session_id }
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
