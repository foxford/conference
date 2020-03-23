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
        Self {
            id: Some(id),
            ..self
        }
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
pub(crate) struct UpdateQuery<'a> {
    id: &'a AgentId,
    handle_id: i64,
    session_id: i64,
}

impl<'a> UpdateQuery<'a> {
    pub(crate) fn new(id: &'a AgentId, handle_id: i64, session_id: i64) -> Self {
        Self {
            id,
            handle_id,
            session_id,
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

const LEAST_LOADED_SQL: &str = r#"
    select jb.*
    from janus_backend as jb
    left join (
        select *
        from janus_rtc_stream
        where lower(time) is not null
        and   upper(time) is null
    ) as jrs
    on jrs.backend_id = jb.id
    group by jb.id
    order by count(jrs.id)
"#;

pub(crate) fn least_loaded(conn: &PgConnection) -> Result<Option<Object>, Error> {
    use diesel::prelude::*;

    diesel::sql_query(LEAST_LOADED_SQL)
        .get_result(conn)
        .optional()
}
