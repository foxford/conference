use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::Serialize;
use std::ops::Bound;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::schema::{janus_rtc_stream, rtc};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type Time = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

////////////////////////////////////////////////////////////////////////////////

type AllColumns = (
    janus_rtc_stream::id,
    janus_rtc_stream::handle_id,
    janus_rtc_stream::rtc_id,
    janus_rtc_stream::backend_id,
    janus_rtc_stream::label,
    janus_rtc_stream::sent_by,
    janus_rtc_stream::time,
    janus_rtc_stream::created_at,
);
const ALL_COLUMNS: AllColumns = (
    janus_rtc_stream::id,
    janus_rtc_stream::handle_id,
    janus_rtc_stream::rtc_id,
    janus_rtc_stream::backend_id,
    janus_rtc_stream::label,
    janus_rtc_stream::sent_by,
    janus_rtc_stream::time,
    janus_rtc_stream::created_at,
);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Identifiable, Queryable, QueryableByName, Associations)]
#[table_name = "janus_rtc_stream"]
pub(crate) struct Object {
    id: Uuid,
    handle_id: i64,
    rtc_id: Uuid,
    backend_id: AgentId,
    label: String,
    sent_by: AgentId,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<Time>,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
}

impl Object {
    pub(crate) fn handle_id(&self) -> i64 {
        self.handle_id
    }

    pub(crate) fn rtc_id(&self) -> Uuid {
        self.rtc_id
    }

    pub(crate) fn backend_id(&self) -> &AgentId {
        &self.backend_id
    }

    pub(crate) fn label(&self) -> &str {
        self.label.as_ref()
    }

    pub(crate) fn sent_by(&self) -> &AgentId {
        &self.sent_by
    }

    pub(crate) fn time(&self) -> Option<Time> {
        self.time
    }

    pub(crate) fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

////////////////////////////////////////////////////////////////////////////////

const ACTIVE_SQL: &str = r#"(
    lower("janus_rtc_stream"."time") is not null
    and upper("janus_rtc_stream"."time") is null
)"#;

pub(crate) struct FindQuery {
    id: Option<Uuid>,
    rtc_id: Option<Uuid>,
    active: Option<bool>,
}

impl FindQuery {
    pub(crate) fn new() -> Self {
        Self {
            id: None,
            rtc_id: None,
            active: None,
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

    pub(crate) fn active(self, active: bool) -> Self {
        Self {
            active: Some(active),
            ..self
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::dsl::sql;
        use diesel::prelude::*;

        let query = match (self.id, self.rtc_id) {
            (Some(ref id), None) => janus_rtc_stream::table.find(id.to_owned()).into_boxed(),
            (None, Some(ref rtc_id)) => janus_rtc_stream::table
                .filter(janus_rtc_stream::rtc_id.eq(rtc_id.to_owned()))
                .into_boxed(),
            _ => {
                return Err(Error::QueryBuilderError(
                    "id either rtc_id is required parameter of the query".into(),
                ))
            }
        };

        let query = match self.active {
            Some(true) => query.filter(sql(ACTIVE_SQL)),
            Some(false) => query.filter(sql(&format!("not {}", ACTIVE_SQL))),
            None => query,
        };

        query.get_result(conn).optional()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ListQuery {
    room_id: Option<Uuid>,
    rtc_id: Option<Uuid>,
    time: Option<Time>,
    active: Option<bool>,
    offset: Option<i64>,
    limit: Option<i64>,
}

impl ListQuery {
    pub(crate) fn new() -> Self {
        Self {
            room_id: None,
            rtc_id: None,
            time: None,
            active: None,
            offset: None,
            limit: None,
        }
    }

    pub(crate) fn room_id(self, room_id: Uuid) -> Self {
        Self {
            room_id: Some(room_id),
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

    pub(crate) fn active(self, active: bool) -> Self {
        Self {
            active: Some(active),
            ..self
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
        use diesel::{dsl::sql, sql_types::Tstzrange};

        let mut q = janus_rtc_stream::table.into_boxed();
        if let Some(rtc_id) = self.rtc_id {
            q = q.filter(janus_rtc_stream::rtc_id.eq(rtc_id));
        }
        if let Some(time) = self.time {
            q = q.filter(sql("time && ").bind::<Tstzrange, _>(time));
        }
        match self.active {
            None => (),
            Some(true) => q = q.filter(sql(ACTIVE_SQL)),
            Some(false) => q = q.filter(sql(&format!("not {}", ACTIVE_SQL))),
        }
        if let Some(offset) = self.offset {
            q = q.offset(offset);
        }
        if let Some(limit) = self.limit {
            q = q.limit(limit);
        }

        if let Some(room_id) = self.room_id {
            return q
                .inner_join(rtc::table)
                .filter(rtc::room_id.eq(room_id))
                .select(ALL_COLUMNS)
                .order_by(janus_rtc_stream::created_at.desc())
                .get_results(conn);
        }

        q.order_by(janus_rtc_stream::created_at.desc())
            .get_results(conn)
    }
}

impl
    From<(
        Option<Uuid>,
        Option<Uuid>,
        Option<Time>,
        Option<bool>,
        Option<i64>,
        Option<i64>,
    )> for ListQuery
{
    fn from(
        value: (
            Option<Uuid>,
            Option<Uuid>,
            Option<Time>,
            Option<bool>,
            Option<i64>,
            Option<i64>,
        ),
    ) -> Self {
        Self {
            room_id: value.0,
            rtc_id: value.1,
            time: value.2,
            active: value.3,
            offset: value.4,
            limit: value.5,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "janus_rtc_stream"]
pub(crate) struct InsertQuery<'a> {
    id: Uuid,
    handle_id: i64,
    rtc_id: Uuid,
    backend_id: &'a AgentId,
    label: &'a str,
    sent_by: &'a AgentId,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(
        id: Uuid,
        handle_id: i64,
        rtc_id: Uuid,
        backend_id: &'a AgentId,
        label: &'a str,
        sent_by: &'a AgentId,
    ) -> Self {
        Self {
            id,
            rtc_id,
            backend_id,
            handle_id,
            label,
            sent_by,
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::janus_rtc_stream::dsl::janus_rtc_stream;
        use diesel::RunQueryDsl;

        diesel::insert_into(janus_rtc_stream)
            .values(self)
            .get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn start(id: Uuid, conn: &PgConnection) -> Result<Option<Object>, Error> {
    use diesel::prelude::*;
    use diesel::sql_types::Uuid;

    diesel::sql_query(
        "\
         update janus_rtc_stream \
         set time = tstzrange(now(), null, '[)') \
         where id = $1 \
         returning *\
         ",
    )
    .bind::<Uuid, _>(id)
    .get_result(conn)
    .optional()
}

pub(crate) fn stop(id: Uuid, conn: &PgConnection) -> Result<Option<Object>, Error> {
    use diesel::prelude::*;
    use diesel::sql_types::Uuid;

    diesel::sql_query(
        "\
         update janus_rtc_stream \
         set time = case when time is not null then tstzrange(lower(time), now(), '[)') end \
         where id = $1 \
         returning *\
         ",
    )
    .bind::<Uuid, _>(id)
    .get_result(conn)
    .optional()
}

pub(crate) fn stop_by_agent_id(agent_id: &AgentId, conn: &PgConnection) -> Result<usize, Error> {
    use diesel::prelude::*;
    use svc_agent::sql::Agent_id;

    diesel::sql_query(
        "\
         update janus_rtc_stream \
         set time = case when time is not null then tstzrange(lower(time), now(), '[)') end \
         where sent_by = $1 \
         ",
    )
    .bind::<Agent_id, _>(agent_id)
    .execute(conn)
}
