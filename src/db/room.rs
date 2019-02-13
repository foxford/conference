use crate::schema::{room, rtc};
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::Serialize;
use std::ops::Bound;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

type AllColumns = (room::id, room::time, room::audience, room::created_at);
const ALL_COLUMNS: AllColumns = (room::id, room::time, room::audience, room::created_at);

#[derive(Debug, Identifiable, Queryable, Serialize, QueryableByName)]
#[table_name = "room"]
pub(crate) struct Object {
    id: Uuid,
    #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
    time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    audience: String,
    created_at: DateTime<Utc>,
}

impl Object {
    pub(crate) fn audience(&self) -> &str {
        &self.audience
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FindQuery {
    id: Option<Uuid>,
    rtc_id: Option<Uuid>,
}

impl FindQuery {
    pub(crate) fn new() -> Self {
        Self {
            id: None,
            rtc_id: None,
        }
    }

    pub(crate) fn id(mut self, id: Uuid) -> Self {
        self.id = Some(id);
        self
    }

    pub(crate) fn rtc_id(mut self, rtc_id: Uuid) -> Self {
        self.rtc_id = Some(rtc_id);
        self
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        match (self.id, self.rtc_id) {
            (Some(ref id), None) => room::table.find(id).get_result(conn).optional(),
            (None, Some(ref rtc_id)) => room::table
                .inner_join(rtc::table)
                .filter(rtc::id.eq(rtc_id))
                .select(ALL_COLUMNS)
                .get_result(conn)
                .optional(),
            _ => Err(Error::QueryBuilderError(
                "id or rtc_id are required parameters of the query".into(),
            )),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ListQuery {
    id: Option<Uuid>,
    rtc_id: Option<Uuid>,
    finished: Option<bool>,
}

impl ListQuery {
    pub(crate) fn new() -> Self {
        Self {
            id: None,
            rtc_id: None,
            finished: None,
        }
    }

    pub(crate) fn id(mut self, id: Uuid) -> Self {
        self.id = Some(id);
        self
    }

    pub(crate) fn rtc_id(mut self, rtc_id: Uuid) -> Self {
        self.rtc_id = Some(rtc_id);
        self
    }

    pub(crate) fn finished(mut self, finished: bool) -> Self {
        self.finished = Some(finished);
        self
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::{dsl::sql, prelude::*};

        match self.finished {
            Some(finished) => {
                let predicate = if finished {
                    sql("upper(time) < now()")
                } else {
                    sql("time @> now()")
                };
                room::table.filter(predicate).load(conn)
            }
            _ => Err(Error::QueryBuilderError(
                "finished is required parameter of the query".into(),
            )),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "room"]
pub(crate) struct InsertQuery<'a> {
    id: Option<&'a Uuid>,
    time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    audience: &'a str,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(
        time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
        audience: &'a str,
    ) -> Self {
        Self {
            id: None,
            time,
            audience,
        }
    }

    pub(crate) fn id(self, id: &'a Uuid) -> Self {
        Self {
            id: Some(id),
            time: self.time,
            audience: self.audience,
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::room::dsl::room;
        use diesel::RunQueryDsl;

        diesel::insert_into(room).values(self).get_result(conn)
    }
}
