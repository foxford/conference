use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::Serialize;
use std::ops::Bound;
use uuid::Uuid;

use crate::schema::{room, rtc};

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

    pub(crate) fn time(&self) -> (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>) {
        self.time
    }

    pub(crate) fn started_at(&self) -> Option<DateTime<Utc>> {
        use std::ops::Bound;

        let (started_at, _finished_at) = self.time();
        match started_at {
            Bound::Excluded(dt) | Bound::Included(dt) => Some(dt),
            Bound::Unbounded => None,
        }
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
    finished: Option<bool>,
    with_recordings: Option<bool>,
}

impl ListQuery {
    pub(crate) fn new() -> Self {
        Self {
            finished: None,
            with_recordings: None,
        }
    }

    pub(crate) fn finished(mut self, finished: bool) -> Self {
        self.finished = Some(finished);
        self
    }

    pub(crate) fn with_recordings(mut self, with_recordings: bool) -> Self {
        self.with_recordings = Some(with_recordings);
        self
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use crate::schema;
        use diesel::{dsl::sql, prelude::*};

        let mut q = room::table.select(ALL_COLUMNS).into_boxed();

        if let Some(finished) = self.finished {
            let predicate = if finished {
                sql("upper(\"room\".\"time\") < now()")
            } else {
                sql("\"room\".\"time\" @> now()")
            };
            q = q.filter(predicate);
        }

        match self.with_recordings {
            Some(true) => {
                let q = q.inner_join(schema::rtc::table.inner_join(schema::recording::table));
                return q.load(conn);
            }
            Some(false) => {
                let q = q
                    .left_join(schema::rtc::table.inner_join(schema::recording::table))
                    .filter(schema::rtc::id.is_null());
                return q.load(conn);
            }
            None => {}
        }

        q.load(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "room"]
pub(crate) struct InsertQuery<'a> {
    id: Option<Uuid>,
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

    pub(crate) fn id(self, id: Uuid) -> Self {
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
