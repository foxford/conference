use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::{Deserialize, Serialize};
use std::ops::Bound;
use uuid::Uuid;

use crate::schema::{room, rtc};

////////////////////////////////////////////////////////////////////////////////

type AllColumns = (room::id, room::time, room::audience, room::created_at);
const ALL_COLUMNS: AllColumns = (room::id, room::time, room::audience, room::created_at);

////////////////////////////////////////////////////////////////////////////////

pub(crate) type Time = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Identifiable, Queryable, QueryableByName)]
#[table_name = "room"]
pub(crate) struct Object {
    id: Uuid,
    #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
    time: Time,
    audience: String,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
}

impl Object {
    pub(crate) fn audience(&self) -> &str {
        &self.audience
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn time(&self) -> Time {
        self.time
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

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        match (self.id, self.rtc_id) {
            (Some(id), None) => room::table.find(id).get_result(conn).optional(),
            (None, Some(rtc_id)) => room::table
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
}

impl ListQuery {
    pub(crate) fn new() -> Self {
        Self { finished: None }
    }

    pub(crate) fn finished(self, finished: bool) -> Self {
        Self {
            finished: Some(finished),
            ..self
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::{dsl::sql, prelude::*};

        let mut q = room::table.into_boxed();

        if let Some(finished) = self.finished {
            let predicate = if finished {
                sql("upper(\"room\".\"time\") < now()")
            } else {
                sql("\"room\".\"time\" @> now()")
            };
            q = q.filter(predicate);
        }

        q.load(conn)
    }
}

// Filtering out rooms with every recording ready using left and inner joins
// and condition that recording.rtc_id is null. In diagram below room1
// and room3 will be selected (room1 - there's one recording that is not
// ready, room3 - there's no ready recordings at all).
// room1 | rtc1 | null           room1 | null | null
// room1 | rtc2 | recording1  -> room1 | rtc2 | recording1
// room2 | rtc3 | recording2     room2 | rtc3 | recording2
// room3 | rtc4 | null           room3 | null | null
pub(crate) fn finished_without_recordings(
    conn: &PgConnection,
) -> Result<Vec<(self::Object, super::rtc::Object)>, Error> {
    use crate::schema;
    use diesel::{dsl::sql, prelude::*};

    schema::room::table
        .inner_join(schema::rtc::table.left_join(schema::recording::table))
        .filter(schema::recording::rtc_id.is_null())
        .filter(sql("upper(\"room\".\"time\") < now()"))
        .select((self::ALL_COLUMNS, super::rtc::ALL_COLUMNS))
        .load(conn)
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "room"]
pub(crate) struct InsertQuery<'a> {
    id: Option<Uuid>,
    time: Time,
    audience: &'a str,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(time: Time, audience: &'a str) -> Self {
        Self {
            id: None,
            time,
            audience,
        }
    }

    pub(crate) fn id(self, id: Uuid) -> Self {
        Self {
            id: Some(id),
            ..self
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::room::dsl::room;
        use diesel::RunQueryDsl;

        diesel::insert_into(room).values(self).get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct DeleteQuery {
    id: Uuid,
}

impl DeleteQuery {
    pub(crate) fn new(id: Uuid) -> Self {
        Self { id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use diesel::prelude::*;

        diesel::delete(room::table.filter(room::id.eq(self.id))).execute(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, AsChangeset, Deserialize)]
#[table_name = "room"]
pub(crate) struct UpdateQuery {
    id: Uuid,
    #[serde(default)]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<Time>,
    audience: Option<String>,
}

impl UpdateQuery {
    pub(crate) fn new(id: Uuid) -> Self {
        Self {
            id,
            time: None,
            audience: None,
        }
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        diesel::update(self).set(self).get_result(conn)
    }
}
