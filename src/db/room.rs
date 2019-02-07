use crate::schema::room;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::Serialize;
use std::ops::Bound;
use uuid::Uuid;

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

    pub(crate) fn bucket_name(&self) -> String {
        format!("origin.webinar.{}", self.audience())
    }
}

/////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct FindQuery<'a> {
    id: Option<&'a Uuid>,
    finished: Option<bool>,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new() -> Self {
        Self {
            id: None,
            finished: None,
        }
    }

    pub(crate) fn id(mut self, id: &'a Uuid) -> Self {
        self.id = Some(id);
        self
    }

    pub(crate) fn finished(mut self, finished: bool) -> Self {
        self.finished = Some(finished);
        self
    }

    pub(crate) fn one(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        match self.id {
            Some(id) => room::table.find(id).get_result(conn).optional(),
            _ => Err(Error::QueryBuilderError(
                "rtc_id or session_id and location_id are required parameters of the query".into(),
            )),
        }
    }

    pub(crate) fn many(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::prelude::*;

        match self.finished {
            Some(finished) => {
                let q = if finished {
                    "select room where upper(time) < now()"
                } else {
                    "select room where time @> now()"
                };
                diesel::sql_query(q).load(conn)
            }
            _ => Err(Error::QueryBuilderError(
                "finished is required parameter of the query".into(),
            )),
        }
    }
}
