use crate::schema::room;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::Serialize;
use std::ops::Range;
use uuid::Uuid;

#[derive(Debug, Identifiable, Queryable, Serialize)]
#[table_name = "room"]
pub(crate) struct Object {
    id: Uuid,
    time: Range<DateTime<Utc>>,
    audience: String,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[table_name = "room"]
pub(crate) struct InsertQuery<'a> {
    id: Option<&'a Uuid>,
    time: Range<&'a DateTime<Utc>>,
    audience: &'a str,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(time: Range<&'a DateTime<Utc>>, audience: &'a str) -> Self {
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
