use crate::schema::{room, rtc};
use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, Associations)]
#[belongs_to(room::Record, foreign_key = "room_id")]
#[table_name = "rtc"]
pub(crate) struct Record {
    id: Uuid,
    room_id: Uuid,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
}

impl Record {
    pub(crate) fn id(&self) -> &Uuid {
        &self.id
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FindQuery<'a> {
    id: &'a Uuid,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new(id: &'a Uuid) -> Self {
        Self { id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Record, Error> {
        use diesel::prelude::*;

        rtc::table.find(self.id).get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ListQuery<'a> {
    room_id: Option<&'a Uuid>,
}

impl<'a> ListQuery<'a> {
    pub(crate) fn new() -> Self {
        Self { room_id: None }
    }

    pub(crate) fn from_options(room_id: Option<&'a Uuid>) -> Self {
        Self { room_id: room_id }
    }

    pub(crate) fn room_id(self, room_id: &'a Uuid) -> Self {
        Self {
            room_id: Some(room_id),
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Vec<Record>, Error> {
        use diesel::prelude::*;

        let mut q = rtc::table.into_boxed();
        if let Some(room_id) = self.room_id {
            q = q.filter(rtc::room_id.eq(room_id));
        }
        q.get_results(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "rtc"]
pub(crate) struct InsertQuery<'a> {
    id: Option<&'a Uuid>,
    room_id: &'a Uuid,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(room_id: &'a Uuid) -> Self {
        Self { id: None, room_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Record, Error> {
        use crate::schema::rtc::dsl::rtc;
        use diesel::RunQueryDsl;

        diesel::insert_into(rtc).values(self).get_result(conn)
    }
}
