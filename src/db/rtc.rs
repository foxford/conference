use crate::schema::{room, rtc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, Associations)]
#[belongs_to(room::Record, foreign_key = "room_id")]
#[table_name = "rtc"]
pub(crate) struct Record {
    id: Uuid,
    room_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    jsep: Option<JsonValue>,
}

impl Record {
    pub(crate) fn id(&self) -> &Uuid {
        &self.id
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, AsChangeset)]
#[table_name = "rtc"]
pub(crate) struct UpdateQuery<'a> {
    id: &'a Uuid,
    jsep: Option<&'a JsonValue>,
}

impl<'a> UpdateQuery<'a> {
    pub(crate) fn new(id: &'a Uuid) -> Self {
        Self { id, jsep: None }
    }

    pub(crate) fn jsep(self, jsep: &'a JsonValue) -> Self {
        Self {
            id: self.id,
            jsep: Some(jsep),
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Record, Error> {
        use diesel::prelude::*;

        diesel::update(rtc::table).set(self).get_result(conn)
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
