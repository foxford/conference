use super::room::Object as Room;
use crate::db;
use crate::schema::group;
use diesel::{pg::PgConnection, result::Error};
use serde::{Deserialize, Serialize};

pub type Id = db::id::Id;

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Room, foreign_key = "room_id")]
#[table_name = "group"]
pub struct Object {
    id: Id,
    room_id: db::room::Id,
    number: i32,
}

#[derive(Debug, Insertable)]
#[table_name = "group"]
pub struct InsertQuery {
    room_id: db::room::Id,
    number: i32,
}

impl InsertQuery {
    pub fn new(room_id: db::room::Id) -> Self {
        Self { room_id, number: 0 }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::group::dsl::*;
        use diesel::RunQueryDsl;

        diesel::insert_into(group)
            .values(self)
            .on_conflict_do_nothing()
            .get_result(conn)
    }
}
