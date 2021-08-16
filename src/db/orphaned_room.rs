use super::room::Object as Room;
use crate::diesel::ExpressionMethods;
use crate::diesel::RunQueryDsl;
use crate::schema::orphaned_room;
use chrono::{serde::ts_seconds, DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Room, foreign_key = "id")]
#[table_name = "orphaned_room"]
pub struct Object {
    pub id: super::room::Id,
    #[serde(with = "ts_seconds")]
    pub host_left_time: DateTime<Utc>,
}

pub fn upsert_room(
    id: super::room::Id,
    left_time: DateTime<Utc>,
    connection: &PgConnection,
) -> Result<(), Error> {
    let record = (
        orphaned_room::dsl::id.eq(id),
        orphaned_room::dsl::host_left_time.eq(left_time),
    );
    diesel::insert_into(orphaned_room::table)
        .values(&record)
        .on_conflict(orphaned_room::id)
        .do_update()
        .set(record)
        .execute(connection)?;
    Ok(())
}

pub fn remove_room(id: super::room::Id, connection: &PgConnection) -> Result<(), Error> {
    diesel::delete(orphaned_room::table)
        .filter(orphaned_room::dsl::id.eq(id))
        .execute(connection)?;
    Ok(())
}
