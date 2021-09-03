use super::room::Object as Room;
use crate::diesel::RunQueryDsl;
use crate::schema::orphaned_room;
use crate::{diesel::ExpressionMethods, schema};
use chrono::{serde::ts_seconds, DateTime, Utc};
use diesel::{dsl::any, pg::PgConnection, result::Error, QueryDsl};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Room, foreign_key = "id")]
#[table_name = "orphaned_room"]
pub struct Object {
    pub id: super::room::Id,
    #[serde(with = "ts_seconds")]
    pub host_left_at: DateTime<Utc>,
}

pub fn upsert_room(
    id: super::room::Id,
    host_left_at: DateTime<Utc>,
    connection: &PgConnection,
) -> Result<(), Error> {
    let record = (
        orphaned_room::id.eq(id),
        orphaned_room::host_left_at.eq(host_left_at),
    );
    diesel::insert_into(orphaned_room::table)
        .values(&record)
        .on_conflict(orphaned_room::id)
        .do_update()
        .set(record)
        .execute(connection)?;
    Ok(())
}

pub fn get_timed_out(
    load_till: DateTime<Utc>,
    connection: &PgConnection,
) -> Result<Vec<(Object, Option<super::room::Object>)>, Error> {
    orphaned_room::table
        .filter(orphaned_room::host_left_at.lt(load_till))
        .left_join(schema::room::table)
        .get_results(connection)
}

pub fn remove_rooms(ids: &[super::room::Id], connection: &PgConnection) -> Result<(), Error> {
    diesel::delete(orphaned_room::table)
        .filter(orphaned_room::id.eq(any(ids)))
        .execute(connection)?;
    Ok(())
}

pub fn remove_room(id: super::room::Id, connection: &PgConnection) -> Result<(), Error> {
    diesel::delete(orphaned_room::table)
        .filter(orphaned_room::id.eq(id))
        .execute(connection)?;
    Ok(())
}
