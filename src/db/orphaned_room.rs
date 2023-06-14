use chrono::{serde::ts_seconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use svc_agent::AgentId;

use super::room::sqlx_to_uuid;
use super::room::Object as Room;
use super::Ids;

#[derive(Debug, Serialize, Deserialize)]
pub struct Object {
    pub id: super::room::Id,
    #[serde(with = "ts_seconds")]
    pub host_left_at: DateTime<Utc>,
}

pub async fn upsert_room(
    id: super::room::Id,
    host_left_at: DateTime<Utc>,
    connection: &mut sqlx::PgConnection,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO orphaned_room
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE
        SET
            host_left_at = $2
        "#,
        id as super::room::Id,
        host_left_at,
    )
    .execute(connection)
    .await?;

    Ok(())
}

struct TimedOutRow {
    room_id: super::room::Id,
    host_left_at: DateTime<Utc>,
    backend_id: Option<AgentId>,
    time: super::room::TimeSqlx,
    reserve: Option<i32>,
    tags: serde_json::Value,
    classroom_id: Option<sqlx::types::Uuid>,
    host: Option<AgentId>,
    timed_out: bool,
    audience: String,
    created_at: DateTime<Utc>,
    backend: super::room::RoomBackend,
    rtc_sharing_policy: super::rtc::SharingPolicy,
    infinite: bool,
    closed_by: Option<AgentId>,
}

impl TimedOutRow {
    fn split(self) -> (Object, Option<Room>) {
        (
            Object {
                id: self.room_id,
                host_left_at: self.host_left_at,
            },
            self.classroom_id.map(|classroom_id| Room {
                id: self.room_id,
                time: self.time.into(),
                audience: self.audience,
                created_at: self.created_at,
                backend: self.backend,
                reserve: self.reserve,
                tags: self.tags,
                backend_id: self.backend_id,
                rtc_sharing_policy: self.rtc_sharing_policy,
                classroom_id: sqlx_to_uuid(classroom_id),
                host: self.host,
                timed_out: self.timed_out,
                closed_by: self.closed_by,
                infinite: self.infinite,
            }),
        )
    }
}

pub async fn get_timed_out(
    load_till: DateTime<Utc>,
    connection: &mut sqlx::PgConnection,
) -> sqlx::Result<Vec<(Object, Option<super::room::Object>)>> {
    sqlx::query_as!(
        TimedOutRow,
        r#"
        SELECT
            orph.id as "room_id: super::room::Id",
            orph.host_left_at,
            r.backend_id as "backend_id: AgentId",
            r.time as "time: super::room::TimeSqlx",
            r.reserve,
            r.tags,
            r.classroom_id as "classroom_id?: _",
            r.host as "host: AgentId",
            r.timed_out,
            r.audience,
            r.created_at,
            r.backend as "backend: super::room::RoomBackend",
            r.rtc_sharing_policy as "rtc_sharing_policy: super::rtc::SharingPolicy",
            r.infinite,
            r.closed_by as "closed_by: AgentId"
        FROM orphaned_room as orph
        LEFT JOIN room as r
        ON r.id = orph.id
        WHERE
            orph.host_left_at < $1
        "#,
        load_till
    )
    .fetch_all(connection)
    .await
    .map(|r| r.into_iter().map(|o| o.split()).collect())
}

pub async fn remove_rooms(
    ids: &[super::room::Id],
    connection: &mut sqlx::PgConnection,
) -> sqlx::Result<()> {
    let ids = Ids(ids);

    sqlx::query!(
        r#"
        DELETE FROM orphaned_room
        WHERE
            id = ANY($1)
        "#,
        ids as Ids,
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn remove_room(
    id: super::room::Id,
    connection: &mut sqlx::PgConnection,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        DELETE FROM orphaned_room
        WHERE
            id = $1
        "#,
        id as super::room::Id,
    )
    .execute(connection)
    .await?;

    Ok(())
}
