// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use chrono::{serde::ts_seconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::ops::Bound;
use svc_agent::AgentId;

use crate::{backend::janus::client::HandleId, db, schema::janus_rtc_stream};

use super::room::TimeSqlx;

////////////////////////////////////////////////////////////////////////////////

pub type Time = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

////////////////////////////////////////////////////////////////////////////////
pub type Id = db::id::Id;

#[derive(Debug, Deserialize, Serialize, Identifiable, Queryable, QueryableByName, Associations)]
#[table_name = "janus_rtc_stream"]
pub struct Object {
    id: Id,
    handle_id: HandleId,
    rtc_id: db::rtc::Id,
    backend_id: AgentId,
    label: String,
    sent_by: AgentId,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple_sqlx")]
    time: Option<TimeSqlx>,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
}

impl Object {
    pub fn id(&self) -> Id {
        self.id
    }

    #[cfg(test)]
    pub fn handle_id(&self) -> HandleId {
        self.handle_id
    }

    pub fn rtc_id(&self) -> db::rtc::Id {
        self.rtc_id
    }

    pub fn backend_id(&self) -> &AgentId {
        &self.backend_id
    }

    #[cfg(test)]
    pub fn label(&self) -> &str {
        self.label.as_ref()
    }

    pub fn sent_by(&self) -> &AgentId {
        &self.sent_by
    }

    pub fn time(&self) -> Option<Time> {
        self.time.as_ref().map(|t| Time::from(t.clone()))
    }

    #[cfg(test)]
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn set_time(&mut self, time: Option<Time>) -> &mut Self {
        self.time = time.map(TimeSqlx::from);
        self
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct ListQuery {
    room_id: Option<db::room::Id>,
    rtc_id: Option<db::rtc::Id>,
    time: Option<Time>,
    active: Option<bool>,
    offset: Option<i64>,
    limit: Option<i64>,
}

impl ListQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn room_id(self, room_id: db::room::Id) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
        }
    }

    pub fn rtc_id(self, rtc_id: db::rtc::Id) -> Self {
        Self {
            rtc_id: Some(rtc_id),
            ..self
        }
    }

    pub fn time(self, time: Time) -> Self {
        Self {
            time: Some(time),
            ..self
        }
    }

    pub fn offset(self, offset: i64) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    pub fn limit(self, limit: i64) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                janus_rtc_stream.id as "id: db::id::Id",
                janus_rtc_stream.handle_id as "handle_id: HandleId",
                janus_rtc_stream.rtc_id as "rtc_id: Id",
                janus_rtc_stream.backend_id as "backend_id: AgentId",
                janus_rtc_stream.created_at,
                janus_rtc_stream.label,
                janus_rtc_stream.sent_by as "sent_by: AgentId",
                janus_rtc_stream.time as "time: TimeSqlx"
            FROM janus_rtc_stream
            INNER JOIN rtc
            ON rtc.id = janus_rtc_stream.rtc_id
            WHERE
                ($1::uuid IS NULL OR rtc_id = $1::uuid) AND
                ($2::tstzrange IS NULL OR time && $2) AND
                (
                    $3::boolean IS NULL OR
                    -- if 'active' is set the right hand should be equal to TRUE
                    -- so we pick only active janus rtc streams
                    -- if 'active' is not set the right hand should be equal to FALSE
                    -- so we pick only non-active janus rtc streams
                    $3 = (
                        lower(janus_rtc_stream.time) is not null
                        and upper(janus_rtc_stream.time) is null
                    )
                ) AND
                ($4::uuid IS NULL OR rtc.room_id = $4::uuid)
            ORDER BY created_at DESC
            OFFSET $5
            LIMIT $6
            "#,
            self.rtc_id as Option<Id>,
            self.time.map(|t| TimeSqlx::from(t)) as Option<TimeSqlx>,
            self.active,
            self.room_id as Option<Id>,
            self.offset,
            self.limit,
        )
        .fetch_all(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "janus_rtc_stream"]
pub struct InsertQuery<'a> {
    id: Id,
    handle_id: HandleId,
    rtc_id: db::rtc::Id,
    backend_id: &'a AgentId,
    label: &'a str,
    sent_by: &'a AgentId,
}

impl<'a> InsertQuery<'a> {
    pub fn new(
        id: Id,
        handle_id: HandleId,
        rtc_id: db::rtc::Id,
        backend_id: &'a AgentId,
        label: &'a str,
        sent_by: &'a AgentId,
    ) -> Self {
        Self {
            id,
            handle_id,
            rtc_id,
            backend_id,
            label,
            sent_by,
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO janus_rtc_stream (id, handle_id, rtc_id, backend_id, label, sent_by)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING
                id as "id: db::id::Id",
                handle_id as "handle_id: HandleId",
                rtc_id as "rtc_id: Id",
                backend_id as "backend_id: AgentId",
                created_at,
                label,
                sent_by as "sent_by: AgentId",
                time as "time: TimeSqlx"
            "#,
            self.id as Id,
            self.handle_id as HandleId,
            self.rtc_id as Id,
            self.backend_id as &AgentId,
            self.label,
            self.sent_by as &AgentId
        )
        .fetch_one(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

pub async fn start(
    id: db::janus_rtc_stream::Id,
    conn: &mut sqlx::PgConnection,
) -> sqlx::Result<Option<Object>> {
    sqlx::query_as!(
        Object,
        r#"
        UPDATE janus_rtc_stream
        SET
            time = (TSTZRANGE(NOW(), NULL, '[)'))
        WHERE
            id = $1
        RETURNING
            id as "id: db::id::Id",
            handle_id as "handle_id: HandleId",
            rtc_id as "rtc_id: Id",
            backend_id as "backend_id: AgentId",
            created_at,
            label,
            sent_by as "sent_by: AgentId",
            time as "time: TimeSqlx"
        "#,
        id as Id,
    )
    .fetch_optional(conn)
    .await
}

pub async fn stop(
    id: db::janus_rtc_stream::Id,
    conn: &mut sqlx::PgConnection,
) -> sqlx::Result<Option<Object>> {
    sqlx::query_as!(
        Object,
        r#"
        UPDATE janus_rtc_stream
        SET
            -- Close the stream with current timestamp.
            -- Fall back to start + 1 ms when closing instantly after starting because lower and upper
            -- values of a range can't be equal in Postgres.
            time = (
                CASE WHEN "time" IS NOT NULL THEN
                    TSTZRANGE(
                        LOWER("time"),
                        GREATEST(NOW(), LOWER("time") + '1 millisecond'::INTERVAL),
                        '[)'
                    )
                END
            )
        WHERE
            id = $1
        RETURNING
            id as "id: db::id::Id",
            handle_id as "handle_id: HandleId",
            rtc_id as "rtc_id: Id",
            backend_id as "backend_id: AgentId",
            created_at,
            label,
            sent_by as "sent_by: AgentId",
            time as "time: TimeSqlx"
        "#,
        id as Id,
    )
    .fetch_optional(conn)
    .await
}

#[derive(Debug)]
pub struct StreamWithRoomId {
    pub id: Id,
    pub handle_id: HandleId,
    pub rtc_id: db::rtc::Id,
    pub backend_id: AgentId,
    pub label: String,
    pub sent_by: AgentId,
    pub time: Option<TimeSqlx>,
    pub created_at: DateTime<Utc>,
    pub room_id: db::room::Id,
}

impl StreamWithRoomId {
    pub fn janus_rtc_stream(&self) -> Object {
        Object {
            id: self.id,
            handle_id: self.handle_id,
            rtc_id: self.rtc_id,
            backend_id: self.backend_id.clone(),
            label: self.label.clone(),
            sent_by: self.sent_by.clone(),
            time: self.time.clone(),
            created_at: self.created_at,
        }
    }
}

pub async fn stop_running_streams_by_backend(
    backend_id: &AgentId,
    conn: &mut sqlx::PgConnection,
) -> sqlx::Result<Vec<StreamWithRoomId>> {
    sqlx::query_as!(
        StreamWithRoomId,
        r#"
        UPDATE "janus_rtc_stream"
        SET "time" = (
            CASE WHEN "time" IS NOT NULL THEN
                TSTZRANGE(
                    LOWER("time"),
                    GREATEST(NOW(), LOWER("time") + '1 millisecond'::INTERVAL),
                    '[)'
                )
            END
        )
        FROM "rtc"
        WHERE "rtc"."id" = "janus_rtc_stream"."rtc_id"
        AND   (
            lower("janus_rtc_stream"."time") is not null
            and upper("janus_rtc_stream"."time") is null
        )
        AND "janus_rtc_stream"."backend_id" = $1
        RETURNING
            "janus_rtc_stream"."id" as "id: db::id::Id",
            "janus_rtc_stream"."handle_id" as "handle_id: HandleId",
            "janus_rtc_stream"."rtc_id" as "rtc_id: Id",
            "janus_rtc_stream"."backend_id" as "backend_id: AgentId",
            "janus_rtc_stream"."created_at",
            "janus_rtc_stream"."label",
            "janus_rtc_stream"."sent_by" as "sent_by: AgentId",
            "janus_rtc_stream"."time" as "time: TimeSqlx",
            "rtc"."room_id" as "room_id: Id"
        "#,
        backend_id as &AgentId
    )
    .fetch_all(conn)
    .await
}

#[cfg(test)]
pub async fn get_rtc_stream(
    conn: &mut sqlx::PgConnection,
    id: db::janus_rtc_stream::Id,
) -> sqlx::Result<Option<Object>> {
    sqlx::query_as!(
        Object,
        r#"
        SELECT
            "janus_rtc_stream"."id" as "id: db::id::Id",
            "janus_rtc_stream"."handle_id" as "handle_id: HandleId",
            "janus_rtc_stream"."rtc_id" as "rtc_id: Id",
            "janus_rtc_stream"."backend_id" as "backend_id: AgentId",
            "janus_rtc_stream"."created_at",
            "janus_rtc_stream"."label",
            "janus_rtc_stream"."sent_by" as "sent_by: AgentId",
            "janus_rtc_stream"."time" as "time: TimeSqlx"
        FROM janus_rtc_stream
        WHERE
            id = $1
        "#,
        id as Id,
    )
    .fetch_optional(conn)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps};

    #[tokio::test]
    async fn test_stop_running_streams_by_backend() {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = db_sqlx::TestDb::with_local_postgres(&postgres).await;

        let mut conn = db.get_conn().await;

        let rtc_stream = factory::JanusRtcStream::new(USR_AUDIENCE)
            .insert(&mut conn)
            .await;

        let r = stop_running_streams_by_backend(rtc_stream.backend_id(), &mut conn)
            .await
            .expect("Failed to stop running streams");

        assert_eq!(r.len(), 1);
        let stream = &r[0];
        assert_eq!(stream.id, rtc_stream.id());
        assert!(matches!(
            stream.time.as_ref().map(|t| Time::from(t.clone())),
            Some((std::ops::Bound::Included(_), std::ops::Bound::Excluded(_)))
        ));
    }
}
