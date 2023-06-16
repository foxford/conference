// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use std::fmt;

use chrono::{serde::ts_seconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use svc_agent::AgentId;

use crate::db;

use super::{recording::Object as Recording, AgentIds};

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
pub type Id = db::id::Id;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, sqlx::Type)]
#[serde(rename_all = "lowercase")]
pub enum SharingPolicy {
    None,
    Shared,
    Owned,
}

impl fmt::Display for SharingPolicy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let serialized = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        write!(f, "{serialized}")
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Object {
    pub id: Id,
    pub room_id: db::room::Id,
    #[serde(with = "ts_seconds")]
    pub created_at: DateTime<Utc>,
    pub created_by: AgentId,
}

impl Object {
    pub fn id(&self) -> Id {
        self.id
    }

    pub fn room_id(&self) -> db::room::Id {
        self.room_id
    }

    pub fn created_by(&self) -> &AgentId {
        &self.created_by
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct FindQuery {
    id: Id,
}

impl FindQuery {
    pub fn new(id: Id) -> Self {
        Self { id }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                id as "id: Id",
                room_id as "room_id: Id",
                created_at,
                created_by as "created_by: AgentId"
            FROM rtc
            WHERE
                id = $1
            "#,
            self.id as Id
        )
        .fetch_optional(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct ListQuery<'a> {
    room_id: Option<db::room::Id>,
    created_by: Option<&'a [&'a AgentId]>,
    offset: Option<i64>,
    limit: Option<i64>,
}

impl<'a> ListQuery<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn room_id(self, room_id: db::room::Id) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
        }
    }

    pub fn created_by(self, created_by: &'a [&'a AgentId]) -> Self {
        Self {
            created_by: Some(created_by),
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
        let created_by = self.created_by.unwrap_or(&[]);
        let created_by = AgentIds(created_by);

        sqlx::query_as!(
            Object,
            r#"
            SELECT
                id as "id: Id",
                room_id as "room_id: Id",
                created_at,
                created_by as "created_by: AgentId"
            FROM rtc
            WHERE
                ($1::uuid IS NULL OR room_id = $1) AND
                (array_length($2::agent_id[], 1) IS NULL OR created_by = ANY($2))
            ORDER BY created_at
            OFFSET $3
            LIMIT $4
            "#,
            self.room_id as Option<Id>,
            created_by as AgentIds,
            self.offset,
            self.limit
        )
        .fetch_all(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct ListWithRecordingQuery {
    room_id: db::room::Id,
}

struct ListWithRecordingRow {
    id: db::rtc::Id,
    room_id: db::room::Id,
    created_at: DateTime<Utc>,
    created_by: AgentId,
    started_at: Option<DateTime<Utc>>,
    segments: Option<Vec<db::recording::SegmentPg>>,
    status: Option<db::recording::Status>,
    mjr_dumps_uris: Option<Vec<String>>,
}

impl ListWithRecordingRow {
    fn split(self) -> (Object, Option<Recording>) {
        (
            Object {
                id: self.id,
                room_id: self.room_id,
                created_at: self.created_at,
                created_by: self.created_by,
            },
            match self.status {
                Some(status) => Some(Recording {
                    rtc_id: self.id,
                    started_at: self.started_at,
                    segments: self.segments,
                    status,
                    mjr_dumps_uris: self.mjr_dumps_uris,
                }),
                None => None,
            },
        )
    }
}

impl ListWithRecordingQuery {
    pub fn new(room_id: db::room::Id) -> Self {
        Self { room_id }
    }

    pub async fn execute(
        &self,
        conn: &mut sqlx::PgConnection,
    ) -> sqlx::Result<Vec<(Object, Option<Recording>)>> {
        sqlx::query_as!(
            ListWithRecordingRow,
            r#"
            SELECT
                rtc.id as "id: db::rtc::Id",
                rtc.room_id as "room_id: db::room::Id",
                rtc.created_at,
                rtc.created_by as "created_by: AgentId",
                recording.started_at,
                recording.segments as "segments: Vec<db::recording::SegmentPg>",
                recording.status as "status?: db::recording::Status",
                recording.mjr_dumps_uris
            FROM rtc
            LEFT JOIN recording
            ON rtc.id = recording.rtc_id
            WHERE
                rtc.room_id = $1
            "#,
            self.room_id as db::room::Id,
        )
        .fetch_all(conn)
        .await
        .map(|r| r.into_iter().map(|r| r.split()).collect())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct InsertQuery<'a> {
    room_id: db::room::Id,
    created_by: &'a AgentId,
}

impl<'a> InsertQuery<'a> {
    pub fn new(room_id: db::room::Id, created_by: &'a AgentId) -> Self {
        Self {
            room_id,
            created_by,
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO rtc (room_id, created_by)
            VALUES ($1, $2)
            RETURNING
                id as "id: Id",
                room_id as "room_id: Id",
                created_at,
                created_by as "created_by: AgentId"
            "#,
            self.room_id as Id,
            self.created_by as &AgentId
        )
        .fetch_one(conn)
        .await
    }
}
