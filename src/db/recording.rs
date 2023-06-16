use std::{fmt, ops::Bound};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::db;

////////////////////////////////////////////////////////////////////////////////

pub type Segment = (Bound<i64>, Bound<i64>);

#[derive(sqlx::Type, Debug, Clone)]
#[sqlx(transparent)]
pub struct SegmentSqlx(sqlx::postgres::types::PgRange<i64>);

impl From<Segment> for SegmentSqlx {
    fn from(value: Segment) -> Self {
        SegmentSqlx(sqlx::postgres::types::PgRange::from(value))
    }
}

impl From<SegmentSqlx> for Segment {
    fn from(value: SegmentSqlx) -> Self {
        (value.0.start, value.0.end)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "recording_status")]
pub enum Status {
    #[serde(rename = "in_progress")]
    InProgress,
    Ready,
    Missing,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let serialized = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        write!(f, "{serialized}")
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub struct Object {
    pub rtc_id: db::rtc::Id,
    #[serde(with = "crate::serde::ts_seconds_option")]
    pub started_at: Option<DateTime<Utc>>,
    pub segments: Option<Vec<Segment>>,
    pub status: Status,
    pub mjr_dumps_uris: Option<Vec<String>>,
}

impl Object {
    pub fn rtc_id(&self) -> db::rtc::Id {
        self.rtc_id
    }

    pub fn status(&self) -> Status {
        self.status
    }

    /// Get a reference to the object's janus dumps uris.
    pub fn mjr_dumps_uris(&self) -> Option<&Vec<String>> {
        self.mjr_dumps_uris.as_ref()
    }
}

pub struct ObjectSqlx {
    rtc_id: db::rtc::Id,
    started_at: Option<DateTime<Utc>>,
    segments: Option<Vec<SegmentSqlx>>,
    status: Status,
    mjr_dumps_uris: Option<Vec<String>>,
}

impl From<ObjectSqlx> for Object {
    fn from(value: ObjectSqlx) -> Self {
        Object {
            rtc_id: value.rtc_id,
            started_at: value.started_at,
            segments: value
                .segments
                .map(|ss| ss.into_iter().map(Segment::from).collect()),
            status: value.status,
            mjr_dumps_uris: value.mjr_dumps_uris,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FindQuery {
    rtc_id: db::rtc::Id,
}

impl FindQuery {
    pub fn new(rtc_id: db::rtc::Id) -> Self {
        Self { rtc_id }
    }

    pub async fn execute(self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        sqlx::query_as!(
            ObjectSqlx,
            r#"
            SELECT
                rtc_id as "rtc_id: db::rtc::Id",
                started_at,
                segments as "segments: Vec<SegmentSqlx>",
                status as "status: Status",
                mjr_dumps_uris
            FROM recording
            WHERE
                rtc_id = $1
            "#,
            self.rtc_id as db::rtc::Id,
        )
        .fetch_optional(conn)
        .await
        .map(|o| o.map(Object::from))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct InsertQuery {
    rtc_id: db::rtc::Id,
}

impl InsertQuery {
    pub fn new(rtc_id: db::rtc::Id) -> Self {
        Self { rtc_id }
    }

    pub async fn execute(self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            ObjectSqlx,
            r#"
            INSERT INTO recording (rtc_id)
            VALUES ($1)
            RETURNING
                rtc_id as "rtc_id: db::rtc::Id",
                started_at,
                segments as "segments: Vec<SegmentSqlx>",
                status as "status: Status",
                mjr_dumps_uris
            "#,
            self.rtc_id as db::rtc::Id,
        )
        .fetch_one(conn)
        .await
        .map(Object::from)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UpdateQuery {
    rtc_id: db::rtc::Id,
    status: Option<Status>,
    mjr_dumps_uris: Option<Vec<String>>,
}

impl UpdateQuery {
    pub fn new(rtc_id: db::rtc::Id) -> Self {
        Self {
            rtc_id,
            status: None,
            mjr_dumps_uris: None,
        }
    }

    pub fn status(self, status: Status) -> Self {
        Self {
            status: Some(status),
            ..self
        }
    }

    pub fn mjr_dumps_uris(self, mjr_dumps_uris: Vec<String>) -> Self {
        Self {
            mjr_dumps_uris: Some(mjr_dumps_uris),
            ..self
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            ObjectSqlx,
            r#"
            UPDATE recording
            SET
                status = $1,
                mjr_dumps_uris = $2
            WHERE
                rtc_id = $3 AND
                -- do not overwrite existing `ready` status with `missing`
                (
                    $1 <> 'missing'::recording_status OR
                    status = 'in_progress'
                )
            RETURNING
                rtc_id as "rtc_id: db::rtc::Id",
                started_at,
                segments as "segments: Vec<SegmentSqlx>",
                status as "status: Status",
                mjr_dumps_uris
            "#,
            self.status as Option<Status>,
            self.mjr_dumps_uris.as_ref().map(|m| m.as_slice()),
            self.rtc_id as db::rtc::Id,
        )
        .fetch_one(conn)
        .await
        .map(Object::from)
    }
}
