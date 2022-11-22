// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use std::{fmt, ops::Bound};

use super::rtc::Object as Rtc;
use crate::db;
use crate::schema::recording;
use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use diesel_derive_enum::DbEnum;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////

pub type AllColumns = (
    recording::rtc_id,
    recording::started_at,
    recording::segments,
    recording::status,
    recording::mjr_dumps_uris,
);

pub const ALL_COLUMNS: AllColumns = (
    recording::rtc_id,
    recording::started_at,
    recording::segments,
    recording::status,
    recording::mjr_dumps_uris,
);

////////////////////////////////////////////////////////////////////////////////

pub type Segment = (Bound<i64>, Bound<i64>);

#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[PgType = "recording_status"]
#[DieselType = "Recording_status"]
pub enum Status {
    #[serde(rename = "in_progress")]
    InProgress,
    Ready,
    Missing,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let serialized = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        write!(f, "{}", serialized)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Identifiable, Associations, Queryable)]
#[belongs_to(Rtc, foreign_key = "rtc_id")]
#[primary_key(rtc_id)]
#[table_name = "recording"]
pub struct Object {
    rtc_id: db::rtc::Id,
    #[serde(with = "crate::serde::ts_seconds_option")]
    started_at: Option<DateTime<Utc>>,
    segments: Option<Vec<Segment>>,
    status: Status,
    mjr_dumps_uris: Option<Vec<String>>,
}

impl Object {
    pub fn rtc_id(&self) -> db::rtc::Id {
        self.rtc_id
    }

    pub fn started_at(&self) -> &Option<DateTime<Utc>> {
        &self.started_at
    }

    pub fn segments(&self) -> &Option<Vec<Segment>> {
        &self.segments
    }

    pub fn status(&self) -> Status {
        self.status
    }

    /// Get a reference to the object's janus dumps uris.
    pub fn mjr_dumps_uris(&self) -> Option<&Vec<String>> {
        self.mjr_dumps_uris.as_ref()
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

    pub fn execute(self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        recording::table
            .filter(recording::rtc_id.eq(self.rtc_id))
            .get_result(conn)
            .optional()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "recording"]
pub struct InsertQuery {
    rtc_id: db::rtc::Id,
}

impl InsertQuery {
    pub fn new(rtc_id: db::rtc::Id) -> Self {
        Self { rtc_id }
    }

    pub fn execute(self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::recording::dsl::recording;
        use diesel::RunQueryDsl;

        diesel::insert_into(recording).values(self).get_result(conn)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, AsChangeset, Queryable)]
#[table_name = "recording"]
#[primary_key(rtc_id)]
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

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        // do not overwrite existing status with `Status::Missing`
        if let Some(Status::Missing) = self.status {
            let source = recording::table.filter(
                recording::status
                    .is_null()
                    .and(recording::rtc_id.eq(self.rtc_id)),
            );

            diesel::update(source).set(self).get_result(conn)
        } else {
            diesel::update(self).set(self).get_result(conn)
        }
    }
}
