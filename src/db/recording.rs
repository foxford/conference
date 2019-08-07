use std::fmt;
use std::ops::Bound;

use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use super::rtc::Object as Rtc;
use crate::schema::recording;

////////////////////////////////////////////////////////////////////////////////

pub(crate) type Time = (Bound<i64>, Bound<i64>);

#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RecordingStatus {
    Ready,
    Missing,
}

impl fmt::Display for RecordingStatus {
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
pub(crate) struct Object {
    rtc_id: Uuid,
    #[serde(with = "crate::serde::ts_seconds_option")]
    started_at: Option<DateTime<Utc>>,
    time: Option<Vec<Time>>,
    status: RecordingStatus,
}

impl Object {
    pub(crate) fn into_tuple(
        self,
    ) -> (
        Uuid,
        RecordingStatus,
        Option<DateTime<Utc>>,
        Option<Vec<Time>>,
    ) {
        (self.rtc_id, self.status, self.started_at, self.time)
    }

    pub(crate) fn started_at(&self) -> &Option<DateTime<Utc>> {
        &self.started_at
    }

    pub(crate) fn time(&self) -> &Option<Vec<Time>> {
        &self.time
    }

    pub(crate) fn status(&self) -> &RecordingStatus {
        &self.status
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "recording"]
pub(crate) struct InsertQuery {
    rtc_id: Uuid,
    started_at: Option<DateTime<Utc>>,
    time: Option<Vec<Time>>,
    status: RecordingStatus,
}

impl InsertQuery {
    pub(crate) fn new(rtc_id: Uuid, status: RecordingStatus) -> Self {
        Self {
            rtc_id,
            started_at: None,
            time: None,
            status,
        }
    }

    pub(crate) fn started_at(self, started_at: DateTime<Utc>) -> Self {
        Self {
            started_at: Some(started_at),
            ..self
        }
    }

    pub(crate) fn time(self, time: Vec<Time>) -> Self {
        Self {
            time: Some(time),
            ..self
        }
    }

    pub(crate) fn execute(self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::recording::dsl::recording;
        use diesel::RunQueryDsl;

        diesel::insert_into(recording).values(self).get_result(conn)
    }
}
