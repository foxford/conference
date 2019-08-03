use std::ops::Bound;

use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use serde_derive::Serialize;
use uuid::Uuid;

use super::rtc::Object as Rtc;
use crate::schema::recording;

////////////////////////////////////////////////////////////////////////////////

pub(crate) type Time = (Bound<i64>, Bound<i64>);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Identifiable, Associations, Queryable)]
#[belongs_to(Rtc, foreign_key = "rtc_id")]
#[primary_key(rtc_id)]
#[table_name = "recording"]
pub(crate) struct Object {
    rtc_id: Uuid,
    #[serde(with = "ts_seconds")]
    started_at: DateTime<Utc>,
    time: Vec<Time>,
}

impl Object {
    pub(crate) fn into_tuple(self) -> (Uuid, DateTime<Utc>, Vec<Time>) {
        (self.rtc_id, self.started_at, self.time)
    }

    pub(crate) fn started_at(&self) -> &DateTime<Utc> {
        &self.started_at
    }

    pub(crate) fn time(&self) -> &Vec<Time> {
        &self.time
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "recording"]
pub(crate) struct InsertQuery {
    rtc_id: Uuid,
    started_at: DateTime<Utc>,
    time: Vec<Time>,
}

impl InsertQuery {
    pub(crate) fn new(rtc_id: Uuid, started_at: DateTime<Utc>, time: Vec<Time>) -> Self {
        Self {
            rtc_id,
            started_at,
            time,
        }
    }

    pub(crate) fn execute(self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::recording::dsl::recording;
        use diesel::RunQueryDsl;

        diesel::insert_into(recording).values(self).get_result(conn)
    }
}
