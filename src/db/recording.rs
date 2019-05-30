use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use std::ops::Bound;
use uuid::Uuid;

use super::rtc::Object as Rtc;
use crate::schema::recording;

////////////////////////////////////////////////////////////////////////////////

pub(crate) type Time = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Associations, Queryable)]
#[belongs_to(Rtc, foreign_key = "rtc_id")]
#[primary_key(rtc_id)]
#[table_name = "recording"]
pub(crate) struct Object {
    rtc_id: Uuid,
    time: Vec<Time>,
}

impl Object {
    pub(crate) fn into_tuple(self) -> (Uuid, Vec<Time>) {
        (self.rtc_id, self.time)
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
    time: Vec<Time>,
}

impl InsertQuery {
    pub(crate) fn new(rtc_id: Uuid, time: Vec<Time>) -> Self {
        Self { rtc_id, time }
    }

    pub(crate) fn execute(self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::recording::dsl::recording;
        use diesel::RunQueryDsl;

        diesel::insert_into(recording).values(self).get_result(conn)
    }
}
