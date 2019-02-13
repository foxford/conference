use std::ops::Bound;

use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use uuid::Uuid;

use super::rtc::Object as Rtc;
use crate::schema::recording;

type TimeIntervals = Vec<(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>)>;

#[derive(Debug, Identifiable, Associations, Queryable)]
#[belongs_to(Rtc, foreign_key = "rtc_id")]
#[primary_key(rtc_id)]
#[table_name = "recording"]
pub(crate) struct Object {
    rtc_id: Uuid,
    time: TimeIntervals,
}

#[derive(Debug, Insertable)]
#[table_name = "recording"]
pub(crate) struct InsertQuery {
    rtc_id: Uuid,
    time: TimeIntervals,
}

impl InsertQuery {
    pub(crate) fn new(rtc_id: Uuid, time: TimeIntervals) -> Self {
        Self { rtc_id, time }
    }

    pub(crate) fn execute(self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::recording::dsl::recording;
        use diesel::RunQueryDsl;

        diesel::insert_into(recording).values(self).get_result(conn)
    }
}
