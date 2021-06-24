use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use derive_more::{Display, FromStr};
use diesel::{pg::PgConnection, result::Error};
use diesel_derive_newtype::DieselNewType;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::db;
use crate::db::rtc::Object as Rtc;
use crate::schema::{rtc, rtc_writer_config_snapshot};

////////////////////////////////////////////////////////////////////////////////

type AllColumns = (
    rtc_writer_config_snapshot::id,
    rtc_writer_config_snapshot::rtc_id,
    rtc_writer_config_snapshot::send_video,
    rtc_writer_config_snapshot::send_audio,
    rtc_writer_config_snapshot::created_at,
);

const ALL_COLUMNS: AllColumns = (
    rtc_writer_config_snapshot::id,
    rtc_writer_config_snapshot::rtc_id,
    rtc_writer_config_snapshot::send_video,
    rtc_writer_config_snapshot::send_audio,
    rtc_writer_config_snapshot::created_at,
);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ListWithRtcQuery {
    room_id: super::room::Id,
}

impl ListWithRtcQuery {
    pub fn new(room_id: super::room::Id) -> Self {
        Self { room_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::prelude::*;

        rtc_writer_config_snapshot::table
            .inner_join(rtc::table)
            .filter(rtc::room_id.eq(self.room_id))
            .select(ALL_COLUMNS)
            .order_by(rtc_writer_config_snapshot::created_at.asc())
            .get_results(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug, Deserialize, Serialize, Display, Copy, Clone, DieselNewType, Hash, PartialEq, Eq, FromStr,
)]
pub struct Id(Uuid);

impl Id {
    pub fn random() -> Self {
        Id(Uuid::new_v4())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, QueryableByName, Associations, Deserialize, Serialize)]
#[belongs_to(Rtc, foreign_key = "rtc_id")]
#[table_name = "rtc_writer_config_snapshot"]
#[primary_key(rtc_id)]
pub struct Object {
    id: Id,
    rtc_id: super::rtc::Id,
    send_video: Option<bool>,
    send_audio: Option<bool>,
    #[serde(with = "ts_milliseconds")]
    created_at: DateTime<Utc>,
}

impl Object {
    #[cfg(test)]
    pub fn send_video(&self) -> Option<bool> {
        self.send_video
    }

    #[cfg(test)]
    pub fn send_audio(&self) -> Option<bool> {
        self.send_audio
    }

    #[cfg(test)]
    pub fn rtc_id(&self) -> super::rtc::Id {
        self.rtc_id
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Insertable, AsChangeset)]
#[table_name = "rtc_writer_config_snapshot"]
pub struct InsertQuery {
    rtc_id: db::rtc::Id,
    send_video: Option<bool>,
    send_audio: Option<bool>,
}

impl InsertQuery {
    pub fn new(rtc_id: db::rtc::Id, send_video: Option<bool>, send_audio: Option<bool>) -> Self {
        Self {
            rtc_id,
            send_video,
            send_audio,
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        diesel::insert_into(rtc_writer_config_snapshot::table)
            .values(self)
            .get_result(conn)
    }
}
