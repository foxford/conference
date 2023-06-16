use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::db;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ListWithRtcQuery {
    room_id: super::room::Id,
}

impl ListWithRtcQuery {
    pub fn new(room_id: super::room::Id) -> Self {
        Self { room_id }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                rwcs.id as "id: Id",
                rwcs.rtc_id as "rtc_id: Id",
                rwcs.send_video,
                rwcs.send_audio,
                rwcs.created_at
            FROM rtc_writer_config_snapshot AS rwcs
            INNER JOIN rtc
            ON rwcs.rtc_id = rtc.id
            WHERE
                rtc.room_id = $1
            ORDER BY rwcs.created_at
            "#,
            self.room_id as db::room::Id,
        )
        .fetch_all(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////
pub type Id = db::id::Id;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug)]
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

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO rtc_writer_config_snapshot (rtc_id, send_video, send_audio)
            VALUES ($1, $2, $3)
            RETURNING
                id as "id: Id",
                rtc_id as "rtc_id: Id",
                send_video,
                send_audio,
                created_at
            "#,
            self.rtc_id as Id,
            self.send_video,
            self.send_audio,
        )
        .fetch_one(conn)
        .await
    }
}
