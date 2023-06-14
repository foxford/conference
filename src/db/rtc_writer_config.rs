// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use svc_agent::AgentId;

use crate::{db, db::rtc::Object as Rtc, schema::rtc_writer_config};

////////////////////////////////////////////////////////////////////////////////

type AllColumns = (
    rtc_writer_config::rtc_id,
    rtc_writer_config::send_video,
    rtc_writer_config::send_audio,
    rtc_writer_config::video_remb,
    rtc_writer_config::send_audio_updated_by,
    rtc_writer_config::updated_at,
);

const ALL_COLUMNS: AllColumns = (
    rtc_writer_config::rtc_id,
    rtc_writer_config::send_video,
    rtc_writer_config::send_audio,
    rtc_writer_config::video_remb,
    rtc_writer_config::send_audio_updated_by,
    rtc_writer_config::updated_at,
);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Rtc, foreign_key = "rtc_id")]
#[table_name = "rtc_writer_config"]
#[primary_key(rtc_id)]
pub struct Object {
    rtc_id: db::rtc::Id,
    send_video: bool,
    send_audio: bool,
    video_remb: Option<i64>,
    send_audio_updated_by: Option<AgentId>,
    updated_at: DateTime<Utc>,
}

impl Object {
    pub fn send_video(&self) -> bool {
        self.send_video
    }

    pub fn send_audio(&self) -> bool {
        self.send_audio
    }

    pub fn video_remb(&self) -> Option<i64> {
        self.video_remb
    }

    pub fn send_audio_updated_by(&self) -> Option<&AgentId> {
        self.send_audio_updated_by.as_ref()
    }

    /// Get a reference to the object's updated at.
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ListWithRtcQuery {
    room_id: db::room::Id,
}

struct ListWithRtcRow {
    rtc_id: db::rtc::Id,
    send_video: bool,
    send_audio: bool,
    video_remb: Option<i64>,
    send_audio_updated_by: Option<AgentId>,
    updated_at: DateTime<Utc>,
    room_id: db::room::Id,
    created_at: DateTime<Utc>,
    created_by: AgentId,
}

impl ListWithRtcRow {
    fn split(self) -> (Object, Rtc) {
        (
            Object {
                rtc_id: self.rtc_id,
                send_video: self.send_video,
                send_audio: self.send_audio,
                video_remb: self.video_remb,
                send_audio_updated_by: self.send_audio_updated_by,
                updated_at: self.updated_at,
            },
            Rtc {
                id: self.rtc_id,
                room_id: self.room_id,
                created_at: self.created_at,
                created_by: self.created_by,
            },
        )
    }
}

impl ListWithRtcQuery {
    pub fn new(room_id: db::room::Id) -> Self {
        Self { room_id }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<(Object, Rtc)>> {
        sqlx::query_as!(
            ListWithRtcRow,
            r#"
            SELECT
                r.id as "rtc_id: db::rtc::Id",
                rwc.send_video,
                rwc.send_audio,
                rwc.video_remb,
                rwc.send_audio_updated_by as "send_audio_updated_by: AgentId",
                rwc.updated_at,
                r.room_id as "room_id: db::room::Id",
                r.created_at,
                r.created_by as "created_by: AgentId"
            FROM rtc_writer_config as rwc
            INNER JOIN rtc as r
            ON rwc.rtc_id = r.id
            WHERE
                r.room_id = $1
            "#,
            self.room_id as db::room::Id,
        )
        .fetch_all(conn)
        .await
        .map(|r| r.into_iter().map(|o| o.split()).collect())
    }
}

pub fn read_config(
    rtc_id: db::rtc::Id,
    connection: &PgConnection,
) -> Result<Option<Object>, Error> {
    use diesel::prelude::*;

    rtc_writer_config::table
        .filter(rtc_writer_config::rtc_id.eq(rtc_id))
        .select(ALL_COLUMNS)
        .get_result(connection)
        .optional()
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Insertable, AsChangeset)]
#[table_name = "rtc_writer_config"]
pub struct UpsertQuery<'a> {
    rtc_id: db::rtc::Id,
    send_video: Option<bool>,
    send_audio: Option<bool>,
    video_remb: Option<i64>,
    send_audio_updated_by: Option<&'a AgentId>,
}

impl<'a> UpsertQuery<'a> {
    pub fn new(rtc_id: db::rtc::Id) -> Self {
        Self {
            rtc_id,
            send_audio: Default::default(),
            send_audio_updated_by: Default::default(),
            send_video: Default::default(),
            video_remb: Default::default(),
        }
    }

    pub fn send_video(self, send_video: bool) -> Self {
        Self {
            send_video: Some(send_video),
            ..self
        }
    }

    pub fn send_audio(self, send_audio: bool) -> Self {
        Self {
            send_audio: Some(send_audio),
            ..self
        }
    }

    pub fn video_remb(self, video_remb: i64) -> Self {
        Self {
            video_remb: Some(video_remb),
            ..self
        }
    }

    pub fn send_audio_updated_by(self, send_audio_updated_by: &'a AgentId) -> Self {
        Self {
            send_audio_updated_by: Some(send_audio_updated_by),
            ..self
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        let send_video = self.send_video.unwrap_or(true);
        let send_audio = self.send_audio.unwrap_or(true);

        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO rtc_writer_config (rtc_id, send_video, send_audio, video_remb, send_audio_updated_by)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (rtc_id) DO UPDATE
            SET
                send_video = $2,
                send_audio = $3,
                video_remb = $4,
                send_audio_updated_by = $5
            RETURNING
                rtc_id as "rtc_id: db::rtc::Id",
                send_video,
                send_audio,
                video_remb,
                send_audio_updated_by as "send_audio_updated_by: AgentId",
                updated_at
            "#,
            self.rtc_id as db::rtc::Id,
            send_video,
            send_audio,
            self.video_remb,
            self.send_audio_updated_by as Option<&AgentId>,
        )
        .fetch_one(conn)
        .await
    }
}
