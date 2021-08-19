use diesel::{pg::PgConnection, result::Error};
use svc_agent::AgentId;

use crate::{
    db,
    db::rtc::Object as Rtc,
    schema::{rtc, rtc_writer_config},
};

////////////////////////////////////////////////////////////////////////////////

type AllColumns = (
    rtc_writer_config::rtc_id,
    rtc_writer_config::send_video,
    rtc_writer_config::send_audio,
    rtc_writer_config::video_remb,
    rtc_writer_config::send_audio_updated_by,
);

const ALL_COLUMNS: AllColumns = (
    rtc_writer_config::rtc_id,
    rtc_writer_config::send_video,
    rtc_writer_config::send_audio,
    rtc_writer_config::video_remb,
    rtc_writer_config::send_audio_updated_by,
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
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ListWithRtcQuery {
    room_id: db::room::Id,
}

impl ListWithRtcQuery {
    pub fn new(room_id: db::room::Id) -> Self {
        Self { room_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Vec<(Object, Rtc)>, Error> {
        use diesel::prelude::*;

        rtc_writer_config::table
            .inner_join(rtc::table)
            .filter(rtc::room_id.eq(self.room_id))
            .select((ALL_COLUMNS, crate::db::rtc::ALL_COLUMNS))
            .get_results(conn)
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

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        let mut insert_values = self.clone();

        if insert_values.send_video.is_none() {
            insert_values.send_video = Some(true);
        }

        if insert_values.send_audio.is_none() {
            insert_values.send_audio = Some(true);
        }

        diesel::insert_into(rtc_writer_config::table)
            .values(insert_values)
            .on_conflict(rtc_writer_config::rtc_id)
            .do_update()
            .set(self)
            .get_result(conn)
    }
}
