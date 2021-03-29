use diesel::{pg::PgConnection, result::Error};
use uuid::Uuid;

use crate::db::rtc::Object as Rtc;
use crate::schema::{rtc, rtc_writer_config};

////////////////////////////////////////////////////////////////////////////////

type AllColumns = (
    rtc_writer_config::rtc_id,
    rtc_writer_config::send_video,
    rtc_writer_config::send_audio,
    rtc_writer_config::video_remb,
);

const ALL_COLUMNS: AllColumns = (
    rtc_writer_config::rtc_id,
    rtc_writer_config::send_video,
    rtc_writer_config::send_audio,
    rtc_writer_config::video_remb,
);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Rtc, foreign_key = "rtc_id")]
#[table_name = "rtc_writer_config"]
#[primary_key(rtc_id)]
pub(crate) struct Object {
    rtc_id: Uuid,
    send_video: bool,
    send_audio: bool,
    video_remb: Option<i64>,
}

impl Object {
    pub(crate) fn send_video(&self) -> bool {
        self.send_video
    }

    pub(crate) fn send_audio(&self) -> bool {
        self.send_audio
    }

    pub(crate) fn video_remb(&self) -> Option<i64> {
        self.video_remb
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct ListWithRtcQuery {
    room_id: Uuid,
}

impl ListWithRtcQuery {
    pub(crate) fn new(room_id: Uuid) -> Self {
        Self { room_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Vec<(Object, Rtc)>, Error> {
        use diesel::prelude::*;

        rtc_writer_config::table
            .inner_join(rtc::table)
            .filter(rtc::room_id.eq(self.room_id))
            .select((ALL_COLUMNS, crate::db::rtc::ALL_COLUMNS))
            .get_results(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Insertable, AsChangeset)]
#[table_name = "rtc_writer_config"]
pub(crate) struct UpsertQuery {
    rtc_id: Uuid,
    send_video: Option<bool>,
    send_audio: Option<bool>,
    video_remb: Option<i64>,
}

impl UpsertQuery {
    pub(crate) fn new(rtc_id: Uuid) -> Self {
        Self {
            rtc_id,
            send_video: None,
            send_audio: None,
            video_remb: None,
        }
    }

    pub(crate) fn send_video(self, send_video: bool) -> Self {
        Self {
            send_video: Some(send_video),
            ..self
        }
    }

    pub(crate) fn send_audio(self, send_audio: bool) -> Self {
        Self {
            send_audio: Some(send_audio),
            ..self
        }
    }

    pub(crate) fn video_remb(self, video_remb: i64) -> Self {
        Self {
            video_remb: Some(video_remb),
            ..self
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
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
