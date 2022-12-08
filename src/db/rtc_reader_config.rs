// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use diesel::dsl::any;
use diesel::{pg::PgConnection, result::Error, RunQueryDsl};
use svc_agent::AgentId;

use crate::{
    db,
    db::rtc::Object as Rtc,
    schema::{rtc, rtc_reader_config},
};

////////////////////////////////////////////////////////////////////////////////

type AllColumns = (
    rtc_reader_config::rtc_id,
    rtc_reader_config::reader_id,
    rtc_reader_config::receive_video,
    rtc_reader_config::receive_audio,
);

const ALL_COLUMNS: AllColumns = (
    rtc_reader_config::rtc_id,
    rtc_reader_config::reader_id,
    rtc_reader_config::receive_video,
    rtc_reader_config::receive_audio,
);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Rtc, foreign_key = "rtc_id")]
#[table_name = "rtc_reader_config"]
#[primary_key(rtc_id, reader_id)]
pub struct Object {
    rtc_id: db::rtc::Id,
    reader_id: AgentId,
    receive_video: bool,
    receive_audio: bool,
}

impl Object {
    pub fn rtc_id(&self) -> db::rtc::Id {
        self.rtc_id
    }

    pub fn reader_id(&self) -> &AgentId {
        &self.reader_id
    }

    pub fn receive_video(&self) -> bool {
        self.receive_video
    }

    pub fn receive_audio(&self) -> bool {
        self.receive_audio
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ListWithRtcQuery<'a> {
    room_id: db::room::Id,
    reader_ids: &'a [&'a AgentId],
}

impl<'a> ListWithRtcQuery<'a> {
    pub fn new(room_id: db::room::Id, reader_ids: &'a [&'a AgentId]) -> Self {
        Self {
            room_id,
            reader_ids,
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Vec<(Object, Rtc)>, Error> {
        use diesel::prelude::*;

        rtc_reader_config::table
            .inner_join(rtc::table)
            .filter(rtc::room_id.eq(self.room_id))
            .filter(rtc_reader_config::reader_id.eq(any(self.reader_ids)))
            .select((ALL_COLUMNS, db::rtc::ALL_COLUMNS))
            .get_results(conn)
    }
}

pub fn read_config(
    rtc_id: db::rtc::Id,
    connection: &PgConnection,
) -> Result<Option<Vec<Object>>, Error> {
    use diesel::prelude::*;

    rtc_reader_config::table
        .filter(rtc_reader_config::rtc_id.eq(rtc_id))
        .select(ALL_COLUMNS)
        .get_results(connection)
        .optional()
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Insertable, AsChangeset)]
#[table_name = "rtc_reader_config"]
pub struct UpsertQuery<'a> {
    rtc_id: db::rtc::Id,
    reader_id: &'a AgentId,
    receive_video: Option<bool>,
    receive_audio: Option<bool>,
}

impl<'a> UpsertQuery<'a> {
    pub fn new(rtc_id: db::rtc::Id, reader_id: &'a AgentId) -> Self {
        Self {
            rtc_id,
            reader_id,
            receive_video: None,
            receive_audio: None,
        }
    }

    pub fn receive_video(self, receive_video: bool) -> Self {
        Self {
            receive_video: Some(receive_video),
            ..self
        }
    }

    pub fn receive_audio(self, receive_audio: bool) -> Self {
        Self {
            receive_audio: Some(receive_audio),
            ..self
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        let mut insert_values = self.clone();

        if insert_values.receive_video.is_none() {
            insert_values.receive_video = Some(true);
        }

        if insert_values.receive_audio.is_none() {
            insert_values.receive_audio = Some(true);
        }

        diesel::insert_into(rtc_reader_config::table)
            .values(insert_values)
            .on_conflict((rtc_reader_config::rtc_id, rtc_reader_config::reader_id))
            .do_update()
            .set(self)
            .get_result(conn)
    }
}

pub fn batch_insert(conn: &PgConnection, configs: &[UpsertQuery]) -> Result<Vec<Object>, Error> {
    use crate::diesel::ExpressionMethods;
    use crate::schema::rtc_reader_config::*;
    use diesel::pg::upsert::excluded;

    diesel::insert_into(rtc_reader_config::table)
        .values(configs)
        .on_conflict((rtc_reader_config::rtc_id, rtc_reader_config::reader_id))
        .do_update()
        .set((
            receive_video.eq(excluded(receive_video)),
            receive_audio.eq(excluded(receive_audio)),
        ))
        .get_results(conn)
}
