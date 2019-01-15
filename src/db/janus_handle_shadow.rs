use crate::authn::AgentId;
use crate::schema::{janus_handle_shadow, janus_session_shadow, rtc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use uuid::Uuid;

#[derive(Debug, Identifiable, Queryable, Associations)]
#[belongs_to(rtc::Record, foreign_key = "rtc_id")]
#[primary_key(handle_id, rtc_id)]
#[table_name = "janus_handle_shadow"]
pub(crate) struct Record {
    handle_id: i64,
    rtc_id: Uuid,
    reply_to: AgentId,
}

#[derive(Debug, Insertable)]
#[table_name = "janus_handle_shadow"]
pub(crate) struct InsertQuery<'a> {
    handle_id: i64,
    rtc_id: &'a Uuid,
    reply_to: &'a AgentId,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(handle_id: i64, rtc_id: &'a Uuid, reply_to: &'a AgentId) -> Self {
        Self {
            handle_id,
            rtc_id,
            reply_to,
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Record, Error> {
        use crate::schema::janus_handle_shadow::dsl::janus_handle_shadow;
        use diesel::RunQueryDsl;

        diesel::insert_into(janus_handle_shadow)
            .values(self)
            .get_result::<Record>(conn)
    }
}

#[derive(Debug, Queryable)]
pub(crate) struct LocationRecord {
    handle_id: i64,
    session_id: i64,
    location_id: AgentId,
}

impl LocationRecord {
    pub(crate) fn handle_id(&self) -> i64 {
        self.handle_id
    }

    pub(crate) fn session_id(&self) -> i64 {
        self.session_id
    }

    pub(crate) fn location_id(&self) -> &AgentId {
        &self.location_id
    }
}

pub(crate) struct FindLocationQuery<'a> {
    reply_to: &'a AgentId,
    rtc_id: &'a Uuid,
}

impl<'a> FindLocationQuery<'a> {
    pub(crate) fn new(reply_to: &'a AgentId, rtc_id: &'a Uuid) -> Self {
        Self { reply_to, rtc_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<LocationRecord, Error> {
        use diesel::prelude::*;

        rtc::table
            .inner_join(janus_handle_shadow::table)
            .inner_join(janus_session_shadow::table)
            .filter(janus_handle_shadow::reply_to.eq(self.reply_to))
            .filter(janus_handle_shadow::rtc_id.eq(self.rtc_id))
            .select((
                janus_handle_shadow::handle_id,
                janus_session_shadow::session_id,
                janus_session_shadow::location_id,
            ))
            .get_result(conn)
    }
}
