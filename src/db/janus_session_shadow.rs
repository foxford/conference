use crate::authn::AgentId;
use crate::schema::{janus_session_shadow, rtc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use uuid::Uuid;

#[derive(Debug, Identifiable, Queryable, Associations)]
#[belongs_to(rtc::Record, foreign_key = "rtc_id")]
#[primary_key(rtc_id)]
#[table_name = "janus_session_shadow"]
pub(crate) struct Record {
    rtc_id: Uuid,
    session_id: i64,
    location_id: AgentId,
}

impl Record {
    pub(crate) fn session_id(&self) -> i64 {
        self.session_id
    }

    pub(crate) fn location_id(&self) -> &AgentId {
        &self.location_id
    }
}

#[derive(Debug, Insertable)]
#[table_name = "janus_session_shadow"]
pub(crate) struct InsertQuery<'a> {
    rtc_id: &'a Uuid,
    session_id: i64,
    location_id: &'a AgentId,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(rtc_id: &'a Uuid, session_id: i64, location_id: &'a AgentId) -> Self {
        Self {
            rtc_id,
            session_id,
            location_id,
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Record, Error> {
        use crate::schema::janus_session_shadow::dsl::janus_session_shadow;
        use diesel::RunQueryDsl;

        diesel::insert_into(janus_session_shadow)
            .values(self)
            .get_result(conn)
    }
}

pub(crate) struct FindQuery<'a> {
    rtc_id: &'a Uuid,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new(rtc_id: &'a Uuid) -> Self {
        Self { rtc_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Record, Error> {
        use diesel::prelude::*;

        janus_session_shadow::table
            .filter(janus_session_shadow::rtc_id.eq(self.rtc_id))
            .get_result(conn)
    }
}
