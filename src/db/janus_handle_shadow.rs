use diesel::pg::PgConnection;
use diesel::result::Error;
use uuid::Uuid;

use super::rtc::Object as Rtc;
use crate::schema::janus_handle_shadow;
use crate::transport::AgentId;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, Associations)]
#[belongs_to(Rtc, foreign_key = "rtc_id")]
#[primary_key(handle_id, rtc_id)]
#[table_name = "janus_handle_shadow"]
pub(crate) struct Object {
    handle_id: i64,
    rtc_id: Uuid,
    reply_to: AgentId,
}

impl Object {
    pub(crate) fn handle_id(&self) -> i64 {
        self.handle_id
    }
}

////////////////////////////////////////////////////////////////////////////////

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

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::janus_handle_shadow::dsl::janus_handle_shadow;
        use diesel::RunQueryDsl;

        diesel::insert_into(janus_handle_shadow)
            .values(self)
            .get_result(conn)
    }
}

/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct FindQuery<'a> {
    handle_id: Option<i64>,
    rtc_id: Option<&'a Uuid>,
    reply_to: Option<&'a AgentId>,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new() -> Self {
        Self {
            handle_id: None,
            rtc_id: None,
            reply_to: None,
        }
    }

    pub(crate) fn handle_id(mut self, handle_id: i64) -> Self {
        self.handle_id = Some(handle_id);
        self
    }

    pub(crate) fn rtc_id(mut self, rtc_id: &'a Uuid) -> Self {
        self.rtc_id = Some(rtc_id);
        self
    }

    pub(crate) fn reply_to(mut self, reply_to: &'a AgentId) -> Self {
        self.reply_to = Some(reply_to);
        self
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        match self.rtc_id {
            Some(rtc_id) => janus_handle_shadow::table
                .filter(janus_handle_shadow::rtc_id.eq(rtc_id))
                .get_result(conn)
                .optional(),
            _ => Err(Error::QueryBuilderError(
                "rtc_id or session_id and location_id are required parameters of the query".into(),
            )),
        }
    }
}
