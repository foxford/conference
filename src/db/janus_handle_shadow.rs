use diesel::pg::PgConnection;
use diesel::result::Error;
use uuid::Uuid;

use crate::schema::{janus_handle_shadow, rtc};
use crate::transport::util::AgentId;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, Associations)]
#[belongs_to(rtc::Object, foreign_key = "rtc_id")]
#[primary_key(handle_id, rtc_id)]
#[table_name = "janus_handle_shadow"]
pub(crate) struct Object {
    handle_id: i64,
    rtc_id: Uuid,
    reply_to: AgentId,
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
