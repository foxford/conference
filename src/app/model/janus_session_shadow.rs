use crate::schema::janus_session_shadow;
use crate::transport::AgentId;
use diesel::pg::PgConnection;
use diesel::result::Error;
use uuid::Uuid;

#[derive(Debug, Queryable)]
pub(crate) struct Record {
    rtc_id: Uuid,
    session_id: i64,
    location_id: AgentId,
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
            .get_result::<Record>(conn)
    }
}
