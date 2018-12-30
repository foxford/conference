use crate::schema::janus_handle_shadow;
use crate::transport::AgentId;
use diesel::pg::PgConnection;
use diesel::result::Error;
use uuid::Uuid;

#[derive(Debug, Queryable)]
pub(crate) struct Record {
    handle_id: i64,
    rtc_id: Uuid,
    owner_id: AgentId,
}

#[derive(Debug, Insertable)]
#[table_name = "janus_handle_shadow"]
pub(crate) struct InsertQuery<'a> {
    handle_id: i64,
    rtc_id: &'a Uuid,
    owner_id: &'a AgentId,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(handle_id: i64, rtc_id: &'a Uuid, owner_id: &'a AgentId) -> Self {
        Self {
            handle_id,
            rtc_id,
            owner_id,
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
