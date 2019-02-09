use crate::schema::{janus_session_shadow, rtc};
use crate::transport::AgentId;
use diesel::pg::PgConnection;
use diesel::result::Error;
use uuid::Uuid;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, Associations)]
#[belongs_to(rtc::Object, foreign_key = "rtc_id")]
#[primary_key(rtc_id)]
#[table_name = "janus_session_shadow"]
pub(crate) struct Object {
    rtc_id: Uuid,
    session_id: i64,
    location_id: AgentId,
}

impl Object {
    pub(crate) fn rtc_id(&self) -> &Uuid {
        &self.rtc_id
    }

    pub(crate) fn session_id(&self) -> i64 {
        self.session_id
    }

    pub(crate) fn location_id(&self) -> &AgentId {
        &self.location_id
    }
}

///////////////////////////////////////////////////////////////////////////////

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

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::janus_session_shadow::dsl::janus_session_shadow;
        use diesel::RunQueryDsl;

        diesel::insert_into(janus_session_shadow)
            .values(self)
            .get_result(conn)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct FindQuery<'a> {
    rtc_id: Option<&'a Uuid>,
    session_id: Option<i64>,
    location_id: Option<&'a AgentId>,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new() -> Self {
        Self {
            rtc_id: None,
            session_id: None,
            location_id: None,
        }
    }

    pub(crate) fn rtc_id(self, rtc_id: &'a Uuid) -> Self {
        Self {
            rtc_id: Some(rtc_id),
            session_id: self.session_id,
            location_id: self.location_id,
        }
    }

    pub(crate) fn session_id(self, session_id: i64) -> Self {
        Self {
            rtc_id: self.rtc_id,
            session_id: Some(session_id),
            location_id: self.location_id,
        }
    }

    pub(crate) fn location_id(self, location_id: &'a AgentId) -> Self {
        Self {
            rtc_id: self.rtc_id,
            session_id: self.session_id,
            location_id: Some(location_id),
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        match (self.rtc_id, (self.session_id, self.location_id)) {
            (Some(ref rtc_id), _) => janus_session_shadow::table
                .find(rtc_id)
                .get_result(conn)
                .optional(),
            (None, (Some(ref session_id), Some(ref location_id))) => janus_session_shadow::table
                .filter(janus_session_shadow::session_id.eq(session_id))
                .filter(janus_session_shadow::location_id.eq(location_id))
                .get_result(conn)
                .optional(),
            _ => Err(Error::QueryBuilderError(
                "rtc_id or session_id and location_id are required parameters of the query".into(),
            )),
        }
    }
}
