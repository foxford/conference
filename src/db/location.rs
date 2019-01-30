use crate::authn::AgentId;
use crate::schema::{janus_handle_shadow, janus_session_shadow, rtc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Queryable)]
pub(crate) struct Object {
    handle_id: i64,
    session_id: i64,
    location_id: AgentId,
}

impl Object {
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

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct FindQuery<'a> {
    reply_to: &'a AgentId,
    rtc_id: &'a Uuid,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new(reply_to: &'a AgentId, rtc_id: &'a Uuid) -> Self {
        Self { reply_to, rtc_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
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
            .optional()
    }
}
