use diesel::pg::PgConnection;
use diesel::result::Error;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::schema::{janus_handle_shadow, janus_session_shadow, room, rtc};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Queryable)]
pub(crate) struct Object {
    handle_id: i64,
    session_id: i64,
    location_id: AgentId,
    rtc_id: Uuid,
    room_id: Uuid,
    audience: String,
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

    pub(crate) fn rtc_id(&self) -> &Uuid {
        &self.rtc_id
    }

    pub(crate) fn room_id(&self) -> &Uuid {
        &self.room_id
    }

    pub(crate) fn audience(&self) -> &str {
        &self.audience
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct FindQuery<'a> {
    reply_to: &'a AgentId,
    rtc_id: Option<&'a Uuid>,
    session_id: Option<i64>,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new(reply_to: &'a AgentId) -> Self {
        Self {
            reply_to,
            rtc_id: None,
            session_id: None,
        }
    }

    pub(crate) fn rtc_id(mut self, rtc_id: &'a Uuid) -> Self {
        self.rtc_id = Some(rtc_id);
        self
    }

    pub(crate) fn session_id(mut self, session_id: i64) -> Self {
        self.session_id = Some(session_id);
        self
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        let mut q = rtc::table
            .inner_join(janus_handle_shadow::table)
            .inner_join(janus_session_shadow::table)
            .inner_join(room::table)
            .filter(janus_handle_shadow::reply_to.eq(self.reply_to))
            .select((
                janus_handle_shadow::handle_id,
                janus_session_shadow::session_id,
                janus_session_shadow::location_id,
                janus_session_shadow::rtc_id,
                room::id,
                room::audience,
            ))
            .into_boxed();

        if let Some(session_id) = self.session_id {
            q = q.filter(janus_session_shadow::session_id.eq(session_id));
        }

        if let Some(rtc_id) = self.rtc_id {
            q = q.filter(janus_handle_shadow::rtc_id.eq(rtc_id));
        }

        q.get_result(conn).optional()
    }
}
