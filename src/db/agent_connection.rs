use chrono::{DateTime, Utc};
use diesel::{dsl::count_star, pg::PgConnection, result::Error};
use svc_agent::AgentId;

use crate::{
    backend::janus::client::HandleId,
    db::{
        self,
        agent::{Object as Agent, Status as AgentStatus},
    },
    schema::{agent, agent_connection},
};

type AllColumns = (
    agent_connection::agent_id,
    agent_connection::handle_id,
    agent_connection::created_at,
    agent_connection::rtc_id,
);

const ALL_COLUMNS: AllColumns = (
    agent_connection::agent_id,
    agent_connection::handle_id,
    agent_connection::created_at,
    agent_connection::rtc_id,
);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Agent, foreign_key = "agent_id")]
#[table_name = "agent_connection"]
#[primary_key(agent_id)]
pub struct Object {
    agent_id: super::agent::Id,
    handle_id: HandleId,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    #[allow(dead_code)]
    rtc_id: db::rtc::Id,
}

impl Object {
    pub fn handle_id(&self) -> HandleId {
        self.handle_id
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct FindQuery<'a> {
    agent_id: &'a AgentId,
    rtc_id: db::rtc::Id,
}

impl<'a> FindQuery<'a> {
    pub fn new(agent_id: &'a AgentId, rtc_id: db::rtc::Id) -> Self {
        Self { agent_id, rtc_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        agent_connection::table
            .inner_join(agent::table)
            .filter(agent::status.eq(AgentStatus::Ready))
            .filter(agent::agent_id.eq(self.agent_id))
            .filter(agent_connection::rtc_id.eq(self.rtc_id))
            .select(ALL_COLUMNS)
            .get_result(conn)
            .optional()
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct CountQuery {}

impl CountQuery {
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<i64, Error> {
        use diesel::prelude::*;

        agent_connection::table
            .select(count_star())
            .get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "agent_connection"]
pub struct UpsertQuery {
    agent_id: db::agent::Id,
    rtc_id: db::rtc::Id,
    handle_id: HandleId,
    created_at: DateTime<Utc>,
}

impl UpsertQuery {
    pub fn new(agent_id: db::agent::Id, rtc_id: db::rtc::Id, handle_id: HandleId) -> Self {
        Self {
            agent_id,
            rtc_id,
            handle_id,
            created_at: Utc::now(),
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::agent_connection::dsl::*;
        use diesel::prelude::*;

        diesel::insert_into(agent_connection)
            .values(self)
            .on_conflict((agent_id, rtc_id))
            .do_update()
            .set(self)
            .get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct BulkDisconnectByRtcQuery {
    rtc_id: db::rtc::Id,
}

impl BulkDisconnectByRtcQuery {
    pub fn new(rtc_id: db::rtc::Id) -> Self {
        Self { rtc_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use diesel::prelude::*;

        diesel::delete(agent_connection::table)
            .filter(agent_connection::rtc_id.eq(self.rtc_id))
            .execute(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

// Diesel doesn't support joins in UPDATE/DELETE queries so it's raw SQL.
const BULK_DISCONNECT_BY_BACKEND_SQL: &str = r#"
    DELETE FROM agent_connection AS ac
    USING agent AS a,
          room AS r
    WHERE a.id = ac.agent_id
    AND   r.id = a.room_id
    AND   r.backend_id = $1
"#;

#[derive(Debug)]
pub struct BulkDisconnectByBackendQuery<'a> {
    backend_id: &'a AgentId,
}

impl<'a> BulkDisconnectByBackendQuery<'a> {
    pub fn new(backend_id: &'a AgentId) -> Self {
        Self { backend_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use crate::db::sql::Agent_id;
        use diesel::prelude::*;

        diesel::sql_query(BULK_DISCONNECT_BY_BACKEND_SQL)
            .bind::<Agent_id, _>(self.backend_id)
            .execute(conn)
    }
}
