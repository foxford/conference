use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use svc_agent::AgentId;
use uuid::Uuid;

use crate::db::agent::{Object as Agent, Status as AgentStatus};
use crate::schema::{agent, agent_connection};

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
pub(crate) struct Object {
    agent_id: Uuid,
    handle_id: i64,
    created_at: DateTime<Utc>,
    rtc_id: Uuid,
}

impl Object {
    pub(crate) fn handle_id(&self) -> i64 {
        self.handle_id
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct FindQuery<'a> {
    agent_id: &'a AgentId,
    rtc_id: Uuid,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new(agent_id: &'a AgentId, rtc_id: Uuid) -> Self {
        Self { agent_id, rtc_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
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

pub(crate) struct CountQuery {}

impl CountQuery {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<i64, Error> {
        use diesel::dsl::count;
        use diesel::prelude::*;

        agent::table.select(count(agent::id)).get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "agent_connection"]
pub(crate) struct UpsertQuery {
    agent_id: Uuid,
    rtc_id: Uuid,
    handle_id: i64,
    created_at: DateTime<Utc>,
}

impl UpsertQuery {
    pub(crate) fn new(agent_id: Uuid, rtc_id: Uuid, handle_id: i64) -> Self {
        Self {
            agent_id,
            rtc_id,
            handle_id,
            created_at: Utc::now(),
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
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

// Diesel doesn't support joins in UPDATE/DELETE queries so it's raw SQL.
const BULK_DISCONNECT_BY_ROOM_SQL: &str = r#"
    DELETE FROM agent_connection AS ac
    USING agent AS a
    WHERE a.id = ac.agent_id
    AND   a.room_id = $1
"#;

#[derive(Debug)]
pub(crate) struct BulkDisconnectByRoomQuery {
    room_id: Uuid,
}

impl BulkDisconnectByRoomQuery {
    pub(crate) fn new(room_id: Uuid) -> Self {
        Self { room_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use diesel::prelude::*;
        use diesel::sql_types::Uuid;

        diesel::sql_query(BULK_DISCONNECT_BY_ROOM_SQL)
            .bind::<Uuid, _>(self.room_id)
            .execute(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct BulkDisconnectByRtcQuery {
    rtc_id: Uuid,
}

impl BulkDisconnectByRtcQuery {
    pub(crate) fn new(rtc_id: Uuid) -> Self {
        Self { rtc_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
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
pub(crate) struct BulkDisconnectByBackendQuery<'a> {
    backend_id: &'a AgentId,
}

impl<'a> BulkDisconnectByBackendQuery<'a> {
    pub(crate) fn new(backend_id: &'a AgentId) -> Self {
        Self { backend_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use crate::db::sql::Agent_id;
        use diesel::prelude::*;

        diesel::sql_query(BULK_DISCONNECT_BY_BACKEND_SQL)
            .bind::<Agent_id, _>(self.backend_id)
            .execute(conn)
    }
}
