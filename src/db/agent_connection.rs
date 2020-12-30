use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use svc_agent::AgentId;
use uuid::Uuid;

use crate::db::agent::{Object as Agent, Status as AgentStatus};
use crate::schema::{agent, agent_connection};

pub(crate) type AllColumns = (
    agent_connection::agent_id,
    agent_connection::handle_id,
    agent_connection::created_at,
);

pub(crate) const ALL_COLUMNS: AllColumns = (
    agent_connection::agent_id,
    agent_connection::handle_id,
    agent_connection::created_at,
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
}

impl Object {
    pub(crate) fn handle_id(&self) -> i64 {
        self.handle_id
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct FindQuery<'a> {
    agent_id: &'a AgentId,
    room_id: Uuid,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new(agent_id: &'a AgentId, room_id: Uuid) -> Self {
        Self { agent_id, room_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        agent_connection::table
            .inner_join(agent::table)
            .filter(agent::agent_id.eq(self.agent_id))
            .filter(agent::room_id.eq(self.room_id))
            .filter(agent::status.eq(AgentStatus::Ready))
            .select(ALL_COLUMNS)
            .get_result(conn)
            .optional()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "agent_connection"]
pub(crate) struct UpsertQuery {
    agent_id: Uuid,
    handle_id: i64,
    created_at: DateTime<Utc>,
}

impl UpsertQuery {
    pub(crate) fn new(agent_id: Uuid, handle_id: i64) -> Self {
        Self {
            agent_id,
            handle_id,
            created_at: Utc::now(),
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::agent_connection::dsl::*;
        use diesel::prelude::*;

        diesel::insert_into(agent_connection)
            .values(self)
            .on_conflict(agent_id)
            .do_update()
            .set(self)
            .get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

// Diesel doesn't support joins in UPDATE/DELETE queries so it's raw SQL.
const BULK_DISCONNECT_SQL: &str = r#"
    DELETE FROM agent_connection AS ac
    USING agent AS a,
          room AS r
    WHERE a.id = ac.agent_id
    AND   r.id = a.room_id
    AND   r.backend = 'janus'
    AND   r.backend_id = $1
"#;

#[derive(Debug)]
pub(crate) struct BulkDisconnectQuery<'a> {
    backend_id: &'a AgentId,
}

impl<'a> BulkDisconnectQuery<'a> {
    pub(crate) fn new(backend_id: &'a AgentId) -> Self {
        Self { backend_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use crate::db::sql::Agent_id;
        use diesel::prelude::*;

        diesel::sql_query(BULK_DISCONNECT_SQL)
            .bind::<Agent_id, _>(self.backend_id)
            .execute(conn)
    }
}
