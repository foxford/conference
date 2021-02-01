use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use svc_agent::AgentId;
use uuid::Uuid;

use crate::db::agent::Object as Agent;
use crate::schema::{agent, agent_connection};

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

// Diesel doesn't support joins in UPDATE/DELETE queries so it's raw SQL.
const BULK_DISCONNECT_BY_BACKEND_SQL: &str = r#"
    DELETE FROM agent_connection AS ac
    USING agent AS a,
          room AS r
    WHERE a.id = ac.agent_id
    AND   r.id = a.room_id
    AND   r.backend = 'janus'
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