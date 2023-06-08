// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use chrono::{DateTime, Utc};
use diesel::{dsl::count_star, pg::PgConnection, result::Error};
use diesel_derive_enum::DbEnum;
use serde::{Deserialize, Serialize};
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
    agent_connection::status,
);

const ALL_COLUMNS: AllColumns = (
    agent_connection::agent_id,
    agent_connection::handle_id,
    agent_connection::created_at,
    agent_connection::rtc_id,
    agent_connection::status,
);

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[PgType = "agent_connection_status"]
#[DieselType = "Agent_connection_status"]
pub enum Status {
    #[serde(rename = "in_progress")]
    InProgress,
    Connected,
}

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
    #[allow(dead_code)]
    status: Status,
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

    #[cfg(test)]
    pub fn created_at(self, created_at: DateTime<Utc>) -> Self {
        Self { created_at, ..self }
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

#[derive(Debug, AsChangeset)]
#[table_name = "agent_connection"]
pub struct UpdateQuery {
    handle_id: HandleId,
    status: Status,
}

impl UpdateQuery {
    pub fn new(handle_id: HandleId, status: Status) -> Self {
        Self { handle_id, status }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use crate::schema::agent_connection::dsl::*;
        use diesel::prelude::*;

        let query = agent_connection.filter(handle_id.eq_all(self.handle_id));

        diesel::update(query).set(self).get_result(conn).optional()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct CleanupNotConnectedQuery {
    created_at: DateTime<Utc>,
}

impl CleanupNotConnectedQuery {
    pub fn new(created_at: DateTime<Utc>) -> Self {
        Self { created_at }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use crate::schema::agent_connection::dsl::*;
        use diesel::prelude::*;

        let q = agent_connection
            .filter(created_at.lt(self.created_at))
            .filter(status.eq(Status::InProgress));

        diesel::delete(q).execute(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DisconnectSingleAgentQuery {
    handle_id: HandleId,
}

impl DisconnectSingleAgentQuery {
    pub fn new(handle_id: HandleId) -> Self {
        Self { handle_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use diesel::prelude::*;

        diesel::delete(agent_connection::table)
            .filter(agent_connection::handle_id.eq(self.handle_id))
            .execute(conn)
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

#[derive(Debug)]
pub struct BulkDisconnectByBackendQuery<'a> {
    backend_id: &'a AgentId,
}

impl<'a> BulkDisconnectByBackendQuery<'a> {
    pub fn new(backend_id: &'a AgentId) -> Self {
        Self { backend_id }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<u64> {
        sqlx::query!(
            r#"
            DELETE FROM agent_connection AS ac
            USING agent AS a,
                room AS r
            WHERE a.id = ac.agent_id
            AND   r.id = a.room_id
            AND   r.backend_id = $1
            "#,
            self.backend_id as &AgentId
        )
        .execute(conn)
        .await
        .map(|r| r.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{prelude::*, test_deps::LocalDeps};
    use chrono::{Duration, Utc};
    use diesel::Identifiable;

    #[test]
    fn test_cleanup_not_connected_query() {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = TestDb::with_local_postgres(&postgres);
        let old = TestAgent::new("web", "old_agent", USR_AUDIENCE);
        let new = TestAgent::new("web", "new_agent", USR_AUDIENCE);

        db.connection_pool()
            .get()
            .map(|conn| {
                let room = shared_helpers::insert_room(&conn);
                let rtc = factory::Rtc::new(room.id())
                    .created_by(new.agent_id().to_owned())
                    .insert(&conn);
                let old = factory::Agent::new()
                    .agent_id(old.agent_id())
                    .room_id(room.id())
                    .status(db::agent::Status::Ready)
                    .insert(&conn);
                factory::AgentConnection::new(
                    *old.id(),
                    rtc.id(),
                    crate::backend::janus::client::HandleId::random(),
                )
                .created_at(Utc::now() - Duration::minutes(20))
                .insert(&conn);

                let new = factory::Agent::new()
                    .agent_id(new.agent_id())
                    .room_id(room.id())
                    .status(db::agent::Status::Ready)
                    .insert(&conn);
                let new_agent_conn = factory::AgentConnection::new(
                    *new.id(),
                    rtc.id(),
                    crate::backend::janus::client::HandleId::random(),
                )
                .insert(&conn);
                UpdateQuery::new(new_agent_conn.handle_id(), Status::Connected)
                    .execute(&conn)
                    .unwrap();

                room
            })
            .expect("Failed to insert room");

        let conn = db.connection_pool().get().unwrap();
        let r = CleanupNotConnectedQuery::new(Utc::now() - Duration::minutes(1))
            .execute(&conn)
            .expect("Failed to run query");

        assert_eq!(r, 1);
    }
}
