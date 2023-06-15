// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use diesel_derive_enum::DbEnum;
use serde::{Deserialize, Serialize};
use svc_agent::AgentId;

use crate::{
    backend::janus::client::HandleId,
    db::{self, agent::Object as Agent},
    schema::agent_connection,
};

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize, PartialEq, Eq, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[PgType = "agent_connection_status"]
#[DieselType = "Agent_connection_status"]
#[sqlx(type_name = "agent_connection_status")]
pub enum Status {
    #[serde(rename = "in_progress")]
    #[sqlx(rename = "in_progress")]
    InProgress,
    #[sqlx(rename = "connected")]
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

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                ac.agent_id as "agent_id: db::id::Id",
                ac.handle_id as "handle_id: HandleId",
                ac.created_at,
                ac.rtc_id as "rtc_id: db::id::Id",
                ac.status as "status: Status"
            FROM agent_connection as ac
            INNER JOIN agent as a
            ON a.id = ac.agent_id
            WHERE
                a.status = 'ready' AND
                a.agent_id = $1 AND
                ac.rtc_id = $2
            "#,
            self.agent_id as &AgentId,
            self.rtc_id as db::id::Id
        )
        .fetch_optional(conn)
        .await
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct CountResult {
    pub count: i64,
}

pub struct CountQuery {}

impl CountQuery {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<CountResult> {
        sqlx::query_as!(
            CountResult,
            r#"
            SELECT COUNT(1) as "count!: i64"
            FROM agent_connection
            "#
        )
        .fetch_one(conn)
        .await
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

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO agent_connection (agent_id, handle_id, created_at, rtc_id)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (agent_id, rtc_id) DO UPDATE
            SET
                agent_id = $1,
                handle_id = $2,
                created_at = $3,
                rtc_id = $4
            RETURNING
                agent_id as "agent_id: db::id::Id",
                handle_id as "handle_id: HandleId",
                created_at,
                rtc_id as "rtc_id: db::id::Id",
                status as "status: Status"
            "#,
            self.agent_id as db::id::Id,
            self.handle_id as HandleId,
            self.created_at,
            self.rtc_id as db::id::Id
        )
        .fetch_one(conn)
        .await
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

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            UPDATE agent_connection
            SET
                status = $2
            WHERE
                handle_id = $1
            RETURNING
                agent_id as "agent_id: db::id::Id",
                handle_id as "handle_id: HandleId",
                created_at,
                rtc_id as "rtc_id: db::id::Id",
                status as "status: Status"
            "#,
            self.handle_id as HandleId,
            self.status as Status
        )
        .fetch_optional(conn)
        .await
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

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<u64> {
        sqlx::query!(
            r#"
            DELETE FROM agent_connection
            WHERE
                created_at = $1 AND
                status = 'in_progress'
            "#,
            self.created_at
        )
        .execute(conn)
        .await
        .map(|r| r.rows_affected())
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

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<u64> {
        sqlx::query!(
            r#"
            DELETE FROM agent_connection
            WHERE
                handle_id = $1
            "#,
            self.handle_id as HandleId
        )
        .execute(conn)
        .await
        .map(|r| r.rows_affected())
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
    use crate::test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps};
    use chrono::{Duration, Utc};
    use diesel::Identifiable;

    #[tokio::test]
    async fn test_cleanup_not_connected_query() {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = db_sqlx::TestDb::with_local_postgres(&postgres).await;

        let old = TestAgent::new("web", "old_agent", USR_AUDIENCE);
        let new = TestAgent::new("web", "new_agent", USR_AUDIENCE);

        let mut conn = db.get_conn().await;

        let room = shared_helpers::insert_room(&mut conn).await;
        let rtc = factory::Rtc::new(room.id())
            .created_by(new.agent_id().to_owned())
            .insert(&mut conn)
            .await;
        let old = factory::Agent::new()
            .agent_id(old.agent_id())
            .room_id(room.id())
            .status(db::agent::Status::Ready)
            .insert(&mut conn)
            .await;

        factory::AgentConnection::new(
            *old.id(),
            rtc.id(),
            crate::backend::janus::client::HandleId::random(),
        )
        .created_at(Utc::now() - Duration::minutes(20))
        .insert(&mut conn)
        .await;

        let new = factory::Agent::new()
            .agent_id(new.agent_id())
            .room_id(room.id())
            .status(db::agent::Status::Ready)
            .insert(&mut conn)
            .await;
        let new_agent_conn = factory::AgentConnection::new(
            *new.id(),
            rtc.id(),
            crate::backend::janus::client::HandleId::random(),
        )
        .insert(&mut conn)
        .await;

        UpdateQuery::new(new_agent_conn.handle_id(), Status::Connected)
            .execute(&mut conn)
            .await
            .unwrap();

        let r = CleanupNotConnectedQuery::new(Utc::now() - Duration::minutes(1))
            .execute(&mut conn)
            .await
            .expect("Failed to run query");

        assert_eq!(r, 1);
    }
}
