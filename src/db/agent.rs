// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use chrono::{serde::ts_seconds, DateTime, Utc};
use diesel_derive_enum::DbEnum;
use serde::{Deserialize, Serialize};
use svc_agent::AgentId;

use super::room::Object as Room;
use crate::db;
use crate::schema::agent;

////////////////////////////////////////////////////////////////////////////////
pub type Id = db::id::Id;

#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize, PartialEq, Eq, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[PgType = "agent_status"]
#[DieselType = "Agent_status"]
#[sqlx(type_name = "agent_status")]
pub enum Status {
    #[serde(rename = "in_progress")]
    #[sqlx(rename = "in_progress")]
    InProgress,
    #[sqlx(rename = "ready")]
    Ready,
}

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Room, foreign_key = "room_id")]
#[table_name = "agent"]
pub struct Object {
    id: Id,
    agent_id: AgentId,
    room_id: db::room::Id,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
    status: Status,
}

impl Object {
    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    #[cfg(test)]
    pub fn status(&self) -> Status {
        self.status
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct ListQuery<'a> {
    agent_id: Option<&'a AgentId>,
    room_id: Option<db::room::Id>,
    status: Option<Status>,
    offset: Option<i64>,
    limit: Option<i64>,
}

impl<'a> ListQuery<'a> {
    pub fn new() -> Self {
        Self {
            agent_id: None,
            room_id: None,
            status: None,
            offset: None,
            limit: None,
        }
    }

    pub fn agent_id(self, agent_id: &'a AgentId) -> Self {
        Self {
            agent_id: Some(agent_id),
            ..self
        }
    }

    pub fn room_id(self, room_id: db::room::Id) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
        }
    }

    pub fn status(self, status: Status) -> Self {
        Self {
            status: Some(status),
            ..self
        }
    }

    pub fn offset(self, offset: i64) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    pub fn limit(self, limit: i64) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                id as "id: Id",
                agent_id as "agent_id: AgentId",
                room_id as "room_id: Id",
                created_at,
                status as "status: Status"
            FROM agent
            WHERE
                status = 'ready' AND
                ($1::agent_id IS NULL     OR agent_id = $1::agent_id) AND
                ($2::uuid IS NULL         OR room_id  = $2::uuid) AND
                ($3::agent_status IS NULL OR status = $3::agent_status)
            ORDER BY created_at DESC
            OFFSET $4
            LIMIT $5
            "#,
            self.agent_id as Option<&AgentId>,
            self.room_id as Option<Id>,
            self.status as Option<Status>,
            self.offset,
            self.limit,
        )
        .fetch_all(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "agent"]
pub struct InsertQuery<'a> {
    id: Option<Id>,
    agent_id: &'a AgentId,
    room_id: db::room::Id,
    status: Status,
    created_at: Option<DateTime<Utc>>,
}

impl<'a> InsertQuery<'a> {
    pub fn new(agent_id: &'a AgentId, room_id: db::room::Id) -> Self {
        Self {
            id: None,
            agent_id,
            room_id,
            status: Status::InProgress,
            created_at: None,
        }
    }

    #[cfg(test)]
    pub fn status(self, status: Status) -> Self {
        Self { status, ..self }
    }

    #[cfg(test)]
    pub fn created_at(self, created_at: DateTime<Utc>) -> Self {
        Self {
            created_at: Some(created_at),
            ..self
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO agent (agent_id, room_id, status, created_at)
            VALUES ($1, $2, $3, COALESCE($4, now()))
            ON CONFLICT (agent_id, room_id) DO UPDATE
            SET
                status = 'in_progress'
            RETURNING
                id as "id: Id",
                agent_id as "agent_id: AgentId",
                room_id as "room_id: Id",
                created_at,
                status as "status: Status"
            "#,
            self.agent_id as &AgentId,
            self.room_id as Id,
            self.status as Status,
            self.created_at
        )
        .fetch_one(conn)
        .await
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, AsChangeset)]
#[table_name = "agent"]
pub struct UpdateQuery<'a> {
    agent_id: &'a AgentId,
    room_id: db::room::Id,
    status: Option<Status>,
}

impl<'a> UpdateQuery<'a> {
    pub fn new(agent_id: &'a AgentId, room_id: db::room::Id) -> Self {
        Self {
            agent_id,
            room_id,
            status: None,
        }
    }

    pub fn status(self, status: Status) -> Self {
        Self {
            status: Some(status),
            ..self
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            UPDATE agent
            SET
                status = $3
            WHERE
                agent_id = $1 AND
                room_id  = $2
            RETURNING
                id as "id: Id",
                agent_id as "agent_id: AgentId",
                room_id as "room_id: Id",
                created_at,
                status as "status: Status"
            "#,
            self.agent_id as &AgentId,
            self.room_id as Id,
            self.status as Option<Status>,
        )
        .fetch_optional(conn)
        .await
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Deletes the agent and associated agent_connection (cascade).
pub struct DeleteQuery<'a> {
    agent_id: Option<&'a AgentId>,
    room_id: Option<db::room::Id>,
}

impl<'a> DeleteQuery<'a> {
    pub fn new() -> Self {
        Self {
            agent_id: None,
            room_id: None,
        }
    }

    pub fn agent_id(self, agent_id: &'a AgentId) -> Self {
        Self {
            agent_id: Some(agent_id),
            ..self
        }
    }

    pub fn room_id(self, room_id: db::room::Id) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<u64> {
        sqlx::query!(
            r#"
            DELETE FROM agent
            WHERE
                ($1::agent_id IS NULL OR agent_id = $1) AND
                ($2::uuid IS NULL OR room_id  = $2)
            "#,
            self.agent_id as Option<&AgentId>,
            self.room_id as Option<Id>,
        )
        .execute(conn)
        .await
        .map(|r| r.rows_affected())
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct CleanupQuery {
    created_at: DateTime<Utc>,
}

impl CleanupQuery {
    pub fn new(created_at: DateTime<Utc>) -> Self {
        Self { created_at }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<u64> {
        sqlx::query!(
            r#"
            DELETE FROM agent
            WHERE
                created_at < $1
            "#,
            self.created_at,
        )
        .execute(conn)
        .await
        .map(|r| r.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::{CleanupQuery, ListQuery, Status};
    use crate::test_helpers::{db_sqlx::TestDb, prelude::*, test_deps::LocalDeps};
    use chrono::{Duration, Utc};

    #[tokio::test]
    async fn test_cleanup_query() {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = TestDb::with_local_postgres(&postgres).await;

        let old = TestAgent::new("web", "old_agent", USR_AUDIENCE);
        let new = TestAgent::new("web", "new_agent", USR_AUDIENCE);

        let mut conn = db.get_conn().await;

        let room = shared_helpers::insert_room(&mut conn).await;
        factory::Agent::new()
            .agent_id(old.agent_id())
            .room_id(room.id())
            .status(Status::Ready)
            .created_at(Utc::now() - Duration::weeks(7))
            .insert(&mut conn)
            .await;
        factory::Agent::new()
            .agent_id(new.agent_id())
            .room_id(room.id())
            .status(Status::Ready)
            .insert(&mut conn)
            .await;

        let r = ListQuery::new()
            .room_id(room.id())
            .execute(&mut conn)
            .await
            .unwrap();
        assert_eq!(r.len(), 2);

        let r = CleanupQuery::new(Utc::now() - Duration::days(1))
            .execute(&mut conn)
            .await
            .expect("Failed to run query");

        assert_eq!(r, 1);
        let r = ListQuery::new()
            .room_id(room.id())
            .execute(&mut conn)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
    }
}
