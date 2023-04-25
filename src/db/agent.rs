// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use chrono::{serde::ts_seconds, DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use diesel_derive_enum::DbEnum;
use serde::{Deserialize, Serialize};
use svc_agent::AgentId;

use super::room::Object as Room;
use crate::db;
use crate::schema::agent;

////////////////////////////////////////////////////////////////////////////////
pub type Id = db::id::Id;

#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[PgType = "agent_status"]
#[DieselType = "Agent_status"]
pub enum Status {
    #[serde(rename = "in_progress")]
    InProgress,
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

    pub fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::prelude::*;

        let mut q = agent::table
            .into_boxed()
            .filter(agent::status.eq(Status::Ready));

        if let Some(agent_id) = self.agent_id {
            q = q.filter(agent::agent_id.eq(agent_id));
        }

        if let Some(room_id) = self.room_id {
            q = q.filter(agent::room_id.eq(room_id));
        }

        if let Some(status) = self.status {
            q = q.filter(agent::status.eq(status));
        }

        if let Some(offset) = self.offset {
            q = q.offset(offset);
        }

        if let Some(limit) = self.limit {
            q = q.limit(limit);
        }

        q.order_by(agent::created_at.desc()).get_results(conn)
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
    #[cfg(test)]
    created_at: Option<DateTime<Utc>>,
}

impl<'a> InsertQuery<'a> {
    pub fn new(agent_id: &'a AgentId, room_id: db::room::Id) -> Self {
        Self {
            id: None,
            agent_id,
            room_id,
            status: Status::InProgress,
            #[cfg(test)]
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

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::agent::dsl::*;
        use diesel::{ExpressionMethods, RunQueryDsl};

        diesel::insert_into(agent)
            .values(self)
            .on_conflict((agent_id, room_id))
            .do_update()
            .set(status.eq(Status::InProgress))
            .get_result(conn)
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

    pub fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        let query = agent::table
            .filter(agent::agent_id.eq(self.agent_id))
            .filter(agent::room_id.eq(self.room_id));

        diesel::update(query).set(self).get_result(conn).optional()
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

    pub fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use diesel::prelude::*;

        let mut query = diesel::delete(agent::table).into_boxed();

        if let Some(agent_id) = self.agent_id {
            query = query.filter(agent::agent_id.eq(agent_id));
        }

        if let Some(room_id) = self.room_id {
            query = query.filter(agent::room_id.eq(room_id));
        }

        query.execute(conn)
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

    pub fn execute(&self, conn: &PgConnection) -> Result<usize, Error> {
        use diesel::prelude::*;

        diesel::delete(agent::table.filter(agent::created_at.lt(self.created_at))).execute(conn)
    }
}

#[cfg(test)]
mod tests {
    use super::{CleanupQuery, ListQuery, Status};
    use crate::test_helpers::{prelude::*, test_deps::LocalDeps};
    use chrono::{Duration, Utc};

    #[test]
    fn test_cleanup_query() {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = TestDb::with_local_postgres(&postgres);
        let old = TestAgent::new("web", "old_agent", USR_AUDIENCE);
        let new = TestAgent::new("web", "new_agent", USR_AUDIENCE);

        let room = db
            .connection_pool()
            .get()
            .map(|conn| {
                let room = shared_helpers::insert_room(&conn);
                factory::Agent::new()
                    .agent_id(old.agent_id())
                    .room_id(room.id())
                    .status(Status::Ready)
                    .created_at(Utc::now() - Duration::weeks(7))
                    .insert(&conn);
                factory::Agent::new()
                    .agent_id(new.agent_id())
                    .room_id(room.id())
                    .status(Status::Ready)
                    .insert(&conn);

                let r = ListQuery::new().room_id(room.id()).execute(&conn).unwrap();
                assert_eq!(r.len(), 2);
                room
            })
            .expect("Failed to insert room");

        let conn = db.connection_pool().get().unwrap();
        let r = CleanupQuery::new(Utc::now() - Duration::days(1))
            .execute(&conn)
            .expect("Failed to run query");

        assert_eq!(r, 1);
        let r = ListQuery::new().room_id(room.id()).execute(&conn).unwrap();
        assert_eq!(r.len(), 1);
    }
}
