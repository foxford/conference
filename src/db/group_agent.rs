use super::agent::Object as Agent;
use super::group::Object as Group;
use crate::db;
use crate::schema::{group, group_agent};
use diesel::{pg::PgConnection, result::Error};
use serde::{Deserialize, Serialize};

pub type Id = db::id::Id;

type AllColumns = (
    group_agent::id,
    group_agent::group_id,
    group_agent::agent_id,
);

const ALL_COLUMNS: AllColumns = (
    group_agent::id,
    group_agent::group_id,
    group_agent::agent_id,
);

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Group, foreign_key = "group_id")]
#[belongs_to(Agent, foreign_key = "agent_id")]
#[table_name = "group_agent"]
pub struct Object {
    id: Id,
    group_id: db::group::Id,
    agent_id: db::agent::Id,
}

#[derive(Debug, Insertable)]
#[table_name = "group_agent"]
pub struct InsertQuery {
    group_id: db::group::Id,
    agent_id: db::agent::Id,
}

impl InsertQuery {
    pub fn new(group_id: db::group::Id, agent_id: db::agent::Id) -> Self {
        Self { group_id, agent_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::group_agent::dsl::*;
        use diesel::RunQueryDsl;

        diesel::insert_into(group_agent)
            .values(self)
            .on_conflict_do_nothing()
            .get_result(conn)
    }
}

pub struct FindQuery {
    room_id: db::room::Id,
    agent_id: db::agent::Id,
}

impl FindQuery {
    pub fn new(room_id: db::room::Id, agent_id: db::agent::Id) -> Self {
        Self { room_id, agent_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        group_agent::table
            .inner_join(group::table)
            .filter(group::room_id.eq(self.room_id))
            .filter(group_agent::agent_id.eq(self.agent_id))
            .select(ALL_COLUMNS)
            .get_result(conn)
            .optional()
    }
}
