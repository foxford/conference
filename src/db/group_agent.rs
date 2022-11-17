use super::agent::Object as Agent;
use super::group::Object as Group;
use crate::db;
use crate::schema::{group, group_agent};
use diesel::{pg::PgConnection, result::Error};
use serde::{Deserialize, Serialize};
use svc_agent::AgentId;

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
    agent_id: AgentId,
}

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "group_agent"]
pub struct InsertQuery<'a> {
    group_id: db::group::Id,
    agent_id: &'a AgentId,
}

impl<'a> InsertQuery<'a> {
    pub fn new(group_id: db::group::Id, agent_id: &'a AgentId) -> Self {
        Self { group_id, agent_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::group_agent::dsl::*;
        use diesel::RunQueryDsl;

        diesel::insert_into(group_agent)
            .values(self)
            .on_conflict((group_id, agent_id))
            .do_update()
            .set(self)
            .get_result(conn)
    }
}

pub struct FindQuery<'a> {
    room_id: db::room::Id,
    agent_id: &'a AgentId,
}

impl<'a> FindQuery<'a> {
    pub fn new(room_id: db::room::Id, agent_id: &'a AgentId) -> Self {
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

pub struct ListWithGroupQuery {
    room_id: db::room::Id,
    agent_id: Option<AgentId>,
}

#[derive(QueryableByName)]
pub struct GroupAgent {
    #[sql_type = "diesel::sql_types::Integer"]
    pub number: i32,
    #[sql_type = "svc_agent::sql::Agent_id"]
    pub agent_id: AgentId,
}

const GROUP_AGENT_SQL: &'static str = r#"
    select distinct
            g.number,
            ga.agent_id
    from group_agent ga
         join "group" g on g.id = ga.group_id
         join (
             select
                 group_id,
                 agent_id
             from group_agent
         ) ga2 on ga2.group_id = ga.group_id
         where g.room_id = $1
    "#;

impl ListWithGroupQuery {
    pub fn new(room_id: db::room::Id) -> Self {
        Self {
            room_id,
            agent_id: None,
        }
    }

    pub fn within_group(self, agent_id: AgentId) -> Self {
        Self {
            agent_id: Some(agent_id),
            ..self
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Vec<GroupAgent>, Error> {
        use crate::db::sql::Agent_id;
        use diesel::{prelude::*, sql_types::Uuid};

        if let Some(agent_id) = &self.agent_id {
            let sql = format!("{} and ga2.agent_id = $2", GROUP_AGENT_SQL);

            diesel::sql_query(sql)
                .bind::<Uuid, _>(self.room_id)
                .bind::<Agent_id, _>(agent_id)
                .get_results(conn)
        } else {
            diesel::sql_query(GROUP_AGENT_SQL)
                .bind::<Uuid, _>(self.room_id)
                .get_results(conn)
        }
    }
}
