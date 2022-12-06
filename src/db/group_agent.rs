use super::room::Object as Room;
use crate::{db, schema::group_agent};
use diesel::{pg::Pg, result::Error, sql_types::Jsonb, PgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, slice::Iter};
use svc_agent::AgentId;

pub type Id = db::id::Id;

type AllColumns = (group_agent::id, group_agent::room_id, group_agent::groups);

const ALL_COLUMNS: AllColumns = (group_agent::id, group_agent::room_id, group_agent::groups);

#[derive(Clone, Debug, Deserialize, Serialize, FromSqlRow, AsExpression, Eq, PartialEq)]
#[sql_type = "Jsonb"]
pub struct Groups(Vec<GroupItem>);
impl_jsonb!(Groups);

impl Groups {
    pub fn new(items: Vec<GroupItem>) -> Self {
        Self { 0: items }
    }

    pub fn filter(self, agent_id: &AgentId) -> Groups {
        let items = self
            .0
            .into_iter()
            .filter(|i| i.agents.contains(agent_id))
            .collect();

        Groups { 0: items }
    }

    pub fn exist(&self, agent_id: &AgentId) -> bool {
        for item in &self.0 {
            if item.agents.contains(agent_id) {
                return true;
            }
        }

        false
    }

    // todo: think how to optimize
    pub fn add_to_default_group(&self, agent_id: &AgentId) -> Self {
        let mut groups = self
            .0
            .iter()
            .map(|i| (i.number, i.agents.clone()))
            .collect::<HashMap<_, _>>();

        if let Some(agents) = groups.get_mut(&0) {
            agents.push(agent_id.clone())
        }

        let g = groups
            .into_iter()
            .map(|(k, v)| GroupItem {
                number: k,
                agents: v,
            })
            .collect::<Vec<GroupItem>>();

        Self(g)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> Iter<'_, GroupItem> {
        self.0.iter()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, FromSqlRow, AsExpression, Eq, PartialEq)]
#[sql_type = "Jsonb"]
pub struct GroupItem {
    number: i32,
    agents: Vec<AgentId>,
}
impl_jsonb!(GroupItem);

impl GroupItem {
    pub fn new(number: i32, agents: Vec<AgentId>) -> Self {
        Self { number, agents }
    }

    pub fn number(&self) -> i32 {
        self.number
    }

    pub fn agents(&self) -> &[AgentId] {
        self.agents.as_slice()
    }
}

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Room, foreign_key = "room_id")]
#[table_name = "group_agent"]
pub struct Object {
    id: Id,
    room_id: db::room::Id,
    groups: Groups,
}

impl Object {
    // todo: fix clone
    pub fn groups(&self) -> Groups {
        self.groups.clone()
    }
}

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "group_agent"]
pub struct UpsertQuery {
    room_id: db::room::Id,
    groups: Groups,
}

impl UpsertQuery {
    pub fn new(room_id: db::room::Id) -> Self {
        let groups = Groups::new(vec![GroupItem::new(0, vec![])]);

        Self { room_id, groups }
    }

    pub fn groups(self, groups: Groups) -> Self {
        Self { groups, ..self }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::diesel::pg::upsert::excluded;
        use crate::diesel::ExpressionMethods;
        use crate::schema::group_agent::dsl::*;

        diesel::insert_into(group_agent)
            .values(self)
            .on_conflict(room_id)
            .do_update()
            .set(groups.eq(excluded(groups)))
            .get_result(conn)
    }
}

pub struct FindQuery {
    room_id: db::room::Id,
}

impl FindQuery {
    pub fn new(room_id: db::room::Id) -> Self {
        Self { room_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        group_agent::table
            .for_update()
            .filter(group_agent::room_id.eq(self.room_id))
            .select(ALL_COLUMNS)
            .get_result(conn)
    }
}

#[cfg(test)]
mod tests {
    mod groups {
        use super::super::*;
        use crate::test_helpers::{agent::TestAgent, USR_AUDIENCE};

        #[test]
        fn filter_test_succeed() {
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
            let groups = Groups::new(vec![
                GroupItem::new(0, vec![agent1.agent_id().clone()]),
                GroupItem::new(
                    1,
                    vec![agent2.agent_id().clone(), agent3.agent_id().clone()],
                ),
            ]);

            assert_eq!(
                groups.filter(agent2.agent_id()),
                Groups::new(vec![GroupItem::new(
                    1,
                    vec![agent2.agent_id().clone(), agent3.agent_id().clone()],
                )])
            )
        }

        #[test]
        fn filter_test_not_exists() {
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
            let groups = Groups::new(vec![
                GroupItem::new(0, vec![agent1.agent_id().clone()]),
                GroupItem::new(1, vec![agent2.agent_id().clone()]),
            ]);

            assert_eq!(groups.filter(agent3.agent_id()), Groups::new(vec![]));
        }

        #[test]
        fn exist_test_succeed() {
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let groups = Groups::new(vec![
                GroupItem::new(0, vec![agent1.agent_id().clone()]),
                GroupItem::new(1, vec![agent2.agent_id().clone()]),
            ]);

            assert!(groups.exist(agent2.agent_id()));
        }

        #[test]
        fn exist_test_not_exists() {
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
            let groups = Groups::new(vec![
                GroupItem::new(0, vec![agent1.agent_id().clone()]),
                GroupItem::new(1, vec![agent2.agent_id().clone()]),
            ]);

            assert!(!groups.exist(agent3.agent_id()));
        }

        #[test]
        fn add_to_default_group_test() {
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
            let groups = Groups::new(vec![
                GroupItem::new(0, vec![agent1.agent_id().clone()]),
                GroupItem::new(1, vec![agent2.agent_id().clone()]),
            ]);

            let changed_groups = groups.add_to_default_group(agent3.agent_id());

            let groups = changed_groups
                .iter()
                .map(|i| (i.number, i.agents.clone()))
                .collect::<HashMap<_, _>>();

            let group0 = groups.get(&0).unwrap();
            assert_eq!(
                group0,
                &vec![agent1.agent_id().clone(), agent3.agent_id().clone()]
            );
        }
    }
}
