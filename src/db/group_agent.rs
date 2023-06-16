use crate::db;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, slice::Iter};
use svc_agent::AgentId;

pub type Id = db::id::Id;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct Groups(Vec<GroupItem>);

impl Groups {
    pub fn new(items: Vec<GroupItem>) -> Self {
        Self(items)
    }

    pub fn filter_by_agent(self, agent_id: &AgentId) -> Groups {
        let items = self
            .0
            .into_iter()
            .filter(|i| i.agents.contains(agent_id))
            .collect();

        Groups(items)
    }

    pub fn is_agent_exist(&self, agent_id: &AgentId) -> bool {
        for item in &self.0 {
            if item.agents.contains(agent_id) {
                return true;
            }
        }

        false
    }

    // todo: think how to optimize it
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

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for Groups {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        match serde_json::to_value(self) {
            Ok(value) => value.encode_by_ref(buf),
            Err(_) => sqlx::encode::IsNull::Yes,
        }
    }
}

impl<'q> sqlx::Decode<'q, sqlx::Postgres> for Groups {
    fn decode(
        value: <sqlx::Postgres as sqlx::database::HasValueRef<'q>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let raw_value = serde_json::Value::decode(value)?;
        match serde_json::from_value::<Groups>(raw_value) {
            Ok(groups) => Ok(groups),
            _ => Err("failed to decode jsonb value as groups object"
                .to_owned()
                .into()),
        }
    }
}

impl sqlx::Type<sqlx::Postgres> for Groups {
    fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
        sqlx::types::JsonValue::type_info()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct GroupItem {
    number: i32,
    agents: Vec<AgentId>,
}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct Object {
    id: Id,
    room_id: db::room::Id,
    groups: Groups,
}

impl Object {
    pub fn groups(self) -> Groups {
        self.groups
    }
}

#[derive(Debug)]
pub struct UpsertQuery<'a> {
    room_id: db::room::Id,
    groups: &'a Groups,
}

impl<'a> UpsertQuery<'a> {
    pub fn new(room_id: db::room::Id, groups: &'a Groups) -> Self {
        Self { room_id, groups }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO group_agent (room_id, groups)
            VALUES ($1, $2)
            ON CONFLICT (room_id) DO UPDATE
            SET
                groups = EXCLUDED.groups
            RETURNING
                id as "id: Id",
                room_id as "room_id: Id",
                groups as "groups: Groups"
            "#,
            self.room_id as Id,
            self.groups as &Groups,
        )
        .fetch_one(conn)
        .await
    }
}

pub struct FindQuery {
    room_id: db::room::Id,
}

impl FindQuery {
    pub fn new(room_id: db::room::Id) -> Self {
        Self { room_id }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                id as "id: Id",
                room_id as "room_id: Id",
                groups as "groups: Groups"
            FROM group_agent
            WHERE
                room_id = $1
            FOR UPDATE
            "#,
            self.room_id as Id,
        )
        .fetch_one(conn)
        .await
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
                groups.filter_by_agent(agent2.agent_id()),
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

            assert_eq!(
                groups.filter_by_agent(agent3.agent_id()),
                Groups::new(vec![])
            );
        }

        #[test]
        fn is_agent_exist_test_succeed() {
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let groups = Groups::new(vec![
                GroupItem::new(0, vec![agent1.agent_id().clone()]),
                GroupItem::new(1, vec![agent2.agent_id().clone()]),
            ]);

            assert!(groups.is_agent_exist(agent2.agent_id()));
        }

        #[test]
        fn is_agent_exist_test_not_exists() {
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
            let groups = Groups::new(vec![
                GroupItem::new(0, vec![agent1.agent_id().clone()]),
                GroupItem::new(1, vec![agent2.agent_id().clone()]),
            ]);

            assert!(!groups.is_agent_exist(agent3.agent_id()));
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
