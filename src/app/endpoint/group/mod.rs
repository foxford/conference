use crate::db::group_agent::GroupAgent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use svc_agent::AgentId;

pub use list::list;
pub use update::update;

mod list;
mod update;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct State(Vec<StateItem>);

impl State {
    fn new(groups: &[GroupAgent]) -> Self {
        let groups = groups
            .iter()
            .fold(HashMap::new(), |mut map, ga| {
                map.entry(ga.number)
                    .or_insert_with(Vec::new)
                    .push(ga.agent_id.to_owned());
                map
            })
            .into_iter()
            .map(|(number, agents)| StateItem {
                number: number.to_owned(),
                agents,
            })
            .collect();

        Self(groups)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct StateItem {
    number: i32,
    agents: Vec<AgentId>,
}
