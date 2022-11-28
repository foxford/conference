use crate::app::error::Error;
use crate::db;
use crate::db::rtc::Id;
use crate::db::rtc_reader_config::UpsertQuery;
use diesel::PgConnection;
use std::collections::HashMap;
use svc_agent::AgentId;

#[derive(Debug)]
pub struct GroupReaderConfig {
    pub agent_id: AgentId,
    pub rtc_id: Id,
    pub availability: bool,
}

/// Creates/updates `rtc_reader_configs` based on `group_agents`
pub fn update(conn: &PgConnection, room_id: db::room::Id) -> Result<Vec<GroupReaderConfig>, Error> {
    let group_agents = db::group_agent::ListWithGroupQuery::new(room_id).execute(conn)?;

    let agent_ids = group_agents
        .iter()
        .map(|g| &g.agent_id)
        .collect::<Vec<&AgentId>>();

    let rtcs = db::rtc::ListQuery::new()
        .room_id(room_id)
        .created_by(agent_ids.as_slice())
        .execute(conn)?;

    let agents_to_rtcs = rtcs
        .iter()
        .map(|rtc| (rtc.created_by(), rtc.id()))
        .collect::<HashMap<_, _>>();

    let mut all_configs = Vec::new();
    let mut true_configs = Vec::new();

    // Generates cross-configs for agents: agent1 <-> agent2
    for group_agent1 in &group_agents {
        for group_agent2 in &group_agents {
            if group_agent1 == group_agent2 {
                continue;
            }

            let rtc_id = match agents_to_rtcs.get(&group_agent2.agent_id) {
                None => continue,
                Some(rtc_id) => rtc_id,
            };
            let agent_id = group_agent1.agent_id.to_owned();

            if group_agent1.number != group_agent2.number {
                let cfg = GroupReaderConfig {
                    agent_id,
                    rtc_id: *rtc_id,
                    availability: false,
                };

                all_configs.push(cfg);
            } else {
                let cfg = GroupReaderConfig {
                    agent_id,
                    rtc_id: *rtc_id,
                    availability: true,
                };

                true_configs.push(cfg);
            };
        }
    }

    let reader_configs_with_rtcs =
        db::rtc_reader_config::ListWithRtcQuery::new(room_id, &agent_ids)
            .execute(conn)?
            .iter()
            .filter(|(cfg, _)| !cfg.receive_video() && !cfg.receive_audio())
            .map(|(cfg, rtc)| (rtc.id(), cfg.reader_id().to_owned()))
            .collect::<Vec<(Id, AgentId)>>();

    for cfg in true_configs {
        if reader_configs_with_rtcs.contains(&(cfg.rtc_id.to_owned(), cfg.agent_id.to_owned())) {
            all_configs.push(cfg);
        }
    }

    let configs = all_configs
        .iter()
        .map(|cfg| {
            UpsertQuery::new(cfg.rtc_id, &cfg.agent_id)
                .receive_video(cfg.availability)
                .receive_audio(cfg.availability)
        })
        .collect::<Vec<UpsertQuery>>();

    db::rtc_reader_config::batch_insert(conn, &configs)?;

    Ok(all_configs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::rtc::SharingPolicy as RtcSharingPolicy;
    use crate::test_helpers::prelude::{TestAgent, TestDb};
    use crate::test_helpers::test_deps::LocalDeps;
    use crate::test_helpers::{factory, USR_AUDIENCE};
    use chrono::Utc;
    use diesel::Identifiable;
    use std::ops::Bound;

    #[test]
    fn distribution_by_groups() {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = TestDb::with_local_postgres(&postgres);
        let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
        let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
        let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
        let agent4 = TestAgent::new("web", "user4", USR_AUDIENCE);
        let agent5 = TestAgent::new("web", "user5", USR_AUDIENCE);

        let agents = vec![&agent1, &agent2, &agent3, &agent4, &agent5]
            .into_iter()
            .map(|a| a.agent_id().to_owned())
            .collect::<Vec<_>>();

        let (room, rtcs, groups) = db
            .connection_pool()
            .get()
            .map(|conn| {
                let room = factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Included(Utc::now()), Bound::Unbounded))
                    .rtc_sharing_policy(RtcSharingPolicy::Owned)
                    .insert(&conn);

                let group0 = factory::Group::new(room.id()).insert(&conn);
                let group1 = factory::Group::new(room.id()).number(1).insert(&conn);
                let group2 = factory::Group::new(room.id()).number(2).insert(&conn);

                factory::GroupAgent::new(*group0.id())
                    .agent_id(&agents[0])
                    .insert(&conn);
                factory::GroupAgent::new(*group1.id())
                    .agent_id(&agents[1])
                    .insert(&conn);
                factory::GroupAgent::new(*group2.id())
                    .agent_id(&agents[2])
                    .insert(&conn);
                factory::GroupAgent::new(*group1.id())
                    .agent_id(&agents[3])
                    .insert(&conn);
                factory::GroupAgent::new(*group2.id())
                    .agent_id(&agents[4])
                    .insert(&conn);

                let groups = vec![*group0.id(), *group1.id(), *group2.id()];

                let rtcs = agents
                    .iter()
                    .map(|agent| {
                        let rtc = factory::Rtc::new(room.id())
                            .created_by(agent.to_owned())
                            .insert(&conn);

                        (rtc.id(), rtc.created_by().to_owned())
                    })
                    .collect::<HashMap<_, _>>();

                (room, rtcs, groups)
            })
            .unwrap();

        // First distribution by groups
        let conn = db.connection_pool().get().unwrap();
        let _ = update(&conn, room.id()).expect("group reader config update failed");

        let agents = agents.iter().map(|a| a).collect::<Vec<_>>();
        let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(room.id(), &agents)
            .execute(&conn)
            .unwrap();

        assert_eq!(reader_configs.len(), 16);

        let agent1_configs = reader_configs
            .iter()
            .filter(|(cfg, rtc)| cfg.reader_id() == agent1.agent_id())
            .map(|(cfg, rtc)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
            .collect::<HashMap<_, _>>();

        assert_eq!(agent1_configs.len(), 4);

        let agent1_agent2_cfg = agent1_configs.get(agent2.agent_id()).unwrap();
        assert!(!agent1_agent2_cfg.receive_video());
        assert!(!agent1_agent2_cfg.receive_audio());

        let agent1_agent3_cfg = agent1_configs.get(agent3.agent_id()).unwrap();
        assert!(!agent1_agent3_cfg.receive_video());
        assert!(!agent1_agent3_cfg.receive_audio());

        let agent3_configs = reader_configs
            .iter()
            .filter(|(cfg, rtc)| cfg.reader_id() == agent3.agent_id())
            .map(|(cfg, rtc)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
            .collect::<HashMap<_, _>>();

        assert_eq!(agent3_configs.len(), 3);

        let agent3_agent1_cfg = agent3_configs.get(agent1.agent_id()).unwrap();
        assert!(!agent3_agent1_cfg.receive_video());
        assert!(!agent3_agent1_cfg.receive_audio());

        let agent3_agent2_cfg = agent3_configs.get(agent2.agent_id()).unwrap();
        assert!(!agent3_agent2_cfg.receive_video());
        assert!(!agent3_agent2_cfg.receive_audio());

        let group_agent2 = db::group_agent::FindQuery::new(room.id(), agent2.agent_id())
            .execute(&conn)
            .unwrap()
            .unwrap();

        // Move agent2 to the group 0
        factory::GroupAgent::new(groups[0])
            .id(*group_agent2.id())
            .update(&conn);

        // Second distribution by groups
        let conn = db.connection_pool().get().unwrap();
        let _ = update(&conn, room.id()).expect("group reader config update failed");

        let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(room.id(), &agents)
            .execute(&conn)
            .unwrap();

        assert_eq!(reader_configs.len(), 18);

        let agent1_configs = reader_configs
            .iter()
            .filter(|(cfg, rtc)| cfg.reader_id() == agent1.agent_id())
            .map(|(cfg, rtc)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
            .collect::<HashMap<_, _>>();

        assert_eq!(agent1_configs.len(), 4);

        let agent1_agent2_cfg = agent1_configs.get(agent2.agent_id()).unwrap();
        assert!(agent1_agent2_cfg.receive_video());
        assert!(agent1_agent2_cfg.receive_audio());

        let agent4_configs = reader_configs
            .iter()
            .filter(|(cfg, rtc)| cfg.reader_id() == agent4.agent_id())
            .map(|(cfg, rtc)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
            .collect::<HashMap<_, _>>();

        assert_eq!(agent4_configs.len(), 4);

        let agent4_agent2_cfg = agent4_configs.get(agent2.agent_id()).unwrap();
        assert!(!agent4_agent2_cfg.receive_video());
        assert!(!agent4_agent2_cfg.receive_audio());
    }
}
