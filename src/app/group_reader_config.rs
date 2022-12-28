use crate::db::group_agent::Groups;
use crate::{
    app::error::Error,
    db::{self, rtc::Id, rtc_reader_config::UpsertQuery},
};
use diesel::PgConnection;
use std::collections::HashMap;
use svc_agent::AgentId;
use tracing::warn;

#[derive(Debug)]
pub struct GroupReaderConfig {
    pub agent_id: AgentId,
    pub rtc_id: Id,
    pub availability: bool,
}

/// Creates/updates `rtc_reader_configs` based on `group_agents`.
///
/// Note: This function should be run within a database transaction.
pub fn update(
    conn: &PgConnection,
    room_id: db::room::Id,
    groups: Groups,
) -> Result<Vec<GroupReaderConfig>, Error> {
    let agent_ids = groups.iter().flat_map(|g| g.agents()).collect::<Vec<_>>();

    let rtcs = db::rtc::ListQuery::new()
        .room_id(room_id)
        .created_by(&agent_ids)
        .execute(conn)?;

    let agent_rtcs = rtcs
        .iter()
        .map(|rtc| (rtc.created_by(), rtc.id()))
        .collect::<HashMap<_, _>>();

    let mut configs = Vec::new();

    let group_agents = groups
        .iter()
        .flat_map(|g| g.agents().iter().map(move |a| (g.number(), a.clone())))
        .collect::<Vec<(_, _)>>();

    // Generates cross-configs for agents: agent1 <-> agent2
    for (group1, agent1) in &group_agents {
        for (group2, agent2) in &group_agents {
            if agent1 == agent2 {
                continue;
            }

            let rtc_id = match agent_rtcs.get(agent2) {
                None => {
                    warn!(%agent2, "rtc_id not found");
                    continue;
                }
                Some(rtc_id) => rtc_id,
            };

            configs.push(GroupReaderConfig {
                agent_id: agent1.to_owned(),
                rtc_id: *rtc_id,
                availability: (group1 == group2),
            });
        }
    }

    let mut upsert_queries = configs
        .iter()
        .map(|cfg| {
            UpsertQuery::new(cfg.rtc_id, &cfg.agent_id)
                .receive_video(cfg.availability)
                .receive_audio(cfg.availability)
        })
        .collect::<Vec<UpsertQuery>>();

    upsert_queries.dedup();
    db::rtc_reader_config::batch_insert(conn, &upsert_queries)?;

    Ok(configs)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        db::rtc::SharingPolicy as RtcSharingPolicy,
        test_helpers::{
            factory,
            prelude::{TestAgent, TestDb},
            test_deps::LocalDeps,
            USR_AUDIENCE,
        },
    };

    use crate::db::group_agent::GroupItem;
    use chrono::Utc;
    use std::ops::Bound;
    use svc_agent::Addressable;

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

                let groups = Groups::new(vec![
                    GroupItem::new(0, vec![agents[0].as_agent_id().clone()]),
                    GroupItem::new(
                        1,
                        vec![
                            agents[1].as_agent_id().clone(),
                            agents[3].as_agent_id().clone(),
                        ],
                    ),
                    GroupItem::new(
                        2,
                        vec![
                            agents[2].as_agent_id().clone(),
                            agents[4].as_agent_id().clone(),
                        ],
                    ),
                ]);

                factory::GroupAgent::new(room.id(), groups.clone()).upsert(&conn);

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
        let _ = update(&conn, room.id(), groups).expect("group reader config update failed");

        let agents = agents.iter().map(|a| a).collect::<Vec<_>>();
        let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(room.id(), &agents)
            .execute(&conn)
            .unwrap();

        assert_eq!(reader_configs.len(), 20);

        let agent1_configs = reader_configs
            .iter()
            .filter(|(cfg, _)| cfg.reader_id() == agent1.agent_id())
            .map(|(cfg, _)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
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
            .filter(|(cfg, _)| cfg.reader_id() == agent3.agent_id())
            .map(|(cfg, _)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
            .collect::<HashMap<_, _>>();

        assert_eq!(agent3_configs.len(), 4);

        let agent3_agent1_cfg = agent3_configs.get(agent1.agent_id()).unwrap();
        assert!(!agent3_agent1_cfg.receive_video());
        assert!(!agent3_agent1_cfg.receive_audio());

        let agent3_agent2_cfg = agent3_configs.get(agent2.agent_id()).unwrap();
        assert!(!agent3_agent2_cfg.receive_video());
        assert!(!agent3_agent2_cfg.receive_audio());

        // Move agent2 to the group 0
        let groups = Groups::new(vec![
            GroupItem::new(
                0,
                vec![agent1.agent_id().clone(), agent2.agent_id().clone()],
            ),
            GroupItem::new(1, vec![agent4.agent_id().clone()]),
            GroupItem::new(
                2,
                vec![agent3.agent_id().clone(), agent5.agent_id().clone()],
            ),
        ]);

        factory::GroupAgent::new(room.id(), groups.clone()).upsert(&conn);

        // Second distribution by groups
        let conn = db.connection_pool().get().unwrap();
        let _ = update(&conn, room.id(), groups).expect("group reader config update failed");

        let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(room.id(), &agents)
            .execute(&conn)
            .unwrap();

        assert_eq!(reader_configs.len(), 20);

        let agent1_configs = reader_configs
            .iter()
            .filter(|(cfg, _)| cfg.reader_id() == agent1.agent_id())
            .map(|(cfg, _)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
            .collect::<HashMap<_, _>>();

        assert_eq!(agent1_configs.len(), 4);

        let agent1_agent2_cfg = agent1_configs.get(agent2.agent_id()).unwrap();
        assert!(agent1_agent2_cfg.receive_video());
        assert!(agent1_agent2_cfg.receive_audio());

        let agent4_configs = reader_configs
            .iter()
            .filter(|(cfg, _)| cfg.reader_id() == agent4.agent_id())
            .map(|(cfg, _)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
            .collect::<HashMap<_, _>>();

        assert_eq!(agent4_configs.len(), 4);

        let agent4_agent2_cfg = agent4_configs.get(agent2.agent_id()).unwrap();
        assert!(!agent4_agent2_cfg.receive_video());
        assert!(!agent4_agent2_cfg.receive_audio());
    }
}
