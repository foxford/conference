use std::collections::HashMap;
use svc_agent::AgentId;
use tracing::warn;

use crate::db::group_agent::Groups;
use crate::db::{self, rtc::Id};

/// Creates/updates `rtc_reader_configs` based on `group_agents`.
///
/// Note: This function should be run within a database transaction.
pub async fn update(
    conn: &mut sqlx::PgConnection,
    room_id: db::room::Id,
    groups: Groups,
) -> sqlx::Result<HashMap<(Id, AgentId), bool>> {
    let agent_ids = groups.iter().flat_map(|g| g.agents()).collect::<Vec<_>>();

    let rtcs = db::rtc::ListQuery::new()
        .room_id(room_id)
        .created_by(&agent_ids)
        .execute(conn)
        .await?;

    let agent_rtcs = rtcs
        .iter()
        .map(|rtc| (rtc.created_by(), rtc.id()))
        .collect::<HashMap<_, _>>();

    // Use HashMap to avoid duplicated configs in cases
    // where a teacher can be in several groups at the same time
    let mut configs = HashMap::new();

    let group_agents = groups
        .iter()
        .flat_map(|g| g.agents().iter().map(move |a| (g.number(), a.clone())))
        .collect::<Vec<(_, _)>>();

    let agents_by_groups = groups
        .iter()
        .map(|g| (g.number(), g.agents()))
        .collect::<HashMap<_, _>>();

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

            // Checks the case where a teacher can be in several groups at the same time.
            // We don't want to create false configs in such cases for participants.
            // For example:
            // [
            //     "number": 1,
            //     "agents": [
            //       "web.Z2lkOi8vc3RvZWdlL1VzZXI6OlB1cGlsLzIyNDUxOTE=.usr.foxford.ru",
            //       "web.Z2lkOi8vc3RvZWdlL0FkbWluLzc1NA==.usr.foxford.ru"
            //     ]
            //   },
            //   {
            //     "number": 2,
            //     "agents": [
            //       "web.Z2lkOi8vc3RvZWdlL1VzZXI6OlB1cGlsLzIyNDUwOTE=.usr.foxford.ru",
            //       "web.Z2lkOi8vc3RvZWdlL0FkbWluLzc1NA==.usr.foxford.ru"
            //     ]
            //   }
            // ]
            if group1 != group2 {
                let group1_agents = agents_by_groups.get(group1);
                let group2_agents = agents_by_groups.get(group2);

                // If at least one of the agents is in another group, then we don't create false configs
                match group1_agents.and_then(|g1| {
                    group2_agents.map(|g2| (g1.contains(agent2), g2.contains(agent1)))
                }) {
                    Some((true, false)) | Some((false, true)) => {
                        continue;
                    }
                    _ => {}
                }
            }

            configs
                .entry((*rtc_id, agent1.to_owned()))
                .or_insert(group1 == group2);
        }
    }

    let (mut rtc_ids, mut agent_ids, mut receive_video, mut receive_audio) =
        (vec![], vec![], vec![], vec![]);

    for ((rtc_id, agent_id), value) in configs.iter() {
        rtc_ids.push(*rtc_id);
        agent_ids.push(agent_id);
        receive_video.push(*value);
        receive_audio.push(*value);
    }

    db::rtc_reader_config::batch_insert(conn, &rtc_ids, &agent_ids, &receive_video, &receive_audio)
        .await?;

    Ok(configs)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        db::rtc::SharingPolicy as RtcSharingPolicy,
        test_helpers::{
            db_sqlx, factory,
            prelude::{TestAgent, TestDb},
            test_deps::LocalDeps,
            USR_AUDIENCE,
        },
    };

    use crate::db::group_agent::GroupItem;
    use chrono::Utc;
    use std::ops::Bound;
    use svc_agent::Addressable;

    #[tokio::test]
    async fn distribution_by_groups() {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = TestDb::with_local_postgres(&postgres);
        let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

        let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
        let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
        let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
        let agent4 = TestAgent::new("web", "user4", USR_AUDIENCE);
        let agent5 = TestAgent::new("web", "user5", USR_AUDIENCE);

        let agents = vec![&agent1, &agent2, &agent3, &agent4, &agent5]
            .into_iter()
            .map(|a| a.agent_id().to_owned())
            .collect::<Vec<_>>();

        let conn = db.get_conn();
        let mut conn_sqlx = db_sqlx.get_conn().await;

        let room = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((Bound::Included(Utc::now()), Bound::Unbounded))
            .rtc_sharing_policy(RtcSharingPolicy::Owned)
            .insert(&conn);

        let groups = Groups::new(vec![
            GroupItem::new(
                0,
                vec![
                    agents[0].as_agent_id().clone(),
                    agents[4].as_agent_id().clone(),
                ],
            ),
            GroupItem::new(
                1,
                vec![
                    agents[1].as_agent_id().clone(),
                    agents[3].as_agent_id().clone(),
                    agents[4].as_agent_id().clone(),
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

        factory::GroupAgent::new(room.id(), groups.clone())
            .upsert(&mut conn_sqlx)
            .await;

        let rtcs = agents
            .iter()
            .map(|agent| {
                let rtc = factory::Rtc::new(room.id())
                    .created_by(agent.to_owned())
                    .insert(&conn);

                (rtc.id(), rtc.created_by().to_owned())
            })
            .collect::<HashMap<_, _>>();

        // First distribution by groups
        let _ = update(&mut conn_sqlx, room.id(), groups)
            .await
            .expect("group reader config update failed");

        let agents = agents.iter().map(|a| a).collect::<Vec<_>>();
        let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(room.id(), &agents)
            .execute(&mut conn_sqlx)
            .await
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

        let agent1_agent3_cfg = agent1_configs.get(agent3.agent_id()).unwrap();
        assert!(!agent1_agent3_cfg.receive_video());

        let agent3_configs = reader_configs
            .iter()
            .filter(|(cfg, _)| cfg.reader_id() == agent3.agent_id())
            .map(|(cfg, _)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
            .collect::<HashMap<_, _>>();

        assert_eq!(agent3_configs.len(), 4);

        let agent3_agent1_cfg = agent3_configs.get(agent1.agent_id()).unwrap();
        assert!(!agent3_agent1_cfg.receive_video());

        let agent3_agent2_cfg = agent3_configs.get(agent2.agent_id()).unwrap();
        assert!(!agent3_agent2_cfg.receive_video());

        // Checks the case where a teacher can be in several groups at the same time
        let agent5_configs = reader_configs
            .iter()
            .filter(|(cfg, _)| cfg.reader_id() == agent5.agent_id())
            .map(|(cfg, _)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
            .collect::<HashMap<_, _>>();

        // The teacher can see all group participants
        for (_, cfg) in agent5_configs {
            assert!(cfg.receive_video());
        }

        // And everyone else sees the teacher
        let agent1_agent5_cfg = agent1_configs.get(agent5.agent_id()).unwrap();
        assert!(agent1_agent5_cfg.receive_video());

        let agent3_agent5_cfg = agent3_configs.get(agent5.agent_id()).unwrap();
        assert!(agent3_agent5_cfg.receive_video());

        // Move agent2 to the group 0
        let groups = Groups::new(vec![
            GroupItem::new(
                0,
                vec![
                    agent1.agent_id().clone(),
                    agent5.agent_id().clone(),
                    agent2.agent_id().clone(),
                ],
            ),
            GroupItem::new(
                1,
                vec![agent4.agent_id().clone(), agent5.agent_id().clone()],
            ),
            GroupItem::new(
                2,
                vec![agent3.agent_id().clone(), agent5.agent_id().clone()],
            ),
        ]);

        factory::GroupAgent::new(room.id(), groups.clone())
            .upsert(&mut conn_sqlx)
            .await;

        // Second distribution by groups
        let _ = update(&mut conn_sqlx, room.id(), groups)
            .await
            .expect("group reader config update failed");

        let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(room.id(), &agents)
            .execute(&mut conn_sqlx)
            .await
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

        let agent4_configs = reader_configs
            .iter()
            .filter(|(cfg, _)| cfg.reader_id() == agent4.agent_id())
            .map(|(cfg, _)| (rtcs.get(&cfg.rtc_id()).unwrap(), cfg))
            .collect::<HashMap<_, _>>();

        assert_eq!(agent4_configs.len(), 4);

        let agent4_agent2_cfg = agent4_configs.get(agent2.agent_id()).unwrap();
        assert!(!agent4_agent2_cfg.receive_video());
    }
}
