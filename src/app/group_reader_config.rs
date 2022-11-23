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
