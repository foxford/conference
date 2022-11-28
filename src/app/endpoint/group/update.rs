use crate::app::context::{AppContext, Context};
use crate::app::endpoint::group::StateItem;
use crate::app::endpoint::prelude::{AppError, AppErrorKind};
use crate::app::endpoint::{helpers, RequestHandler, RequestResult};
use crate::app::error::ErrorExt;
use crate::app::group_reader_config;
use crate::app::service_utils::{RequestParams, Response};
use crate::backend::janus::client::update_agent_reader_config::{
    UpdateReaderConfigRequest, UpdateReaderConfigRequestBody,
    UpdateReaderConfigRequestBodyConfigItem,
};
use crate::db;
use anyhow::{anyhow, Context as AnyhowContext};
use async_trait::async_trait;
use axum::extract::Path;
use axum::{Extension, Json};
use diesel::{Connection, Identifiable};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use svc_agent::mqtt::ResponseStatus;
use svc_utils::extractors::AgentIdExtractor;

#[derive(Deserialize)]
pub struct Payload {
    room_id: db::room::Id,
    groups: Vec<StateItem>,
}

pub async fn update(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    Json(groups): Json<Vec<StateItem>>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = Payload { room_id, groups };

    Handler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct Handler;

#[async_trait]
impl RequestHandler for Handler {
    type Payload = Payload;
    const ERROR_TITLE: &'static str = "Failed to update groups";

    // TODO: Add tests
    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        _: RequestParams<'_>,
    ) -> RequestResult {
        let conn = context.get_conn().await?;

        let (room, configs, maybe_backend) = crate::util::spawn_blocking({
            let group_agents = payload.groups;
            let room = helpers::find_room_by_id(
                payload.room_id,
                helpers::RoomTimeRequirement::NotClosed,
                &conn,
            )?;

            if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
                return Err(anyhow!(
                    "Updating groups is only available for rooms with owned RTC sharing policy"
                ))
                .error(AppErrorKind::InvalidPayload)?;
            }

            move || {
                let configs = conn.transaction(|| {
                    // Deletes all groups and groups agents in the room
                    db::group::DeleteQuery::new(room.id()).execute(&conn)?;

                    // Creates groups
                    let numbers = group_agents.iter().map(|g| g.number).collect::<Vec<i32>>();
                    let groups = db::group::batch_insert(&conn, room.id(), numbers)?
                        .iter()
                        .map(|g| (g.number(), *g.id()))
                        .collect::<HashMap<_, _>>();

                    let agents = group_agents.into_iter().fold(Vec::new(), |mut acc, g| {
                        if let Some(group_id) = groups.get(&g.number) {
                            acc.push((*group_id, g.agents));
                        }

                        acc
                    });

                    // Creates group agents
                    db::group_agent::batch_insert(&conn, agents)?;

                    // Update rtc_reader_configs
                    let configs = group_reader_config::update(&conn, room.id())?;

                    Ok::<_, AppError>(configs)
                })?;

                // Find backend and send updates to it if present.
                let maybe_backend = match room.backend_id() {
                    None => None,
                    Some(backend_id) => db::janus_backend::FindQuery::new()
                        .id(backend_id)
                        .execute(&conn)?,
                };

                Ok::<_, AppError>((room, configs, maybe_backend))
            }
        })
        .await?;

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // TODO: Need refactoring
        if let Some(backend) = maybe_backend {
            let items = configs
                .iter()
                .map(|cfg| UpdateReaderConfigRequestBodyConfigItem {
                    reader_id: cfg.agent_id.to_owned(),
                    stream_id: cfg.rtc_id,
                    receive_video: cfg.availability,
                    receive_audio: cfg.availability,
                })
                .collect();

            let request = UpdateReaderConfigRequest {
                session_id: backend.session_id(),
                handle_id: backend.handle_id(),
                body: UpdateReaderConfigRequestBody::new(items),
            };
            context
                .janus_clients()
                .get_or_insert(&backend)
                .error(AppErrorKind::BackendClientCreationFailed)?
                .reader_update(request)
                .await
                .context("Reader update")
                .error(AppErrorKind::BackendRequestFailed)?
        }

        let mut response = Response::new(
            ResponseStatus::OK,
            json!({}),
            context.start_timestamp(),
            None,
        );

        response.add_notification(
            "group.update",
            &format!("rooms/{}/events", room.id()),
            json!({}),
            context.start_timestamp(),
        );

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::rtc::SharingPolicy as RtcSharingPolicy;
    use crate::test_helpers::prelude::{GlobalContext, TestAgent, TestAuthz, TestContext, TestDb};
    use crate::test_helpers::test_deps::LocalDeps;
    use crate::test_helpers::{factory, handle_request, shared_helpers, USR_AUDIENCE};
    use chrono::{Duration, Utc};
    use std::ops::Bound;

    #[tokio::test]
    async fn missing_room() -> std::io::Result<()> {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = TestDb::with_local_postgres(&postgres);
        let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
        let mut context = TestContext::new(db, TestAuthz::new());

        let payload = Payload {
            room_id: db::room::Id::random(),
            groups: vec![],
        };

        // Assert error.
        let err = handle_request::<Handler>(&mut context, &agent, payload)
            .await
            .expect_err("Unexpected group list success");

        assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
        assert_eq!(err.kind(), "room_not_found");
        Ok(())
    }

    #[tokio::test]
    async fn closed_room() -> std::io::Result<()> {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = TestDb::with_local_postgres(&postgres);
        let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

        let room = db
            .connection_pool()
            .get()
            .map(|conn| {
                let room = factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((
                        Bound::Included(Utc::now() - Duration::hours(2)),
                        Bound::Excluded(Utc::now() - Duration::hours(1)),
                    ))
                    .rtc_sharing_policy(RtcSharingPolicy::Owned)
                    .insert(&conn);

                shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());

                room
            })
            .unwrap();

        let mut context = TestContext::new(db, TestAuthz::new());

        let payload = Payload {
            room_id: room.id(),
            groups: vec![],
        };

        // Assert error.
        let err = handle_request::<Handler>(&mut context, &agent, payload)
            .await
            .expect_err("Unexpected agent reader config read success");

        assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
        assert_eq!(err.kind(), "room_closed");
        Ok(())
    }

    #[tokio::test]
    async fn wrong_rtc_sharing_policy() {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = TestDb::with_local_postgres(&postgres);
        let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);

        let room = db
            .connection_pool()
            .get()
            .map(|conn| {
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Included(Utc::now()), Bound::Unbounded))
                    .rtc_sharing_policy(RtcSharingPolicy::Shared)
                    .insert(&conn)
            })
            .unwrap();

        let mut context = TestContext::new(db, TestAuthz::new());
        let payload = Payload {
            room_id: room.id(),
            groups: vec![],
        };

        // Assert error.
        let err = handle_request::<Handler>(&mut context, &agent1, payload)
            .await
            .expect_err("Unexpected group list success");

        assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
        assert_eq!(err.kind(), "invalid_payload");
    }

    #[tokio::test]
    async fn update_group_agents() {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let janus = local_deps.run_janus();
        let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
        let db = TestDb::with_local_postgres(&postgres);
        let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
        let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);

        let (room, backend) = db
            .connection_pool()
            .get()
            .map(|conn| {
                let backend =
                    shared_helpers::insert_janus_backend(&conn, &janus.url, session_id, handle_id);

                let room = factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Included(Utc::now()), Bound::Unbounded))
                    .rtc_sharing_policy(RtcSharingPolicy::Owned)
                    .backend_id(backend.id())
                    .insert(&conn);

                factory::Group::new(room.id()).insert(&conn);
                factory::Group::new(room.id()).number(1).insert(&conn);

                vec![&agent1, &agent2].iter().for_each(|agent| {
                    factory::Rtc::new(room.id())
                        .created_by(agent.agent_id().to_owned())
                        .insert(&conn);
                });

                (room, backend)
            })
            .unwrap();

        let mut context = TestContext::new(db.clone(), TestAuthz::new());
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        context.with_janus(tx);

        let payload = Payload {
            room_id: room.id(),
            groups: vec![
                StateItem {
                    number: 0,
                    agents: vec![agent1.agent_id().to_owned()],
                },
                StateItem {
                    number: 1,
                    agents: vec![agent2.agent_id().to_owned()],
                },
            ],
        };

        handle_request::<Handler>(&mut context, &agent1, payload)
            .await
            .expect("Group update failed");

        let conn = db
            .connection_pool()
            .get()
            .expect("Failed to get DB connection");

        let group_agents = db::group_agent::ListWithGroupQuery::new(room.id())
            .execute(&conn)
            .expect("failed to get groups with participants");

        assert_eq!(group_agents.len(), 2);

        let group_agents = group_agents
            .iter()
            .map(|ga| (ga.agent_id.to_owned(), ga.number))
            .collect::<HashMap<_, _>>();

        let agent1_number = group_agents.get(agent1.agent_id()).unwrap();
        assert_eq!(*agent1_number, 0);

        let agent2_number = group_agents.get(agent2.agent_id()).unwrap();
        assert_eq!(*agent2_number, 1);

        let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(
            room.id(),
            &[agent1.agent_id(), agent2.agent_id()],
        )
        .execute(&conn)
        .expect("failed to get rtc reader configs");

        assert_eq!(reader_configs.len(), 2);

        for (cfg, _) in reader_configs {
            assert!(!cfg.receive_video());
            assert!(!cfg.receive_audio());
        }

        // Make one more group.update request
        let payload = Payload {
            room_id: room.id(),
            groups: vec![StateItem {
                number: 0,
                agents: vec![agent1.agent_id().to_owned(), agent2.agent_id().to_owned()],
            }],
        };

        handle_request::<Handler>(&mut context, &agent1, payload)
            .await
            .expect("Group update failed");

        let group_agents = db::group_agent::ListWithGroupQuery::new(room.id())
            .execute(&conn)
            .expect("failed to get groups with participants");

        assert_eq!(group_agents.len(), 2);

        let group_agents = group_agents
            .iter()
            .map(|ga| (ga.agent_id.to_owned(), ga.number))
            .collect::<HashMap<_, _>>();

        let agent1_number = group_agents.get(agent1.agent_id()).unwrap();
        assert_eq!(*agent1_number, 0);

        let agent2_number = group_agents.get(agent2.agent_id()).unwrap();
        assert_eq!(*agent2_number, 0);

        let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(
            room.id(),
            &[agent1.agent_id(), agent2.agent_id()],
        )
        .execute(&conn)
        .expect("failed to get rtc reader configs");

        assert_eq!(reader_configs.len(), 2);

        for (cfg, _) in reader_configs {
            assert!(cfg.receive_video());
            assert!(cfg.receive_audio());
        }

        context.janus_clients().remove_client(&backend);
    }
}
