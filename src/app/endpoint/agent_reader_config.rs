use std::{collections::HashMap, sync::Arc};

use crate::{
    app::{
        context::{AppContext, Context},
        endpoint::prelude::*,
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
    },
    authz::AuthzObject,
    backend::janus::client::update_agent_reader_config::{
        UpdateReaderConfigRequest, UpdateReaderConfigRequestBody,
        UpdateReaderConfigRequestBodyConfigItem,
    },
    db::{self, rtc::Object as Rtc, rtc_reader_config::Object as RtcReaderConfig},
    diesel::Connection,
};
use anyhow::{anyhow, Context as AnyhowContext};
use async_trait::async_trait;
use axum::{
    extract::{Extension, Path},
    Json,
};
use serde::{Deserialize, Serialize};
use svc_agent::{mqtt::ResponseStatus, Addressable, AgentId};

use svc_utils::extractors::AgentIdExtractor;

const MAX_STATE_CONFIGS_LEN: usize = 20;

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct State {
    room_id: db::room::Id,
    configs: Vec<StateConfigItem>,
}

impl State {
    fn new(room_id: db::room::Id, rtc_reader_configs: &[(RtcReaderConfig, Rtc)]) -> State {
        let configs = rtc_reader_configs
            .iter()
            .map(|(rtc_reader_config, rtc)| {
                StateConfigItem::new(rtc.created_by().to_owned())
                    .receive_video(rtc_reader_config.receive_video())
                    .receive_audio(rtc_reader_config.receive_audio())
            })
            .collect::<Vec<_>>();

        Self { room_id, configs }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StateConfigItem {
    agent_id: AgentId,
    receive_video: Option<bool>,
    receive_audio: Option<bool>,
}

impl StateConfigItem {
    fn new(agent_id: AgentId) -> Self {
        Self {
            agent_id,
            receive_video: None,
            receive_audio: None,
        }
    }

    fn receive_video(self, receive_video: bool) -> Self {
        Self {
            receive_video: Some(receive_video),
            ..self
        }
    }

    fn receive_audio(self, receive_audio: bool) -> Self {
        Self {
            receive_audio: Some(receive_audio),
            ..self
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
#[derive(Debug, Deserialize)]
pub struct StateConfigs {
    configs: Vec<StateConfigItem>,
}

pub async fn update(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    Json(configs): Json<StateConfigs>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = State {
        room_id,
        configs: configs.configs,
    };
    UpdateHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct UpdateHandler;

#[async_trait]
impl RequestHandler for UpdateHandler {
    type Payload = State;
    const ERROR_TITLE: &'static str = "Failed to update agent reader config";

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        if payload.configs.len() > MAX_STATE_CONFIGS_LEN {
            return Err(anyhow!("Too many items in `configs` list"))
                .error(AppErrorKind::InvalidPayload)?;
        }

        let State { room_id, configs } = payload;

        let room = crate::util::spawn_blocking({
            let conn = context.get_conn().await?;
            move || helpers::find_room_by_id(room_id, helpers::RoomTimeRequirement::Open, &conn)
        })
        .await?;

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
            return Err(anyhow!(
                "Agent reader config is available only for rooms with owned RTC sharing policy"
            ))
            .error(AppErrorKind::InvalidPayload)?;
        }

        // Authorize classrooms.update on the tenant
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "update".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        let (room, rtc_reader_configs_with_rtcs, maybe_backend) = crate::util::spawn_blocking({
            let agent_id = reqp.as_agent_id().clone();

            let mut conn = context.get_conn_sqlx().await?;
            // Find backend and send updates to it if present.
            let maybe_backend = match room.backend_id() {
                None => None,
                Some(backend_id) => {
                    db::janus_backend::FindQuery::new(backend_id)
                        .execute(&mut conn)
                        .await?
                }
            };

            let conn = context.get_conn().await?;

            move || {
                let rtc_reader_configs_with_rtcs = conn.transaction::<_, AppError, _>(|| {
                    // An agent can create/update reader configs only for agents in the same group
                    let groups = db::group_agent::FindQuery::new(room.id())
                        .execute(&conn)?
                        .groups()
                        .filter_by_agent(&agent_id);
                    let group_agents = groups.iter().flat_map(|i| i.agents()).collect::<Vec<_>>();

                    // Find RTCs owned by agents.
                    let agent_ids = configs.iter().map(|c| &c.agent_id).collect::<Vec<_>>();

                    let rtcs = db::rtc::ListQuery::new()
                        .room_id(room.id())
                        .created_by(agent_ids.as_slice())
                        .execute(&conn)?;

                    let agents_to_rtcs = rtcs
                        .iter()
                        .map(|rtc| (rtc.created_by(), rtc.id()))
                        .collect::<HashMap<_, _>>();

                    // Create or update the config.
                    for state_config_item in configs {
                        let rtc_id = agents_to_rtcs
                            .get(&state_config_item.agent_id)
                            .ok_or_else(|| {
                                anyhow!("{} has no owned RTC", state_config_item.agent_id)
                            })
                            .error(AppErrorKind::InvalidPayload)?;

                        if !group_agents.contains(&&state_config_item.agent_id) {
                            return Err(anyhow!(
                                "{} is in another group",
                                state_config_item.agent_id
                            ))
                            .error(AppErrorKind::InvalidPayload)?;
                        }

                        let mut q = db::rtc_reader_config::UpsertQuery::new(*rtc_id, &agent_id);

                        if let Some(receive_video) = state_config_item.receive_video {
                            q = q.receive_video(receive_video);
                        }

                        if let Some(receive_audio) = state_config_item.receive_audio {
                            q = q.receive_audio(receive_audio);
                        }

                        q.execute(&conn)?;
                    }

                    // Retrieve state data.
                    let rtc_reader_configs_with_rtcs =
                        db::rtc_reader_config::ListWithRtcQuery::new(room.id(), &[&agent_id])
                            .execute(&conn)?;

                    Ok(rtc_reader_configs_with_rtcs)
                })?;

                Ok::<_, AppError>((room, rtc_reader_configs_with_rtcs, maybe_backend))
            }
        })
        .await?;

        {
            let mut conn = context.get_conn_sqlx().await?;
            helpers::check_room_presence(&room, reqp.as_agent_id(), &mut conn).await?;
        }

        if let Some(backend) = maybe_backend {
            let items = rtc_reader_configs_with_rtcs
                .iter()
                .map(
                    |(rtc_reader_config, rtc)| UpdateReaderConfigRequestBodyConfigItem {
                        reader_id: rtc_reader_config.reader_id().to_owned(),
                        stream_id: rtc.id(),
                        receive_video: rtc_reader_config.receive_video(),
                        receive_audio: rtc_reader_config.receive_audio(),
                    },
                )
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

        context
            .metrics()
            .request_duration
            .agent_reader_config_update
            .observe_timestamp(context.start_timestamp());
        Ok(Response::new(
            ResponseStatus::OK,
            State::new(room.id(), &rtc_reader_configs_with_rtcs),
            context.start_timestamp(),
            None,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct ReadRequest {
    room_id: db::room::Id,
}

pub async fn read(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = ReadRequest { room_id };
    ReadHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct ReadHandler;

#[async_trait]
impl RequestHandler for ReadHandler {
    type Payload = ReadRequest;
    const ERROR_TITLE: &'static str = "Failed to read agent reader config";

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let room = crate::util::spawn_blocking({
            let conn = context.get_conn().await?;
            move || {
                helpers::find_room_by_id(payload.room_id, helpers::RoomTimeRequirement::Open, &conn)
            }
        })
        .await?;

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize classrooms.read on the tenant
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "read".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        let (room, rtc_reader_configs_with_rtcs) = crate::util::spawn_blocking({
            let agent_id = reqp.as_agent_id().clone();
            let conn = context.get_conn().await?;

            move || {
                if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
                    return Err(anyhow!(
                    "Agent reader config is available only for rooms with owned RTC sharing policy"
                ))
                    .error(AppErrorKind::InvalidPayload)?;
                }

                let rtc_reader_configs_with_rtcs =
                    db::rtc_reader_config::ListWithRtcQuery::new(room.id(), &[&agent_id])
                        .execute(&conn)?;
                Ok::<_, AppError>((room, rtc_reader_configs_with_rtcs))
            }
        })
        .await?;

        {
            let mut conn = context.get_conn_sqlx().await?;
            helpers::check_room_presence(&room, reqp.as_agent_id(), &mut conn).await?;
        }

        context
            .metrics()
            .request_duration
            .agent_reader_config_read
            .observe_timestamp(context.start_timestamp());

        Ok(Response::new(
            ResponseStatus::OK,
            State::new(room.id(), &rtc_reader_configs_with_rtcs),
            context.start_timestamp(),
            None,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod update {
        use std::ops::Bound;

        use crate::db::group_agent::{GroupItem, Groups};
        use crate::{
            db::rtc::SharingPolicy as RtcSharingPolicy,
            test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps},
        };
        use chrono::{Duration, Utc};

        use super::super::*;

        #[tokio::test]
        async fn update_agent_reader_config() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
            let agent4 = TestAgent::new("web", "user4", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            let backend =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            // Insert a room with agents and RTCs.
            let (room, backend, _rtcs) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((Bound::Included(Utc::now()), Bound::Unbounded))
                        .rtc_sharing_policy(RtcSharingPolicy::Owned)
                        .backend_id(backend.id())
                        .insert(&conn);

                    for agent in &[&agent1, &agent2, &agent3, &agent4] {
                        shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    }

                    let rtcs = vec![&agent2, &agent3, &agent4]
                        .into_iter()
                        .map(|agent| {
                            factory::Rtc::new(room.id())
                                .created_by(agent.agent_id().to_owned())
                                .insert(&conn)
                        })
                        .collect::<Vec<_>>();

                    let groups = Groups::new(vec![GroupItem::new(
                        0,
                        vec![
                            agent1.agent_id().clone(),
                            agent2.agent_id().clone(),
                            agent3.agent_id().clone(),
                            agent4.agent_id().clone(),
                        ],
                    )]);

                    factory::GroupAgent::new(room.id(), groups).upsert(&conn);

                    (room, backend, rtcs)
                })
                .unwrap();

            // Allow agent to update the room.
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent1.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make agent_reader_config.update request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = State {
                room_id: room.id(),
                configs: vec![
                    StateConfigItem {
                        agent_id: agent2.agent_id().to_owned(),
                        receive_video: Some(true),
                        receive_audio: Some(false),
                    },
                    StateConfigItem {
                        agent_id: agent3.agent_id().to_owned(),
                        receive_video: Some(false),
                        receive_audio: Some(false),
                    },
                ],
            };

            let messages = handle_request::<UpdateHandler>(&mut context, &agent1, payload)
                .await
                .expect("Agent reader config update failed");

            // Assert response.
            let (state, respp, _) = find_response::<State>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(state.room_id, room.id());
            assert_eq!(state.configs.len(), 2);

            let agent2_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent2.agent_id())
                .expect("Config for agent2 not found");

            assert_eq!(agent2_config.receive_video, Some(true));
            assert_eq!(agent2_config.receive_audio, Some(false));

            let agent3_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent3.agent_id())
                .expect("Config for agent3 not found");

            assert_eq!(agent3_config.receive_video, Some(false));
            assert_eq!(agent3_config.receive_audio, Some(false));

            // Make one more agent_reader_config.update request.
            let payload = State {
                room_id: room.id(),
                configs: vec![
                    StateConfigItem {
                        agent_id: agent4.agent_id().to_owned(),
                        receive_video: Some(true),
                        receive_audio: Some(true),
                    },
                    StateConfigItem {
                        agent_id: agent3.agent_id().to_owned(),
                        receive_video: None,
                        receive_audio: Some(true),
                    },
                ],
            };

            let messages = handle_request::<UpdateHandler>(&mut context, &agent1, payload)
                .await
                .expect("Agent reader config update failed");

            // Assert response.
            let (state, respp, _) = find_response::<State>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(state.room_id, room.id());
            assert_eq!(state.configs.len(), 3);

            let agent2_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent2.agent_id())
                .expect("Config for agent2 not found");

            assert_eq!(agent2_config.receive_video, Some(true));
            assert_eq!(agent2_config.receive_audio, Some(false));

            let agent3_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent3.agent_id())
                .expect("Config for agent3 not found");

            assert_eq!(agent3_config.receive_video, Some(false));
            assert_eq!(agent3_config.receive_audio, Some(true));

            let agent4_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent4.agent_id())
                .expect("Config for agent4 not found");

            assert_eq!(agent4_config.receive_video, Some(true));
            assert_eq!(agent4_config.receive_audio, Some(true));

            context.janus_clients().remove_client(&backend);
            Ok(())
        }

        #[tokio::test]
        async fn too_many_config_items() -> std::io::Result<()> {
            // Make agent_reader_config.update request.
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user", USR_AUDIENCE);
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let configs = (0..(MAX_STATE_CONFIGS_LEN + 1))
                .map(|i| {
                    let agent = TestAgent::new("web", &format!("user{}", i), USR_AUDIENCE);

                    StateConfigItem {
                        agent_id: agent.agent_id().to_owned(),
                        receive_video: Some(false),
                        receive_audio: Some(true),
                    }
                })
                .collect::<Vec<_>>();

            let payload = State {
                room_id: db::room::Id::random(),
                configs,
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config update success");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_payload");
            Ok(())
        }

        #[tokio::test]
        async fn agent_without_rtc() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);

            // Insert a room with agents.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((Bound::Included(Utc::now()), Bound::Unbounded))
                        .rtc_sharing_policy(RtcSharingPolicy::Owned)
                        .insert(&conn);

                    shared_helpers::insert_agent(&conn, agent1.agent_id(), room.id());
                    shared_helpers::insert_agent(&conn, agent2.agent_id(), room.id());

                    factory::GroupAgent::new(
                        room.id(),
                        Groups::new(vec![GroupItem::new(0, vec![])]),
                    )
                    .upsert(&conn);

                    room
                })
                .unwrap();

            // Allow agent to update the room.
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent1.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make agent_reader_config.update request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;

            let payload = State {
                room_id: room.id(),
                configs: vec![StateConfigItem {
                    agent_id: agent2.agent_id().to_owned(),
                    receive_video: Some(false),
                    receive_audio: Some(true),
                }],
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent1, payload)
                .await
                .expect_err("Unexpected agent reader config update success");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_payload");
            Ok(())
        }

        #[tokio::test]
        async fn not_entered() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| shared_helpers::insert_room_with_owned(&conn))
                .unwrap();

            // Allow agent to update the room.
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make agent_reader_config.update request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;

            let payload = State {
                room_id: room.id(),
                configs: vec![],
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config update success");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
            Ok(())
        }

        #[tokio::test]
        async fn closed_room() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

            // Insert a room with an agent.
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

            // Make agent_reader_config.update request.
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = State {
                room_id: room.id(),
                configs: vec![],
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config update success");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
            Ok(())
        }

        #[tokio::test]
        async fn room_with_wrong_rtc_policy() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

            // Insert a room with an agent.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((Bound::Included(Utc::now()), Bound::Unbounded))
                        .rtc_sharing_policy(RtcSharingPolicy::Shared)
                        .insert(&conn);

                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());

                    room
                })
                .unwrap();

            // Allow agent to update the room.
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make agent_reader_config.update request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;

            let payload = State {
                room_id: room.id(),
                configs: vec![],
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config update success");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_payload");
            Ok(())
        }

        #[tokio::test]
        async fn missing_room() -> std::io::Result<()> {
            // Make agent_reader_config.update request.
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = State {
                room_id: db::room::Id::random(),
                configs: vec![],
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config update success");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
            Ok(())
        }

        #[tokio::test]
        async fn agent_in_another_group() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            let backend =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            // Insert a room with agents and RTCs.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((Bound::Included(Utc::now()), Bound::Unbounded))
                        .rtc_sharing_policy(RtcSharingPolicy::Owned)
                        .backend_id(backend.id())
                        .insert(&conn);

                    for agent in &[&agent1, &agent2] {
                        shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    }

                    factory::Rtc::new(room.id())
                        .created_by(agent2.agent_id().to_owned())
                        .insert(&conn);

                    factory::GroupAgent::new(
                        room.id(),
                        Groups::new(vec![GroupItem::new(0, vec![agent1.agent_id().clone()])]),
                    )
                    .upsert(&conn);

                    room
                })
                .unwrap();

            // Allow agent to update the room.
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent1.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make agent_reader_config.update request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = State {
                room_id: room.id(),
                configs: vec![StateConfigItem {
                    agent_id: agent2.agent_id().to_owned(),
                    receive_video: Some(true),
                    receive_audio: Some(false),
                }],
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent1, payload)
                .await
                .expect_err("Unexpected agent reader config update success");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(
                err.to_string(),
                format!("Invalid payload: {} is in another group", agent2.agent_id())
            );

            Ok(())
        }
    }

    mod read {
        use std::ops::Bound;

        use chrono::{Duration, Utc};

        use crate::{
            db::rtc::SharingPolicy as RtcSharingPolicy,
            test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn read_state() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);

            // Insert a room with RTCs and agent reader configs.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((Bound::Included(Utc::now()), Bound::Unbounded))
                        .rtc_sharing_policy(RtcSharingPolicy::Owned)
                        .insert(&conn);

                    shared_helpers::insert_agent(&conn, agent1.agent_id(), room.id());

                    let rtc2 = factory::Rtc::new(room.id())
                        .created_by(agent2.agent_id().to_owned())
                        .insert(&conn);

                    factory::RtcReaderConfig::new(&rtc2, agent1.agent_id())
                        .receive_video(true)
                        .receive_audio(true)
                        .insert(&conn);

                    let rtc3 = factory::Rtc::new(room.id())
                        .created_by(agent3.agent_id().to_owned())
                        .insert(&conn);

                    factory::RtcReaderConfig::new(&rtc3, agent1.agent_id())
                        .receive_video(false)
                        .receive_audio(false)
                        .insert(&conn);

                    room
                })
                .unwrap();

            // Allow agent to read the room.
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent1.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );

            // Make agent_reader_config.read request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;

            let payload = ReadRequest { room_id: room.id() };

            let messages = handle_request::<ReadHandler>(&mut context, &agent1, payload)
                .await
                .expect("Agent reader config read failed");

            // Assert response.
            let (state, respp, _) = find_response::<State>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(state.room_id, room.id());
            assert_eq!(state.configs.len(), 2);

            let agent2_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent2.agent_id())
                .expect("Config for agent2 not found");

            assert_eq!(agent2_config.receive_video, Some(true));
            assert_eq!(agent2_config.receive_audio, Some(true));

            let agent3_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent3.agent_id())
                .expect("Config for agent3 not found");

            assert_eq!(agent3_config.receive_video, Some(false));
            assert_eq!(agent3_config.receive_audio, Some(false));

            Ok(())
        }

        #[tokio::test]
        async fn not_entered() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| shared_helpers::insert_room_with_owned(&conn))
                .unwrap();

            // Allow agent to read the room.
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );

            // Make agent_reader_config.read request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config read success");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
            Ok(())
        }

        #[tokio::test]
        async fn closed_room() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

            // Insert a room with an agent.
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

            // Make agent_reader_config.read request.
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config read success");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
            Ok(())
        }

        #[tokio::test]
        async fn wrong_rtc_sharing_policy() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

            // Insert a room with an agent.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((Bound::Included(Utc::now()), Bound::Unbounded))
                        .rtc_sharing_policy(RtcSharingPolicy::Shared)
                        .insert(&conn);

                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());

                    room
                })
                .unwrap();

            // Allow agent to read the room.
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );

            // Make agent_reader_config.read request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config update success");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_payload");
            Ok(())
        }

        #[tokio::test]
        async fn missing_room() -> std::io::Result<()> {
            // Make agent_reader_config.read request.
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = ReadRequest {
                room_id: db::room::Id::random(),
            };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config read success");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
            Ok(())
        }
    }
}
