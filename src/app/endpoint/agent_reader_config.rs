use std::collections::HashMap;

use async_std::stream;
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{IncomingRequestProperties, ResponseStatus},
    Addressable, AgentId,
};
use uuid::Uuid;

use crate::db;
use crate::db::rtc::Object as Rtc;
use crate::db::rtc_reader_config::Object as RtcReaderConfig;
use crate::diesel::Connection;
use crate::{
    app::context::Context,
    backend::janus::http::update_agent_reader_config::UpdateReaderConfigRequestBodyConfigItem,
};
use crate::{
    app::endpoint::prelude::*,
    backend::janus::http::update_agent_reader_config::{
        UpdateReaderConfigRequest, UpdateReaderConfigRequestBody,
    },
};

const MAX_STATE_CONFIGS_LEN: usize = 20;

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct State {
    room_id: Uuid,
    configs: Vec<StateConfigItem>,
}

impl State {
    fn new(room_id: Uuid, rtc_reader_configs: &[(RtcReaderConfig, Rtc)]) -> State {
        let configs = rtc_reader_configs
            .into_iter()
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
pub(crate) struct StateConfigItem {
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

pub(crate) struct UpdateHandler;

#[async_trait]
impl RequestHandler for UpdateHandler {
    type Payload = State;
    const ERROR_TITLE: &'static str = "Failed to update agent reader config";

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        if payload.configs.len() > MAX_STATE_CONFIGS_LEN {
            return Err(anyhow!("Too many items in `configs` list"))
                .error(AppErrorKind::InvalidPayload)?;
        }

        let conn = context.get_conn()?;

        let room = helpers::find_room_by_id(
            context,
            payload.room_id,
            helpers::RoomTimeRequirement::Open,
            &conn,
        )?;

        if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
            return Err(anyhow!(
                "Agent reader config is available only for rooms with owned RTC sharing policy"
            ))
            .error(AppErrorKind::InvalidPayload)?;
        }

        helpers::check_room_presence(&room, reqp.as_agent_id(), &conn)?;

        let rtc_reader_configs_with_rtcs = conn.transaction::<_, AppError, _>(|| {
            // Find RTCs owned by agents.
            let agent_ids = payload
                .configs
                .iter()
                .map(|c| &c.agent_id)
                .collect::<Vec<_>>();

            let rtcs = db::rtc::ListQuery::new()
                .room_id(room.id())
                .created_by(agent_ids.as_slice())
                .execute(&conn)?;

            let agents_to_rtcs = rtcs
                .iter()
                .map(|rtc| (rtc.created_by(), rtc.id()))
                .collect::<HashMap<_, _>>();

            // Create or update the config.
            for state_config_item in payload.configs {
                let rtc_id = agents_to_rtcs
                    .get(&state_config_item.agent_id)
                    .ok_or_else(|| anyhow!("{} has no owned RTC", state_config_item.agent_id))
                    .error(AppErrorKind::InvalidPayload)?;

                let mut q = db::rtc_reader_config::UpsertQuery::new(*rtc_id, reqp.as_agent_id());

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
                db::rtc_reader_config::ListWithRtcQuery::new(room.id(), reqp.as_agent_id())
                    .execute(&conn)?;

            Ok(rtc_reader_configs_with_rtcs)
        })?;

        // Find backend and send updates to it if present.
        let maybe_backend = match room.backend_id() {
            None => None,
            Some(backend_id) => db::janus_backend::FindQuery::new()
                .id(backend_id)
                .execute(&conn)?,
        };

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
                .janus_http_client()
                .reader_update(request)
                .await
                .error(AppErrorKind::BackendRequestFailed)?
        }
        let response = helpers::build_response(
            ResponseStatus::OK,
            State::new(room.id(), &rtc_reader_configs_with_rtcs),
            reqp,
            context.start_timestamp(),
            None,
        );
        Ok(Box::new(stream::once(response)))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct ReadRequest {
    room_id: Uuid,
}

pub(crate) struct ReadHandler;

#[async_trait]
impl RequestHandler for ReadHandler {
    type Payload = ReadRequest;
    const ERROR_TITLE: &'static str = "Failed to read agent reader config";

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let conn = context.get_conn()?;

        let room = helpers::find_room_by_id(
            context,
            payload.room_id,
            helpers::RoomTimeRequirement::Open,
            &conn,
        )?;

        if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
            return Err(anyhow!(
                "Agent reader config is available only for rooms with owned RTC sharing policy"
            ))
            .error(AppErrorKind::InvalidPayload)?;
        }

        helpers::check_room_presence(&room, reqp.as_agent_id(), &conn)?;

        let rtc_reader_configs_with_rtcs =
            db::rtc_reader_config::ListWithRtcQuery::new(room.id(), reqp.as_agent_id())
                .execute(&conn)?;

        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            State::new(room.id(), &rtc_reader_configs_with_rtcs),
            reqp,
            context.start_timestamp(),
            None,
        ))))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod update {
        use std::ops::Bound;

        use chrono::{Duration, Utc};
        use serde_derive::Deserialize;
        use uuid::Uuid;

        use crate::backend::janus::{self};
        use crate::db::rtc::SharingPolicy as RtcSharingPolicy;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[async_std::test]
        async fn update_agent_reader_config() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
            let agent4 = TestAgent::new("web", "user4", USR_AUDIENCE);

            // Insert a room with agents and RTCs.
            let (room, backend, rtcs) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);

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

                    (room, backend, rtcs)
                })
                .unwrap();

            // Make agent_reader_config.update request.
            let mut context = TestContext::new(db, TestAuthz::new());

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

            Ok(())
        }

        #[async_std::test]
        async fn too_many_config_items() -> std::io::Result<()> {
            // Make agent_reader_config.update request.
            let agent = TestAgent::new("web", "user", USR_AUDIENCE);
            let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

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
                room_id: Uuid::new_v4(),
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

        #[async_std::test]
        async fn agent_without_rtc() -> std::io::Result<()> {
            let db = TestDb::new();
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

                    room
                })
                .unwrap();

            // Make agent_reader_config.update request.
            let mut context = TestContext::new(db, TestAuthz::new());

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

        #[async_std::test]
        async fn not_entered() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| shared_helpers::insert_room_with_owned(&conn))
                .unwrap();

            // Make agent_reader_config.update request.
            let mut context = TestContext::new(db, TestAuthz::new());

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

        #[async_std::test]
        async fn closed_room() -> std::io::Result<()> {
            let db = TestDb::new();
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
            let mut context = TestContext::new(db, TestAuthz::new());

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

        #[async_std::test]
        async fn room_with_wrong_rtc_policy() -> std::io::Result<()> {
            let db = TestDb::new();
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

            // Make agent_reader_config.update request.
            let mut context = TestContext::new(db, TestAuthz::new());

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

        #[async_std::test]
        async fn missing_room() -> std::io::Result<()> {
            // Make agent_reader_config.update request.
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
            let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

            let payload = State {
                room_id: Uuid::new_v4(),
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
    }

    mod read {
        use std::ops::Bound;

        use chrono::{Duration, Utc};
        use uuid::Uuid;

        use crate::db::rtc::SharingPolicy as RtcSharingPolicy;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[async_std::test]
        async fn read_state() -> std::io::Result<()> {
            let db = TestDb::new();
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

            // Make agent_reader_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new());

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

        #[async_std::test]
        async fn not_entered() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| shared_helpers::insert_room_with_owned(&conn))
                .unwrap();

            // Make agent_reader_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config read success");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
            Ok(())
        }

        #[async_std::test]
        async fn closed_room() -> std::io::Result<()> {
            let db = TestDb::new();
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
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config read success");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
            Ok(())
        }

        #[async_std::test]
        async fn wrong_rtc_sharing_policy() -> std::io::Result<()> {
            let db = TestDb::new();
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

            // Make agent_reader_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent reader config update success");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_payload");
            Ok(())
        }

        #[async_std::test]
        async fn missing_room() -> std::io::Result<()> {
            // Make agent_reader_config.read request.
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
            let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

            let payload = ReadRequest {
                room_id: Uuid::new_v4(),
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
