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
use crate::db::rtc_writer_config::Object as RtcWriterConfig;
use crate::diesel::Connection;
use crate::{
    app::context::Context,
    backend::janus::http::update_agent_writer_config::{
        UpdateWriterConfigRequest, UpdateWriterConfigRequestBodyConfigItem,
    },
};
use crate::{
    app::endpoint::prelude::*,
    backend::janus::http::update_agent_writer_config::UpdateWriterConfigRequestBody,
};

const MAX_STATE_CONFIGS_LEN: usize = 20;

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct State {
    room_id: Uuid,
    configs: Vec<StateConfigItem>,
}

impl State {
    fn new(room_id: Uuid, rtc_writer_configs_with_rtcs: &[(RtcWriterConfig, Rtc)]) -> State {
        let configs = rtc_writer_configs_with_rtcs
            .into_iter()
            .map(|(rtc_writer_config, rtc)| {
                let mut config_item = StateConfigItem::new(rtc.created_by().to_owned())
                    .send_video(rtc_writer_config.send_video())
                    .send_audio(rtc_writer_config.send_audio());

                if let Some(video_remb) = rtc_writer_config.video_remb() {
                    config_item = config_item.video_remb(video_remb as u32);
                }

                if let Some(send_audio_updated_by) = rtc_writer_config.send_audio_updated_by() {
                    config_item =
                        config_item.send_audio_updated_by(send_audio_updated_by.to_owned());
                }

                config_item
            })
            .collect::<Vec<_>>();

        Self { room_id, configs }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct StateConfigItem {
    agent_id: AgentId,
    send_video: Option<bool>,
    send_audio: Option<bool>,
    video_remb: Option<u32>,
    #[cfg_attr(not(test), serde(skip_deserializing))]
    send_audio_updated_by: Option<AgentId>,
}

impl StateConfigItem {
    fn new(agent_id: AgentId) -> Self {
        Self {
            agent_id,
            send_video: None,
            send_audio: None,
            video_remb: None,
            send_audio_updated_by: None,
        }
    }

    fn send_video(self, send_video: bool) -> Self {
        Self {
            send_video: Some(send_video),
            ..self
        }
    }

    fn send_audio(self, send_audio: bool) -> Self {
        Self {
            send_audio: Some(send_audio),
            ..self
        }
    }

    fn video_remb(self, video_remb: u32) -> Self {
        Self {
            video_remb: Some(video_remb),
            ..self
        }
    }

    fn send_audio_updated_by(self, send_audio_updated_by: AgentId) -> Self {
        Self {
            send_audio_updated_by: Some(send_audio_updated_by),
            ..self
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct UpdateHandler;

#[async_trait]
impl RequestHandler for UpdateHandler {
    type Payload = State;
    const ERROR_TITLE: &'static str = "Failed to update agent writer config";

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        if payload.configs.len() > MAX_STATE_CONFIGS_LEN {
            return Err(anyhow!("Too many items in `configs` list"))
                .error(AppErrorKind::InvalidPayload)?;
        }

        let room = {
            let conn = context.get_conn()?;

            let room = helpers::find_room_by_id(
                context,
                payload.room_id,
                helpers::RoomTimeRequirement::Open,
                &conn,
            )?;

            if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
                return Err(anyhow!(
                    "Agent writer config is available only for rooms with owned RTC sharing policy"
                ))
                .error(AppErrorKind::InvalidPayload)?;
            }

            helpers::check_room_presence(&room, reqp.as_agent_id(), &conn)?;
            room
        };

        // Authorize agent writer config updating on the tenant.
        let is_only_owned_config =
            payload.configs.len() == 1 && &payload.configs[0].agent_id == reqp.as_agent_id();

        let maybe_authz_time = if is_only_owned_config {
            None
        } else {
            let room_id = room.id().to_string();
            let object = vec!["rooms", &room_id];

            let authz_time = context
                .authz()
                .authorize(room.audience(), reqp, object, "update")
                .await?;

            Some(authz_time)
        };

        let conn = context.get_conn()?;

        let rtc_writer_configs_with_rtcs = conn.transaction::<_, AppError, _>(|| {
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

                let mut q = db::rtc_writer_config::UpsertQuery::new(*rtc_id);

                if let Some(send_video) = state_config_item.send_video {
                    q = q.send_video(send_video);
                }

                if let Some(send_audio) = state_config_item.send_audio {
                    q = q
                        .send_audio(send_audio)
                        .send_audio_updated_by(reqp.as_agent_id());
                }

                if let Some(video_remb) = state_config_item.video_remb {
                    q = q.video_remb(video_remb.into());
                }

                q.execute(&conn)?;
            }

            // Retrieve state data.
            let rtc_writer_configs_with_rtcs =
                db::rtc_writer_config::ListWithRtcQuery::new(room.id()).execute(&conn)?;

            Ok(rtc_writer_configs_with_rtcs)
        })?;

        // Find backend and send updates to it if present.
        let maybe_backend = match room.backend_id() {
            None => None,
            Some(backend_id) => db::janus_backend::FindQuery::new()
                .id(backend_id)
                .execute(&conn)?,
        };

        if let Some(backend) = maybe_backend {
            let items = rtc_writer_configs_with_rtcs
                .iter()
                .map(
                    |(rtc_writer_config, rtc)| UpdateWriterConfigRequestBodyConfigItem {
                        stream_id: rtc.id(),
                        send_video: rtc_writer_config.send_video(),
                        send_audio: rtc_writer_config.send_audio(),
                        video_remb: rtc_writer_config.video_remb().map(|x| x as u32),
                    },
                )
                .collect::<Vec<UpdateWriterConfigRequestBodyConfigItem>>();

            let request = UpdateWriterConfigRequest {
                session_id: backend.session_id(),
                handle_id: backend.handle_id(),
                body: UpdateWriterConfigRequestBody::new(items),
            };
            context
                .janus_http_client()
                .writer_update(request)
                .await
                .error(AppErrorKind::BackendRequestFailed)?;
        }

        let state = State::new(room.id(), &rtc_writer_configs_with_rtcs);

        let response = helpers::build_response(
            ResponseStatus::OK,
            state.clone(),
            reqp,
            context.start_timestamp(),
            maybe_authz_time,
        );

        let notification = helpers::build_notification(
            "agent_writer_config.update",
            &format!("rooms/{}/events", room.id()),
            state,
            reqp,
            context.start_timestamp(),
        );

        let messages = vec![response, notification];

        Ok(Box::new(stream::from_iter(messages)))
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
    const ERROR_TITLE: &'static str = "Failed to read agent writer config";

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
                "Agent writer config is available only for rooms with owned RTC sharing policy"
            ))
            .error(AppErrorKind::InvalidPayload)?;
        }

        helpers::check_room_presence(&room, reqp.as_agent_id(), &conn)?;

        let rtc_writer_configs_with_rtcs =
            db::rtc_writer_config::ListWithRtcQuery::new(room.id()).execute(&conn)?;

        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            State::new(room.id(), &rtc_writer_configs_with_rtcs),
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
        async fn update_agent_writer_config() -> std::io::Result<()> {
            let db = TestDb::new();
            let mut authz = TestAuthz::new();
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

            // Allow agent to update agent_writer_config.
            let room_id = room.id().to_string();
            let object = vec!["rooms", &room_id];
            authz.allow(agent1.account_id(), object, "update");

            // Make agent_writer_config.update request.
            let mut context = TestContext::new(db, authz);

            let payload = State {
                room_id: room.id(),
                configs: vec![
                    StateConfigItem {
                        agent_id: agent2.agent_id().to_owned(),
                        send_video: Some(true),
                        send_audio: Some(false),
                        video_remb: Some(300_000),
                        send_audio_updated_by: None,
                    },
                    StateConfigItem {
                        agent_id: agent3.agent_id().to_owned(),
                        send_video: Some(false),
                        send_audio: Some(false),
                        video_remb: None,
                        send_audio_updated_by: None,
                    },
                ],
            };

            let messages = handle_request::<UpdateHandler>(&mut context, &agent1, payload)
                .await
                .expect("Agent writer config update failed");

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

            assert_eq!(agent2_config.send_video, Some(true));
            assert_eq!(agent2_config.send_audio, Some(false));
            assert_eq!(agent2_config.video_remb, Some(300_000));

            assert_eq!(
                agent2_config.send_audio_updated_by,
                Some(agent1.agent_id().to_owned())
            );

            let agent3_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent3.agent_id())
                .expect("Config for agent3 not found");

            assert_eq!(agent3_config.send_video, Some(false));
            assert_eq!(agent3_config.send_audio, Some(false));
            assert_eq!(agent3_config.video_remb, None);

            assert_eq!(
                agent2_config.send_audio_updated_by,
                Some(agent1.agent_id().to_owned())
            );

            // Assert notification.
            let (state, evp, _) = find_event::<State>(messages.as_slice());
            assert_eq!(evp.label(), "agent_writer_config.update");
            assert_eq!(state.room_id, room.id());
            assert_eq!(state.configs.len(), 2);

            let agent2_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent2.agent_id())
                .expect("Config for agent2 not found");

            assert_eq!(agent2_config.send_video, Some(true));
            assert_eq!(agent2_config.send_audio, Some(false));
            assert_eq!(agent2_config.video_remb, Some(300_000));

            assert_eq!(
                agent2_config.send_audio_updated_by,
                Some(agent1.agent_id().to_owned())
            );

            let agent3_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent3.agent_id())
                .expect("Config for agent3 not found");

            assert_eq!(agent3_config.send_video, Some(false));
            assert_eq!(agent3_config.send_audio, Some(false));
            assert_eq!(agent3_config.video_remb, None);

            // Make one more agent_writer_config.update request.
            let payload = State {
                room_id: room.id(),
                configs: vec![
                    StateConfigItem {
                        agent_id: agent4.agent_id().to_owned(),
                        send_video: Some(true),
                        send_audio: Some(true),
                        video_remb: Some(1_000_000),
                        send_audio_updated_by: None,
                    },
                    StateConfigItem {
                        agent_id: agent3.agent_id().to_owned(),
                        send_video: None,
                        send_audio: Some(true),
                        video_remb: None,
                        send_audio_updated_by: None,
                    },
                ],
            };

            let messages = handle_request::<UpdateHandler>(&mut context, &agent1, payload)
                .await
                .expect("Agent writer config update failed");

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

            assert_eq!(agent2_config.send_video, Some(true));
            assert_eq!(agent2_config.send_audio, Some(false));
            assert_eq!(agent2_config.video_remb, Some(300_000));

            let agent3_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent3.agent_id())
                .expect("Config for agent3 not found");

            assert_eq!(agent3_config.send_video, Some(false));
            assert_eq!(agent3_config.send_audio, Some(true));
            assert_eq!(agent3_config.video_remb, None);

            let agent4_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent4.agent_id())
                .expect("Config for agent4 not found");

            assert_eq!(agent4_config.send_video, Some(true));
            assert_eq!(agent4_config.send_audio, Some(true));
            assert_eq!(agent4_config.video_remb, Some(1_000_000));

            // Assert notification.
            let (state, evp, _) = find_event::<State>(messages.as_slice());
            assert_eq!(evp.label(), "agent_writer_config.update");
            assert_eq!(state.room_id, room.id());
            assert_eq!(state.configs.len(), 3);

            let agent2_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent2.agent_id())
                .expect("Config for agent2 not found");

            assert_eq!(agent2_config.send_video, Some(true));
            assert_eq!(agent2_config.send_audio, Some(false));
            assert_eq!(agent2_config.video_remb, Some(300_000));

            let agent3_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent3.agent_id())
                .expect("Config for agent3 not found");

            assert_eq!(agent3_config.send_video, Some(false));
            assert_eq!(agent3_config.send_audio, Some(true));
            assert_eq!(agent3_config.video_remb, None);

            let agent4_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent4.agent_id())
                .expect("Config for agent4 not found");

            assert_eq!(agent4_config.send_video, Some(true));
            assert_eq!(agent4_config.send_audio, Some(true));
            assert_eq!(agent4_config.video_remb, Some(1_000_000));

            Ok(())
        }

        #[async_std::test]
        async fn not_authorized() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

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

                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    room
                })
                .unwrap();

            // Make agent_writer_config.update request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = State {
                room_id: room.id(),
                configs: vec![],
            };

            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent writer config update success");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
            Ok(())
        }

        #[async_std::test]
        async fn too_many_config_items() -> std::io::Result<()> {
            // Make agent_writer_config.update request.
            let agent = TestAgent::new("web", "user", USR_AUDIENCE);
            let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

            let configs = (0..(MAX_STATE_CONFIGS_LEN + 1))
                .map(|i| {
                    let agent = TestAgent::new("web", &format!("user{}", i), USR_AUDIENCE);

                    StateConfigItem {
                        agent_id: agent.agent_id().to_owned(),
                        send_video: Some(false),
                        send_audio: Some(true),
                        video_remb: Some(300_000),
                        send_audio_updated_by: None,
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
                .expect_err("Unexpected agent writer config update success");

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

            // Make agent_writer_config.update request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = State {
                room_id: room.id(),
                configs: vec![],
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent writer config update success");

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

            // Make agent_writer_config.update request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = State {
                room_id: room.id(),
                configs: vec![],
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent writer config update success");

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

            // Make agent_writer_config.update request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = State {
                room_id: room.id(),
                configs: vec![],
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent writer config update success");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_payload");
            Ok(())
        }

        #[async_std::test]
        async fn missing_room() -> std::io::Result<()> {
            // Make agent_writer_config.update request.
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
            let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

            let payload = State {
                room_id: Uuid::new_v4(),
                configs: vec![],
            };

            // Assert error.
            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent writer config update success");

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

            // Insert a room with RTCs and agent writer configs.
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

                    factory::RtcWriterConfig::new(&rtc2)
                        .send_video(true)
                        .send_audio(true)
                        .video_remb(1_000_000)
                        .insert(&conn);

                    let rtc3 = factory::Rtc::new(room.id())
                        .created_by(agent3.agent_id().to_owned())
                        .insert(&conn);

                    factory::RtcWriterConfig::new(&rtc3)
                        .send_video(false)
                        .send_audio(false)
                        .video_remb(300_000)
                        .send_audio_updated_by(agent2.agent_id())
                        .insert(&conn);

                    room
                })
                .unwrap();

            // Make agent_writer_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = ReadRequest { room_id: room.id() };

            let messages = handle_request::<ReadHandler>(&mut context, &agent1, payload)
                .await
                .expect("Agent writer config read failed");

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

            assert_eq!(agent2_config.send_video, Some(true));
            assert_eq!(agent2_config.send_audio, Some(true));
            assert_eq!(agent2_config.video_remb, Some(1_000_000));
            assert_eq!(agent2_config.send_audio_updated_by, None);

            let agent3_config = state
                .configs
                .iter()
                .find(|c| &c.agent_id == agent3.agent_id())
                .expect("Config for agent3 not found");

            assert_eq!(agent3_config.send_video, Some(false));
            assert_eq!(agent3_config.send_audio, Some(false));
            assert_eq!(agent3_config.video_remb, Some(300_000));

            assert_eq!(
                agent3_config.send_audio_updated_by,
                Some(agent2.agent_id().to_owned())
            );

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

            // Make agent_writer_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent writer config read success");

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

            // Make agent_writer_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent writer config read success");

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

            // Make agent_writer_config.read request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = ReadRequest { room_id: room.id() };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent writer config update success");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_payload");
            Ok(())
        }

        #[async_std::test]
        async fn missing_room() -> std::io::Result<()> {
            // Make agent_writer_config.read request.
            let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
            let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

            let payload = ReadRequest {
                room_id: Uuid::new_v4(),
            };

            // Assert error.
            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected agent writer config read success");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
            Ok(())
        }
    }
}
