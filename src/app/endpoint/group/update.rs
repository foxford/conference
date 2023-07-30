use crate::{
    app::{
        context::{AppContext, GlobalContext},
        endpoint::{
            helpers,
            prelude::{AppError, AppErrorKind},
            RequestResult,
        },
        error::ErrorExt,
        group_reader_config,
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
        stage::video_group::{VideoGroupUpdateJanusConfig, SUBJECT_PREFIX},
    },
    authz::AuthzObject,
    backend::janus::client::update_agent_reader_config::UpdateReaderConfigRequestBodyConfigItem,
    db::{self, group_agent::Groups},
};
use anyhow::{anyhow, Context};
use axum::{extract::Path, Extension, Json};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use sqlx::Connection;
use std::sync::Arc;
use svc_agent::mqtt::ResponseStatus;
use svc_events::{
    stage::UpdateJanusConfigStageV1, EventV1 as Event, VideoGroupEventV1 as VideoGroupEvent,
};
use svc_utils::extractors::AgentIdExtractor;

#[derive(Deserialize)]
pub struct Payload {
    room_id: db::room::Id,
    groups: Groups,
}

pub async fn update(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    Json(groups): Json<Groups>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    Handler::handle(
        ctx,
        Payload { room_id, groups },
        RequestParams::Http {
            agent_id: &agent_id,
        },
        Utc::now(),
    )
    .await
}

pub struct Handler;

impl Handler {
    async fn handle(
        context: Arc<dyn GlobalContext + Send + Sync>,
        payload: Payload,
        reqp: RequestParams<'_>,
        start_timestamp: DateTime<Utc>,
    ) -> RequestResult {
        let Payload { room_id, groups } = payload;

        let room = {
            let mut conn = context.get_conn().await?;
            helpers::find_room_by_id(room_id, helpers::RoomTimeRequirement::NotClosed, &mut conn)
                .await?
        };

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize classrooms.update on the tenant
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "update".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        if room.rtc_sharing_policy() != db::rtc::SharingPolicy::Owned {
            return Err(anyhow!(
                "Updating groups is only available for rooms with owned RTC sharing policy"
            ))
            .error(AppErrorKind::InvalidPayload)?;
        }

        let backend_id = room
            .backend_id()
            .cloned()
            .context("backend not found")
            .error(AppErrorKind::BackendNotFound)?;

        let mut conn = context.get_conn().await?;
        {
            let context = context.clone();
            let backend_id = backend_id.clone();
            let _event_id = conn
                .transaction::<_, _, AppError>(|conn| {
                    Box::pin(async move {
                        let existed_groups = db::group_agent::FindQuery::new(room.id())
                            .execute(conn)
                            .await?
                            .groups()
                            .len();

                        let timestamp = Utc::now().timestamp_nanos();
                        let event = if existed_groups == 1 {
                            VideoGroupEvent::Created {
                                created_at: timestamp,
                            }
                        } else if existed_groups > 1 && groups.len() == 1 {
                            VideoGroupEvent::Deleted {
                                created_at: timestamp,
                            }
                        } else {
                            VideoGroupEvent::Updated {
                                created_at: timestamp,
                            }
                        };
                        let event = Event::from(event);

                        db::group_agent::UpsertQuery::new(room.id(), &groups)
                            .execute(conn)
                            .await?;

                        // Update rtc_reader_configs
                        let configs = group_reader_config::update(conn, room.id(), groups).await?;

                        // Generate configs for janus
                        let items = configs
                            .into_iter()
                            .map(|((rtc_id, agent_id), value)| {
                                UpdateReaderConfigRequestBodyConfigItem {
                                    reader_id: agent_id,
                                    stream_id: rtc_id,
                                    receive_video: value,
                                    receive_audio: value,
                                }
                            })
                            .collect::<Vec<_>>();

                        let init_stage = VideoGroupUpdateJanusConfig::init(
                            event,
                            room.classroom_id(),
                            room.id(),
                            backend_id,
                            items,
                        );

                        let event_id = crate::app::stage::nats_ids::sqlx::get_next_seq_id(conn)
                            .await
                            .error(AppErrorKind::InsertEventIdFailed)?
                            .to_event_id();

                        let serialized_stage = serde_json::to_value(init_stage)
                            .context("serialization failed")
                            .error(AppErrorKind::StageStateSerializationFailed)?;

                        let event = svc_events::Event::from(UpdateJanusConfigStageV1 {
                            event_id: event_id.clone(),
                            stage_state: serialized_stage,
                        });

                        let payload = serde_json::to_vec(&event)
                            .context("serialization failed")
                            .error(AppErrorKind::StageStateSerializationFailed)?;

                        let subject = svc_nats_client::Subject::new(
                            SUBJECT_PREFIX.to_string(),
                            room.classroom_id(),
                            event_id.entity_type().to_string(),
                        );

                        let event = svc_nats_client::event::Builder::new(
                            subject,
                            payload,
                            event_id.to_owned(),
                            context.agent_id().to_owned(),
                        )
                        .build();

                        context
                            .nats_client()
                            .ok_or_else(|| anyhow!("nats client not found"))
                            .error(AppErrorKind::NatsClientNotFound)?
                            .publish(&event)
                            .await
                            .error(AppErrorKind::NatsPublishFailed)?;

                        Ok(event_id)
                    })
                })
                .await?;
        }

        context
            .metrics()
            .request_duration
            .group_update
            .observe_timestamp(start_timestamp);

        Ok(Response::new(
            ResponseStatus::OK,
            json!({}),
            start_timestamp,
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{group_agent::GroupItem, rtc::SharingPolicy as RtcSharingPolicy};
    use crate::test_helpers::{
        db::TestDb,
        factory,
        prelude::{GlobalContext, TestAgent, TestAuthz, TestContext},
        shared_helpers,
        test_deps::LocalDeps,
        USR_AUDIENCE,
    };
    use chrono::{Duration, Utc};
    use std::collections::HashMap;
    use std::ops::Bound;

    #[sqlx::test]
    async fn missing_room(pool: sqlx::PgPool) -> std::io::Result<()> {
        let db = TestDb::new(pool);
        let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
        let context = TestContext::new(db, TestAuthz::new()).await;

        let payload = Payload {
            room_id: db::room::Id::random(),
            groups: Groups::new(vec![]),
        };

        // Assert error.
        let reqp = RequestParams::Http {
            agent_id: &agent.agent_id(),
        };
        let err = Handler::handle(Arc::new(context), payload, reqp, Utc::now())
            .await
            .err()
            .expect("Unexpected group update success");

        assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
        assert_eq!(err.kind(), "room_not_found");
        Ok(())
    }

    #[sqlx::test]
    async fn closed_room(pool: sqlx::PgPool) -> std::io::Result<()> {
        let db = TestDb::new(pool);
        let agent = TestAgent::new("web", "user1", USR_AUDIENCE);

        let mut conn = db.get_conn().await;

        let room = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((
                Bound::Included(Utc::now() - Duration::hours(2)),
                Bound::Excluded(Utc::now() - Duration::hours(1)),
            ))
            .rtc_sharing_policy(RtcSharingPolicy::Owned)
            .insert(&mut conn)
            .await;

        shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

        let context = TestContext::new(db, TestAuthz::new()).await;

        let payload = Payload {
            room_id: room.id(),
            groups: Groups::new(vec![]),
        };

        // Assert error.
        let reqp = RequestParams::Http {
            agent_id: &agent.agent_id(),
        };
        let err = Handler::handle(Arc::new(context), payload, reqp, Utc::now())
            .await
            .err()
            .expect("Unexpected group update success");

        assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
        assert_eq!(err.kind(), "room_closed");
        Ok(())
    }

    #[sqlx::test]
    async fn wrong_rtc_sharing_policy(pool: sqlx::PgPool) {
        let db = TestDb::new(pool);
        let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);

        let mut conn = db.get_conn().await;
        let room = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((Bound::Included(Utc::now()), Bound::Unbounded))
            .rtc_sharing_policy(RtcSharingPolicy::Shared)
            .insert(&mut conn)
            .await;

        // Allow agent to update the room.
        let mut authz = TestAuthz::new();
        let classroom_id = room.classroom_id().to_string();
        authz.allow(
            agent1.account_id(),
            vec!["classrooms", &classroom_id],
            "update",
        );

        let context = TestContext::new(db, authz).await;
        let payload = Payload {
            room_id: room.id(),
            groups: Groups::new(vec![]),
        };

        // Assert error.
        let reqp = RequestParams::Http {
            agent_id: &agent1.agent_id(),
        };
        let err = Handler::handle(Arc::new(context), payload, reqp, Utc::now())
            .await
            .err()
            .expect("Unexpected group update success");

        assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
        assert_eq!(err.kind(), "invalid_payload");
    }

    #[sqlx::test]
    async fn update_group_agents(pool: sqlx::PgPool) {
        let local_deps = LocalDeps::new();
        let janus = local_deps.run_janus();
        let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;

        let db = TestDb::new(pool);

        let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
        let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);

        let mut conn = db.get_conn().await;

        let backend =
            shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                .await;

        let room = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((Bound::Included(Utc::now()), Bound::Unbounded))
            .rtc_sharing_policy(RtcSharingPolicy::Owned)
            .backend_id(backend.id())
            .insert(&mut conn)
            .await;

        factory::GroupAgent::new(
            room.id(),
            Groups::new(vec![GroupItem::new(0, vec![]), GroupItem::new(1, vec![])]),
        )
        .upsert(&mut conn)
        .await;

        for agent in &[&agent1, &agent2] {
            factory::Rtc::new(room.id())
                .created_by(agent.agent_id().to_owned())
                .insert(&mut conn)
                .await;
        }

        // Allow agent to update the room.
        let mut authz = TestAuthz::new();
        let classroom_id = room.classroom_id().to_string();
        authz.allow(
            agent1.account_id(),
            vec!["classrooms", &classroom_id],
            "update",
        );

        let mut context = TestContext::new(db, authz).await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        context.with_janus(tx);

        let payload = Payload {
            room_id: room.id(),
            groups: Groups::new(vec![
                GroupItem::new(0, vec![agent1.agent_id().to_owned()]),
                GroupItem::new(1, vec![agent2.agent_id().to_owned()]),
            ]),
        };

        let reqp = RequestParams::Http {
            agent_id: &agent1.agent_id(),
        };
        Handler::handle(Arc::new(context.clone()), payload, reqp, Utc::now())
            .await
            .expect("Group update failed");

        let group_agent = db::group_agent::FindQuery::new(room.id())
            .execute(&mut conn)
            .await
            .expect("failed to get groups with participants");

        let groups = group_agent.groups();
        assert_eq!(groups.len(), 2);

        let group_agents = groups
            .iter()
            .flat_map(|g| g.agents().into_iter().map(move |a| (a.clone(), g.number())))
            .collect::<HashMap<_, _>>();

        let agent1_number = group_agents.get(agent1.agent_id()).unwrap();
        assert_eq!(*agent1_number, 0);

        let agent2_number = group_agents.get(agent2.agent_id()).unwrap();
        assert_eq!(*agent2_number, 1);

        let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(
            room.id(),
            &[agent1.agent_id(), agent2.agent_id()],
        )
        .execute(&mut conn)
        .await
        .expect("failed to get rtc reader configs");

        assert_eq!(reader_configs.len(), 2);

        for (cfg, _) in reader_configs {
            assert!(!cfg.receive_video());
            assert!(!cfg.receive_audio());
        }

        // Make one more group.update request
        let payload = Payload {
            room_id: room.id(),
            groups: Groups::new(vec![GroupItem::new(
                0,
                vec![agent1.agent_id().to_owned(), agent2.agent_id().to_owned()],
            )]),
        };

        let reqp = RequestParams::Http {
            agent_id: &agent1.agent_id(),
        };
        Handler::handle(Arc::new(context.clone()), payload, reqp, Utc::now())
            .await
            .expect("Group update failed");

        let group_agent = db::group_agent::FindQuery::new(room.id())
            .execute(&mut conn)
            .await
            .expect("failed to get groups with participants");

        let groups = group_agent.groups();
        assert_eq!(groups.len(), 1);

        let group_agents = groups
            .iter()
            .flat_map(|g| g.agents().into_iter().map(move |a| (a.clone(), g.number())))
            .collect::<HashMap<_, _>>();

        let agent1_number = group_agents.get(agent1.agent_id()).unwrap();
        assert_eq!(*agent1_number, 0);

        let agent2_number = group_agents.get(agent2.agent_id()).unwrap();
        assert_eq!(*agent2_number, 0);

        let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(
            room.id(),
            &[agent1.agent_id(), agent2.agent_id()],
        )
        .execute(&mut conn)
        .await
        .expect("failed to get rtc reader configs");

        assert_eq!(reader_configs.len(), 2);

        for (cfg, _) in reader_configs {
            assert!(cfg.receive_video());
            assert!(cfg.receive_audio());
        }

        context.janus_clients().remove_client(&backend);
    }
}
