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
        stage::{self, video_group::VideoGroupUpdateJanusConfig, AppStage},
    },
    authz::AuthzObject,
    backend::janus::client::update_agent_reader_config::UpdateReaderConfigRequestBodyConfigItem,
    db::{self, group_agent::Groups},
    outbox::{
        self,
        error::ErrorKind,
        pipeline::{diesel::Pipeline as DieselPipeline, Pipeline},
    },
};
use anyhow::{anyhow, Context};
use axum::{extract::Path, Extension, Json};
use chrono::{DateTime, Utc};
use diesel::Connection;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use svc_agent::mqtt::ResponseStatus;
use svc_conference_events::{EventV1 as Event, VideoGroupEventV1 as VideoGroupEvent};
use svc_utils::extractors::AgentIdExtractor;
use tracing::error;

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
        context: Arc<dyn GlobalContext + Send>,
        payload: Payload,
        reqp: RequestParams<'_>,
        start_timestamp: DateTime<Utc>,
    ) -> RequestResult {
        let outbox_config = context.config().clone().outbox;

        let Payload { room_id, groups } = payload;

        let room = crate::util::spawn_blocking({
            let conn = context.get_conn().await?;
            move || {
                helpers::find_room_by_id(room_id, helpers::RoomTimeRequirement::NotClosed, &conn)
            }
        })
        .await?;

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

        let event_id = crate::util::spawn_blocking({
            let conn = context.get_conn().await?;

            move || {
                conn.transaction(|| {
                    let existed_groups = db::group_agent::FindQuery::new(room.id())
                        .execute(&conn)?
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

                    db::group_agent::UpsertQuery::new(room.id(), &groups).execute(&conn)?;

                    // Update rtc_reader_configs
                    let configs = group_reader_config::update(&conn, room.id(), groups)?;

                    // Generate config items for janus
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
                        .collect();

                    let init_stage = VideoGroupUpdateJanusConfig::init(
                        event,
                        room.classroom_id(),
                        room.id(),
                        backend_id,
                        items,
                    );

                    let serialized_stage = serde_json::to_value(init_stage)
                        .context("serialization failed")
                        .error(AppErrorKind::OutboxStageSerializationFailed)?;

                    let delivery_deadline_at =
                        outbox::util::delivery_deadline_from_now(outbox_config.try_wake_interval);

                    let event_id = outbox::db::diesel::InsertQuery::new(
                        stage::video_group::ENTITY_TYPE,
                        serialized_stage,
                        delivery_deadline_at,
                    )
                    .execute(&conn)?;

                    Ok::<_, AppError>(event_id)
                })
            }
        })
        .await?;

        let pipeline = DieselPipeline::new(
            context.db().clone(),
            outbox_config.try_wake_interval,
            outbox_config.max_delivery_interval,
        );

        if let Err(err) = pipeline
            .run_single_stage::<AppStage, _>(context.clone(), event_id)
            .await
        {
            if let ErrorKind::StageError(kind) = &err.kind {
                context.metrics().observe_outbox_error(kind);
            }

            error!(%err, "failed to complete stage");
            AppError::from(err).notify_sentry();
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
        factory,
        prelude::{GlobalContext, TestAgent, TestAuthz, TestContext, TestDb},
        shared_helpers,
        test_deps::LocalDeps,
        USR_AUDIENCE,
    };
    use chrono::{Duration, Utc};
    use std::collections::HashMap;
    use std::ops::Bound;

    #[tokio::test]
    async fn missing_room() -> std::io::Result<()> {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let db = TestDb::with_local_postgres(&postgres);
        let agent = TestAgent::new("web", "user1", USR_AUDIENCE);
        let context = TestContext::new(db, TestAuthz::new());

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

        let context = TestContext::new(db, TestAuthz::new());

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

        // Allow agent to update the room.
        let mut authz = TestAuthz::new();
        let classroom_id = room.classroom_id().to_string();
        authz.allow(
            agent1.account_id(),
            vec!["classrooms", &classroom_id],
            "update",
        );

        let context = TestContext::new(db, authz);
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

                factory::GroupAgent::new(
                    room.id(),
                    Groups::new(vec![GroupItem::new(0, vec![]), GroupItem::new(1, vec![])]),
                )
                .upsert(&conn);

                vec![&agent1, &agent2].iter().for_each(|agent| {
                    factory::Rtc::new(room.id())
                        .created_by(agent.agent_id().to_owned())
                        .insert(&conn);
                });

                (room, backend)
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

        let mut context = TestContext::new(db.clone(), authz);
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

        let conn = db
            .connection_pool()
            .get()
            .expect("Failed to get DB connection");

        let group_agent = db::group_agent::FindQuery::new(room.id())
            .execute(&conn)
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
            .execute(&conn)
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
