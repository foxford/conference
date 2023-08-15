use chrono::Utc;
use sqlx::Connection;
use std::sync::Arc;
use svc_agent::AgentId;
use svc_events::{
    Event, EventId, EventV1, VideoGroupCreateIntentEventV1 as VideoGroupCreateIntentEvent,
    VideoGroupDeleteIntentEventV1 as VideoGroupDeleteIntentEvent, VideoGroupEventV1,
    VideoGroupUpdateIntentEventV1 as VideoGroupUpdateIntentEvent,
};
use svc_nats_client::consumer::{FailureKind, HandleMessageFailure};
use uuid::Uuid;

use crate::{
    app::{
        error::{Error, ErrorExt, ErrorKind},
        group_reader_config,
        stage::video_group::{send_mqtt_notification, update_janus_config},
        AppError, AppErrorKind, GlobalContext,
    },
    backend::janus::client::update_agent_reader_config::UpdateReaderConfigRequestBodyConfigItem,
    client::nats,
    db,
};

const ENTITY_TYPE: &str = "video_group";

const CREATE_INTENT_OP: &str = "create_intent";
const DELETE_INTENT_OP: &str = "delete_intent";
const UPDATE_INTENT_OP: &str = "update_intent";

const CREATE_COMPLETED_OP: &str = "create_completed";
const DELETE_COMPLETED_OP: &str = "delete_completed";
const UPDATE_COMPLETED_OP: &str = "update_completed";

pub async fn save_create_intent(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    room: db::room::Object,
    backend_id: AgentId,
) -> Result<EventId, Error> {
    save_intent(
        ctx,
        room,
        CREATE_INTENT_OP,
        EventV1::VideoGroupCreateIntent(VideoGroupCreateIntentEvent {
            created_at: Utc::now().timestamp_nanos(),
            backend_id,
        }),
    )
    .await
}

pub async fn save_delete_intent(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    room: db::room::Object,
    backend_id: AgentId,
) -> Result<EventId, Error> {
    save_intent(
        ctx,
        room,
        DELETE_INTENT_OP,
        EventV1::VideoGroupDeleteIntent(VideoGroupDeleteIntentEvent {
            created_at: Utc::now().timestamp_nanos(),
            backend_id,
        }),
    )
    .await
}

pub async fn save_update_intent(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    room: db::room::Object,
    backend_id: AgentId,
) -> Result<EventId, Error> {
    save_intent(
        ctx,
        room,
        UPDATE_INTENT_OP,
        EventV1::VideoGroupUpdateIntent(VideoGroupUpdateIntentEvent {
            created_at: Utc::now().timestamp_nanos(),
            backend_id,
        }),
    )
    .await
}

pub async fn handle_intent(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    event_id: &EventId,
    room: db::room::Object,
    agent_id: AgentId,
    classroom_id: Uuid,
    backend_id: AgentId,
    event: VideoGroupEventV1,
) -> Result<(), HandleMessageFailure<Error>> {
    let configs = {
        let mut conn = ctx
            .get_conn()
            .await
            .error(AppErrorKind::DbConnAcquisitionFailed)
            .transient()?;
        let room = room.clone();
        conn.transaction::<_, _, AppError>(|conn| {
            Box::pin(async move {
                let group_agent = db::group_agent::FindQuery::new(room.id())
                    .execute(conn)
                    .await
                    .error(AppErrorKind::DbQueryFailed)?;

                let mut groups = group_agent.groups();
                if !groups.is_agent_exist(&agent_id) {
                    groups = groups.add_to_default_group(&agent_id);
                }

                db::group_agent::UpsertQuery::new(room.id(), &groups)
                    .execute(conn)
                    .await
                    .error(AppErrorKind::DbQueryFailed)?;

                // Update rtc_reader_configs
                let configs = group_reader_config::update(conn, room.id(), groups)
                    .await
                    .error(AppErrorKind::DbQueryFailed)?;

                Ok(configs)
            })
        })
        .await
        .error(AppErrorKind::DbQueryFailed)
        .transient()?
    };

    // Generate configs for janus
    let items = configs
        .into_iter()
        .map(
            |((rtc_id, agent_id), value)| UpdateReaderConfigRequestBodyConfigItem {
                reader_id: agent_id,
                stream_id: rtc_id,
                receive_video: value,
                receive_audio: value,
            },
        )
        .collect::<Vec<_>>();

    update_janus_config(ctx.clone(), backend_id, items)
        .await
        .error(ErrorKind::JanusConfigUpdatingFailed)
        .transient()?;

    let event_id = &EventId::from((
        ENTITY_TYPE.to_string(),
        event_to_operation(&event),
        event_id.sequence_id(),
    ));
    let event = Event::from(event);

    nats::publish_event(ctx.clone(), classroom_id, event_id, event)
        .await
        .error(ErrorKind::NatsPublishFailed)
        .transient()?;

    send_mqtt_notification(ctx, room.id)
        .await
        .error(ErrorKind::MqttPublishFailed)
        .transient()?;

    Ok(())
}

async fn save_intent(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    room: db::room::Object,
    operation: &str,
    event: EventV1,
) -> Result<EventId, Error> {
    let sequence_id = get_next_sequence_id(ctx.clone()).await?;
    let event_id = EventId::from((ENTITY_TYPE.to_string(), operation.to_string(), sequence_id));
    let event = svc_events::Event::from(event);

    nats::publish_event(ctx.clone(), room.classroom_id(), &event_id, event).await?;

    Ok(event_id)
}

async fn get_next_sequence_id(ctx: Arc<dyn GlobalContext + Sync + Send>) -> Result<i64, Error> {
    let mut conn = ctx.get_conn().await?;
    let value = db::video_group_op::get_next_seq_id(&mut conn)
        .await
        .error(AppErrorKind::CreatingNewSequenceIdFailed)?
        .value;

    Ok(value)
}

fn event_to_operation(event: &VideoGroupEventV1) -> String {
    match &event {
        VideoGroupEventV1::Created { .. } => CREATE_COMPLETED_OP.to_string(),
        VideoGroupEventV1::Deleted { .. } => DELETE_COMPLETED_OP.to_string(),
        VideoGroupEventV1::Updated { .. } => UPDATE_COMPLETED_OP.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{
        db::TestDb,
        factory,
        prelude::{TestAgent, TestAuthz, TestContext},
        shared_helpers,
        test_deps::LocalDeps,
        USR_AUDIENCE,
    };
    use crate::{
        app::service_utils::RequestParams,
        db::{
            group_agent::{GroupItem, Groups},
            rtc::SharingPolicy as RtcSharingPolicy,
        },
    };
    use chrono::{Duration, Utc};
    use std::{collections::HashMap, ops::Bound};
    use svc_authn::AccountId;

    #[sqlx::test]
    async fn handle_intent_test(pool: sqlx::PgPool) {
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

        let agent_id = AgentId::new(
            "instance01",
            AccountId::new("service_name", "svc.example.org"),
        );
        let event_id = EventId::from((ENTITY_TYPE.to_string(), CREATE_INTENT_OP.to_string(), 1));

        let event = VideoGroupEventV1::Created {
            created_at: Utc::now().timestamp_nanos(),
        };

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
        let classroom_id = room.classroom_id();
        authz.allow(
            agent1.account_id(),
            vec!["classrooms", &classroom_id.to_string()],
            "update",
        );

        let mut context = TestContext::new(db, authz).await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        context.with_janus(tx);

        handle_intent(
            context,
            &event_id,
            room,
            agent_id,
            classroom_id,
            backend.id().clone(),
            event,
        )
        .await;

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

        // handle_intent(
        //     context,
        //     event_id,
        //     room,
        //     agent_id,
        //     classroom_id,
        //     backend_id,
        //     event,
        // ).await;

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
