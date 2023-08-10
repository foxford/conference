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

const CREATE_COMPLETED_OP: &str = "create_complited";
const DELETE_COMPLETED_OP: &str = "delete_complited";
const UPDATE_COMPLETED_OP: &str = "update_complited";

pub async fn save_create_intent(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    room: db::room::Object,
    backend_id: AgentId,
) -> Result<EventId, Error> {
    let secuence_id = get_next_secuence_id(ctx.clone()).await?;
    let event_id = EventId::from((
        ENTITY_TYPE.to_string(),
        CREATE_INTENT_OP.to_string(),
        secuence_id,
    ));

    let created_at = Utc::now().timestamp_nanos();
    let event = svc_events::Event::from(EventV1::VideoGroupCreateIntent(
        VideoGroupCreateIntentEvent {
            created_at,
            backend_id,
        },
    ));

    nats::publish_event(ctx.clone(), room.classroom_id(), &event_id, event).await?;

    Ok(event_id)
}

pub async fn save_delete_intent(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    room: db::room::Object,
    backend_id: AgentId,
) -> Result<EventId, Error> {
    let secuence_id = get_next_secuence_id(ctx.clone()).await?;
    let event_id = EventId::from((
        ENTITY_TYPE.to_string(),
        DELETE_INTENT_OP.to_string(),
        secuence_id,
    ));

    let created_at = Utc::now().timestamp_nanos();
    let event = svc_events::Event::from(EventV1::VideoGroupDeleteIntent(
        VideoGroupDeleteIntentEvent {
            created_at,
            backend_id,
        },
    ));

    nats::publish_event(ctx.clone(), room.classroom_id(), &event_id, event).await?;

    Ok(event_id)
}

pub async fn save_update_intent(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    room: db::room::Object,
    backend_id: AgentId,
) -> Result<EventId, Error> {
    let secuence_id = get_next_secuence_id(ctx.clone()).await?;
    let event_id = EventId::from((
        ENTITY_TYPE.to_string(),
        UPDATE_INTENT_OP.to_string(),
        secuence_id,
    ));

    let created_at = Utc::now().timestamp_nanos();
    let event = svc_events::Event::from(EventV1::VideoGroupUpdateIntent(
        VideoGroupUpdateIntentEvent {
            created_at,
            backend_id,
        },
    ));

    nats::publish_event(ctx.clone(), room.classroom_id(), &event_id, event).await?;

    Ok(event_id)
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
        .error(ErrorKind::StageProcessingFailed)
        .transient()?;

    let event_id = &EventId::from((
        ENTITY_TYPE.to_string(),
        event_to_operation(&event),
        event_id.sequence_id(),
    ));
    let event = Event::from(event);

    nats::publish_event(ctx.clone(), classroom_id, event_id, event)
        .await
        .error(ErrorKind::StageProcessingFailed)
        .transient()?;

    send_mqtt_notification(ctx, room.id)
        .await
        .error(ErrorKind::StageProcessingFailed)
        .transient()?;

    Ok(())
}

async fn get_next_secuence_id(ctx: Arc<dyn GlobalContext + Sync + Send>) -> Result<i64, Error> {
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
