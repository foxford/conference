use chrono::Utc;
use std::sync::Arc;
use svc_agent::AgentId;
use svc_events::{
    EventId, EventV1, VideoGroupCreateIntentEventV1 as VideoGroupCreateIntentEvent,
    VideoGroupDeleteIntentEventV1 as VideoGroupDeleteIntentEvent,
    VideoGroupUpdateIntentEventV1 as VideoGroupUpdateIntentEvent,
};

use crate::{
    app::{
        error::{Error, ErrorExt, ErrorKind as AppErrorKind},
        GlobalContext,
    },
    client::nats,
    db,
};

const ENTITY_TYPE: &str = "video_group";
const CREATE_INTENT_OP: &str = "video_group_create_intent";
const DELETE_INTENT_OP: &str = "video_group_delete_intent";
const UPDATE_INTENT_OP: &str = "video_group_update_intent";

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

async fn get_next_secuence_id(ctx: Arc<dyn GlobalContext + Sync + Send>) -> Result<i64, Error> {
    let mut conn = ctx.get_conn().await?;
    let value = db::nats_id::get_next_seq_id(&mut conn)
        .await
        .error(AppErrorKind::CreatingNewSequenceIdFailed)?
        .value;

    Ok(value)
}
