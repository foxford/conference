use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use diesel::PgConnection;
use futures::stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::result::Result as StdResult;
use svc_agent::{
    mqtt::{
        IncomingEventProperties, IncomingRequestProperties, IncomingResponseProperties,
        IntoPublishableMessage, OutgoingEvent, ResponseStatus, ShortTermTimingProperties,
    },
    Addressable, AgentId,
};

use crate::{
    app::{context::Context, endpoint::prelude::*, metrics::HistogramExt},
    db::{self, room::FindQueryable},
};
use tracing_attributes::instrument;

use super::MqttResult;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub struct CorrelationDataPayload {
    reqp: IncomingRequestProperties,
    subject: AgentId,
    object: Vec<String>,
}

impl CorrelationDataPayload {
    pub fn new(reqp: IncomingRequestProperties, subject: AgentId, object: Vec<String>) -> Self {
        Self {
            reqp,
            subject,
            object,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct RoomEnterLeaveEvent {
    id: db::room::Id,
    agent_id: AgentId,
}

impl RoomEnterLeaveEvent {
    pub fn new(id: db::room::Id, agent_id: AgentId) -> Self {
        Self { id, agent_id }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct CreateDeleteResponsePayload {}

///////////////////////////////////////////////////////////////////////////////

pub struct DeleteResponseHandler;

#[async_trait]
impl ResponseHandler for DeleteResponseHandler {
    type Payload = CreateDeleteResponsePayload;
    type CorrelationData = CorrelationDataPayload;

    #[instrument(skip(context, _payload, respp, corr_data))]
    async fn handle<C: Context>(
        context: &mut C,
        _payload: Self::Payload,
        respp: &IncomingResponseProperties,
        corr_data: &Self::CorrelationData,
    ) -> MqttResult {
        ensure_broker(context, respp)?;
        let room_id = try_room_id(&corr_data.object)?;
        let maybe_left = leave_room(context, &corr_data.subject, room_id).await?;
        if maybe_left {
            let response = helpers::build_response(
                ResponseStatus::OK,
                json!({}),
                &corr_data.reqp,
                context.start_timestamp(),
                None,
            );

            let notification = helpers::build_notification(
                "room.leave",
                &format!("rooms/{}/events", room_id),
                RoomEnterLeaveEvent::new(room_id, corr_data.subject.to_owned()),
                corr_data.reqp.tracking(),
                context.start_timestamp(),
            );
            context
                .metrics()
                .request_duration
                .subscription_delete_response
                .observe_timestamp(context.start_timestamp());

            Ok(Box::new(stream::iter(vec![response, notification])))
        } else {
            Err(anyhow!("The agent is not found")).error(AppErrorKind::AgentNotEnteredTheRoom)
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DeleteEventPayload {
    subject: AgentId,
    object: Vec<String>,
}

pub struct DeleteEventHandler;

#[async_trait]
impl EventHandler for DeleteEventHandler {
    type Payload = DeleteEventPayload;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
    ) -> MqttResult {
        ensure_broker(context, evp)?;
        let room_id = try_room_id(&payload.object)?;
        if leave_room(context, &payload.subject, room_id).await? {
            let outgoing_event_payload =
                RoomEnterLeaveEvent::new(room_id, payload.subject.to_owned());
            let short_term_timing = ShortTermTimingProperties::until_now(context.start_timestamp());
            let props = evp.to_event("room.leave", short_term_timing);
            let to_uri = format!("rooms/{}/events", room_id);
            let outgoing_event = OutgoingEvent::broadcast(outgoing_event_payload, props, &to_uri);
            let notification =
                Box::new(outgoing_event) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
            context
                .metrics()
                .request_duration
                .subscription_delete_event
                .observe_timestamp(context.start_timestamp());

            Ok(Box::new(stream::once(std::future::ready(notification))))
        } else {
            Ok(Box::new(stream::empty()))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

fn ensure_broker<C: Context, A: Addressable>(
    context: &mut C,
    sender: &A,
) -> StdResult<(), AppError> {
    if sender.as_account_id() == &context.config().broker_id {
        Ok(())
    } else {
        Err(anyhow!(
            "Expected subscription.delete event to be sent from the broker account '{}', got '{}'",
            context.config().broker_id,
            sender.as_account_id()
        ))
        .error(AppErrorKind::AccessDenied)
    }
}

fn try_room_id(object: &[String]) -> StdResult<db::room::Id, AppError> {
    let object: Vec<&str> = object.iter().map(AsRef::as_ref).collect();

    match object.as_slice() {
        ["rooms", room_id, "events"] => room_id
            .parse()
            .map_err(|err| anyhow!("UUID parse error: {}", err)),
        _ => Err(anyhow!(
            "Bad 'object' format; expected [\"room\", <ROOM_ID>, \"events\"], got: {:?}",
            object
        )),
    }
    .error(AppErrorKind::InvalidSubscriptionObject)
}

#[instrument(skip(context))]
pub async fn leave_room<C: Context>(
    context: &mut C,
    agent_id: &AgentId,
    room_id: db::room::Id,
) -> StdResult<bool, AppError> {
    let conn = context.get_conn().await?;
    let left = crate::util::spawn_blocking({
        let agent_id = agent_id.clone();
        move || {
            let row_count = db::agent::DeleteQuery::new()
                .agent_id(&agent_id)
                .room_id(room_id)
                .execute(&conn)?;

            if row_count < 1 {
                return Ok::<_, AppError>(false);
            }

            make_orphaned_if_host_left(room_id, &agent_id, &conn)?;
            Ok::<_, AppError>(true)
        }
    })
    .await?;
    Ok(left)
}

fn make_orphaned_if_host_left(
    room_id: db::room::Id,
    agent_left: &AgentId,
    connection: &PgConnection,
) -> StdResult<(), diesel::result::Error> {
    let room = db::room::FindQuery::new(room_id).execute(connection)?;
    if room.as_ref().and_then(|x| x.host()) == Some(agent_left) {
        db::orphaned_room::upsert_room(room_id, Utc::now(), connection)?;
    }
    Ok(())
}
