use anyhow::{anyhow, Context as AnyhowContext};
use async_trait::async_trait;
use axum::{
    extract::{Extension, Path},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::Connection;
use std::{ops::Bound, sync::Arc};
use svc_agent::{
    mqtt::{OutgoingRequest, ResponseStatus, ShortTermTimingProperties, SubscriptionTopic},
    Addressable, AgentId, Authenticable, Subscription,
};
use svc_events::{
    stage::UpdateJanusConfigAndSendNotificationStageV1, VideoGroupEventV1 as VideoGroupEvent,
};

use svc_utils::extractors::AgentIdExtractor;
use tracing_attributes::instrument;
use uuid::Uuid;

use crate::{
    app::{
        context::{AppContext, Context, GlobalContext},
        endpoint::{
            prelude::*,
            rtc::{RtcCreate, RtcCreateResult},
            subscription::CorrelationDataPayload,
        },
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
        stage::video_group::{
            MQTT_NOTIFICATION_LABEL, SUBJECT_PREFIX,
        },
        API_VERSION,
    },
    authz::AuthzObject,
    client::mqtt_gateway::MqttGatewayClient,
    db::{
        self,
        group_agent::{GroupItem, Groups},
        room::{RoomBackend, Time},
        rtc::SharingPolicy as RtcSharingPolicy,
    },
};

use super::subscription::RoomEnterLeaveEvent;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
struct SubscriptionRequest {
    subject: AgentId,
    object: Vec<String>,
}

impl SubscriptionRequest {
    fn new(subject: AgentId, object: Vec<String>) -> Self {
        Self { subject, object }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct CreateRequest {
    #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
    time: Time,
    audience: String,
    // Deprecated in favor of `rtc_sharing_policy`.
    #[serde(default)]
    backend: Option<RoomBackend>,
    #[serde(default)]
    rtc_sharing_policy: Option<RtcSharingPolicy>,
    reserve: Option<i32>,
    tags: Option<JsonValue>,
    classroom_id: Uuid,
}

pub async fn create(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Json(request): Json<CreateRequest>,
) -> RequestResult {
    CreateHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct CreateHandler;

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;
    const ERROR_TITLE: &'static str = "Failed to create room";

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        // Prefer `rtc_sharing_policy` with fallback to `backend` and `None` as default.
        let rtc_sharing_policy = payload
            .rtc_sharing_policy
            .or_else(|| payload.backend.map(|b| b.into()))
            .unwrap_or(RtcSharingPolicy::None);

        // Authorize room creation on the tenant.
        let authz_time = context
            .authz()
            .authorize(
                payload.audience.clone(),
                reqp,
                AuthzObject::new(&["classrooms"]).into(),
                "create".into(),
            )
            .await?;
        context.metrics().observe_auth(authz_time);

        // Create a room.
        let audience = payload.audience.clone();
        let mut conn = context.get_conn().await?;
        let mut q = db::room::InsertQuery::new(
            payload.time,
            &payload.audience,
            rtc_sharing_policy,
            payload.classroom_id,
        );

        if let Some(reserve) = payload.reserve {
            q = q.reserve(reserve);
        }

        if let Some(ref tags) = payload.tags {
            q = q.tags(tags);
        }

        let room = q.execute(&mut conn).await?;

        // Create a default group for minigroups
        if room.rtc_sharing_policy() == db::rtc::SharingPolicy::Owned {
            let mut conn = context.get_conn().await?;
            let groups = Groups::new(vec![GroupItem::new(0, vec![])]);
            db::group_agent::UpsertQuery::new(room.id(), &groups)
                .execute(&mut conn)
                .await?;
        }

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Respond and broadcast to the audience topic.
        let mut response = Response::new(
            // TODO: Change to `ResponseStatus::CREATED` (breaking).
            ResponseStatus::OK,
            room.clone(),
            context.start_timestamp(),
            Some(authz_time),
        );

        response.add_notification(
            "room.create",
            &format!("audiences/{audience}/events"),
            room,
            context.start_timestamp(),
        );

        Ok(response)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Clone)]
pub struct ReadRequest {
    id: db::room::Id,
}

pub async fn read(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = ReadRequest { id: room_id };
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
    const ERROR_TITLE: &'static str = "Failed to read room";

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let room = {
            let mut conn = context.get_conn().await?;
            helpers::find_room_by_id(payload.id, helpers::RoomTimeRequirement::Any, &mut conn)
                .await?
        };

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize room reading on the tenant.
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "read".into())
            .await?;
        context.metrics().observe_auth(authz_time);
        context
            .metrics()
            .request_duration
            .room_read
            .observe_timestamp(context.start_timestamp());

        Ok(Response::new(
            ResponseStatus::OK,
            room,
            context.start_timestamp(),
            Some(authz_time),
        ))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct UpdateRequest {
    id: db::room::Id,
    #[serde(default)]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<db::room::Time>,
    reserve: Option<Option<i32>>,
    tags: Option<JsonValue>,
    classroom_id: Option<Uuid>,
    host: Option<AgentId>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateFields {
    #[serde(default)]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<db::room::Time>,
    reserve: Option<Option<i32>>,
    tags: Option<JsonValue>,
    classroom_id: Option<Uuid>,
    host: Option<AgentId>,
}

pub async fn update(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    Json(request): Json<UpdateFields>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = UpdateRequest {
        id: room_id,
        time: request.time,
        reserve: request.reserve,
        tags: request.tags,
        classroom_id: request.classroom_id,
        host: request.host,
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
    type Payload = UpdateRequest;
    const ERROR_TITLE: &'static str = "Failed to update room";

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let time_requirement = if payload.time.is_some() {
            // Forbid changing time of a closed room.
            helpers::RoomTimeRequirement::NotClosedOrUnboundedOpen
        } else {
            helpers::RoomTimeRequirement::Any
        };

        let room = {
            let mut conn = context.get_conn().await?;
            helpers::find_room_by_id(payload.id, time_requirement, &mut conn).await?
        };

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize room updating on the tenant.
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "update".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        let room_was_open = !room.is_closed();

        // Update room.
        let room = {
            let mut conn = context.get_conn().await?;

            let time = match payload.time {
                None => None,
                Some(new_time) => {
                    match new_time {
                        (Bound::Included(o), Bound::Excluded(c)) if o < c => (),
                        (Bound::Included(_), Bound::Unbounded) => (),
                        _ => {
                            return Err(anyhow!("Invalid room time"))
                                .error(AppErrorKind::InvalidRoomTime)
                        }
                    };

                    match room.time() {
                        (_, Bound::Unbounded) => match new_time {
                            (_, Bound::Excluded(_)) if room.infinite() => {
                                return Err(anyhow!("Setting closing time is not allowed in this room since it's infinite"))
                                .error(AppErrorKind::RoomTimeChangingForbidden);
                            }
                            // Allow any change when no closing date specified.
                            _ => Some(new_time),
                        },
                        (Bound::Included(o), Bound::Excluded(c)) if c > Utc::now() => {
                            match new_time {
                                // Allow reschedule future closing.
                                (_, Bound::Excluded(nc)) => {
                                    let nc = std::cmp::max(nc, Utc::now());
                                    Some((Bound::Included(o), Bound::Excluded(nc)))
                                }
                                _ => {
                                    return Err(anyhow!("Setting unbounded closing time is not allowed in this room anymore"))
                                        .error(AppErrorKind::RoomTimeChangingForbidden);
                                }
                            }
                        }
                        _ => {
                            return Err(anyhow!("Room has been already closed"))
                                .error(AppErrorKind::RoomTimeChangingForbidden);
                        }
                    }
                }
            };

            db::room::UpdateQuery::new(room.id())
                .time(time)
                .reserve(payload.reserve)
                .tags(payload.tags)
                .classroom_id(payload.classroom_id)
                .host(payload.host.as_ref())
                .execute(&mut conn)
                .await?
        };

        // Respond and broadcast to the audience topic.
        let mut response = Response::new(
            ResponseStatus::OK,
            room.clone(),
            context.start_timestamp(),
            Some(authz_time),
        );

        response.add_notification(
            "room.update",
            &format!("audiences/{}/events", room.audience()),
            room.clone(),
            context.start_timestamp(),
        );

        // Publish room closed notification.
        if let (_, Bound::Excluded(closed_at)) = room.time() {
            if room_was_open && closed_at <= Utc::now() {
                let room = {
                    let mut conn = context.get_conn().await?;
                    db::room::set_closed_by(room.id(), reqp.as_agent_id(), &mut conn).await?
                };

                response.add_notification(
                    "room.close",
                    &format!("rooms/{}/events", room.id()),
                    room.clone(),
                    context.start_timestamp(),
                );

                response.add_notification(
                    "room.close",
                    &format!("audiences/{}/events", room.audience()),
                    room,
                    context.start_timestamp(),
                );
            }
        }
        context
            .metrics()
            .request_duration
            .room_update
            .observe_timestamp(context.start_timestamp());

        Ok(response)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct CloseRequest {
    id: db::room::Id,
}

pub async fn close(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = CloseRequest { id: room_id };
    CloseHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct CloseHandler;

#[async_trait]
impl RequestHandler for CloseHandler {
    type Payload = CloseRequest;
    const ERROR_TITLE: &'static str = "Failed to close room";

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let room = {
            let mut conn = context.get_conn().await?;
            helpers::find_room_by_id(
                payload.id,
                helpers::RoomTimeRequirement::NotClosedOrUnboundedOpen,
                &mut conn,
            )
            .await?
        };

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        if room.infinite() {
            return Err(anyhow!("Not closing this room because its infinite"))
                .error(AppErrorKind::RoomTimeChangingForbidden);
        }

        // Authorize room updating on the tenant.
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "update".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        // Update room.
        let room = {
            let mut conn = context.get_conn().await?;
            db::room::set_closed_by(room.id(), reqp.as_agent_id(), &mut conn).await?
        };

        // Respond and broadcast to the audience topic.
        let mut response = Response::new(
            ResponseStatus::OK,
            room.clone(),
            context.start_timestamp(),
            Some(authz_time),
        );

        response.add_notification(
            "room.update",
            &format!("audiences/{}/events", room.audience()),
            room.clone(),
            context.start_timestamp(),
        );

        response.add_notification(
            "room.close",
            &format!("rooms/{}/events", room.id()),
            room.clone(),
            context.start_timestamp(),
        );

        response.add_notification(
            "room.close",
            &format!("audiences/{}/events", room.audience()),
            room,
            context.start_timestamp(),
        );

        context
            .metrics()
            .request_duration
            .room_close
            .observe_timestamp(context.start_timestamp());

        Ok(response)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize)]
pub struct EnterPayload {
    #[serde(default)]
    agent_label: Option<String>,
}

pub async fn enter(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    payload: Option<Json<EnterPayload>>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = EnterRequest { id: room_id };

    let agent_id = payload
        .and_then(|p| {
            p.agent_label
                .as_ref()
                .map(|label| AgentId::new(label, agent_id.as_account_id().to_owned()))
        })
        .unwrap_or(agent_id);

    EnterHandler::handle(
        ctx,
        request.clone(),
        RequestParams::Http {
            agent_id: &agent_id,
        },
        Utc::now(),
    )
    .await
}

pub type EnterRequest = ReadRequest;
pub struct EnterHandler;

impl EnterHandler {
    async fn handle(
        context: Arc<dyn GlobalContext + Send + Sync>,
        payload: EnterRequest,
        reqp: RequestParams<'_>,
        start_timestamp: DateTime<Utc>,
    ) -> RequestResult {
        let room = {
            let mut conn = context.get_conn().await?;
            helpers::find_room_by_id(
                payload.id,
                helpers::RoomTimeRequirement::NotClosed,
                &mut conn,
            )
            .await?
        };

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize subscribing to the room's events.
        let room_id = room.id().to_string();
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "read".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        // Register agent in `in_progress` state.
        {
            let mut conn = context.get_conn().await?;
            db::agent::InsertQuery::new(reqp.as_agent_id(), room.id())
                .execute(&mut conn)
                .await?;
        }

        // Send dynamic subscription creation request to the broker.
        let subject = reqp.as_agent_id().to_owned();
        let object = ["rooms", &room_id, "events"];

        tracing::info!(
            "send dynsub request to mqtt gateway {} {:?}",
            subject,
            object
        );

        context
            .mqtt_gateway_client()
            .create_subscription(subject.clone(), &object)
            .await
            .error(AppErrorKind::BrokerRequestFailed)?;

        {
            let mut conn = context.get_conn().await?;

            if room.host() == Some(&subject) {
                db::orphaned_room::remove_room(room.id(), &mut conn).await?;
            }

            // Update agent state to `ready`.
            db::agent::UpdateQuery::new(&subject, room.id())
                .status(db::agent::Status::Ready)
                .execute(&mut conn)
                .await?;
        }

        let mut response = Response::new(ResponseStatus::OK, json!({}), start_timestamp, None);

        let ctx = context.clone();
        let room_id = room.id();
        if room.rtc_sharing_policy() == db::rtc::SharingPolicy::Owned {
            let mut conn = context.get_conn().await?;
            let rtcs = db::rtc::ListQuery::new()
                .room_id(room_id)
                .created_by(&[reqp.as_agent_id()])
                .execute(&mut conn)
                .await?;

            let rtc = rtcs.into_iter().next();

            if rtc.is_none() {
                let RtcCreateResult {
                    rtc,
                    authz_time,
                    notification_label,
                    notification_topic,
                } = RtcCreate {
                    ctx: context.as_ref(),
                    room: either::Either::Left(room.clone()),
                    reqp,
                }
                .run()
                .await?;

                response.set_authz_time(authz_time);

                response.add_notification(
                    notification_label,
                    &notification_topic,
                    rtc,
                    start_timestamp,
                );
            }

            // Adds participants to the default group for minigroups
            let mut conn = context.get_conn().await?;
            let agent_id = reqp.as_agent_id().clone();

            let maybe_event_id = {
                let ctx = ctx.clone();
                conn.transaction::<_, _, AppError>(|conn| {
                    Box::pin(async move {
                        let group_agent = db::group_agent::FindQuery::new(room_id)
                            .execute(conn)
                            .await?;

                        let groups = group_agent.groups();
                        let agent_exists = groups.is_agent_exist(&agent_id);

                        if !agent_exists {
                            // Check the number of groups, and if there are more than 1,
                            // then create RTC reader configs for participants from other groups
                            if groups.len() > 1 {
                                let backend_id = room
                                    .backend_id()
                                    .cloned()
                                    .context("backend not found")
                                    .error(AppErrorKind::BackendNotFound)?;

                                let timestamp = Utc::now().timestamp_nanos();
                                let event = VideoGroupEvent::Updated {
                                    created_at: timestamp,
                                };

                                let event_id =
                                    crate::app::stage::nats_ids::sqlx::get_next_seq_id(conn)
                                        .await
                                        .error(AppErrorKind::CreatingNewSequenceIdFailed)?
                                        .to_event_id("update configs");

                                let event = svc_events::Event::from(UpdateJanusConfigAndSendNotificationStageV1 {
                                    backend_id,
                                    event,
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
                                    ctx.agent_id().to_owned(),
                                )
                                .build();

                                ctx.nats_client()
                                    .ok_or_else(|| anyhow!("nats client not found"))
                                    .error(AppErrorKind::NatsClientNotFound)?
                                    .publish(&event)
                                    .await
                                    .error(AppErrorKind::NatsPublishFailed)?;

                                return Ok(Some(event_id));
                            }
                        }

                        Ok(None)
                    })
                })
                .await?
            };

            match maybe_event_id {
                Some(_event_id) => (),
                None => {
                    response.add_notification(
                        MQTT_NOTIFICATION_LABEL,
                        &format!("rooms/{room_id}/events"),
                        json!({}),
                        start_timestamp,
                    );
                }
            }
        };

        response.add_notification(
            "room.enter",
            &format!("rooms/{room_id}/events"),
            RoomEnterLeaveEvent::new(room_id, subject),
            start_timestamp,
        );

        context
            .metrics()
            .request_duration
            .room_enter
            .observe_timestamp(start_timestamp);

        Ok(response)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub type LeaveRequest = ReadRequest;
pub struct LeaveHandler;

#[async_trait]
impl RequestHandler for LeaveHandler {
    type Payload = LeaveRequest;
    const ERROR_TITLE: &'static str = "Failed to leave room";

    #[instrument(skip(context, payload, reqp), fields(room_id = %payload.id))]
    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let mqtt_params = reqp.as_mqtt_params()?;

        let mut conn = context.get_conn().await?;

        let room =
            helpers::find_room_by_id(payload.id, helpers::RoomTimeRequirement::Any, &mut conn)
                .await?;

        let agent_id = reqp.as_agent_id().clone();
        // Check room presence.
        let presence = db::agent::ListQuery::new()
            .room_id(room.id())
            .agent_id(&agent_id)
            .execute(&mut conn)
            .await?;

        if presence.is_empty() {
            return Err(anyhow!("Agent is not online in the room"))
                .error(AppErrorKind::AgentNotEnteredTheRoom);
        }

        // Send dynamic subscription deletion request to the broker.
        let subject = reqp.as_agent_id().to_owned();
        let room_id = room.id().to_string();
        let object = vec![String::from("rooms"), room_id, String::from("events")];
        let payload = SubscriptionRequest::new(subject.clone(), object.clone());

        let broker_id = AgentId::new("nevermind", context.config().broker_id.to_owned());

        let response_topic = Subscription::unicast_responses_from(&broker_id)
            .subscription_topic(context.agent_id(), API_VERSION)
            .context("Failed to build response topic")
            .error(AppErrorKind::BrokerRequestFailed)?;

        let corr_data_payload =
            CorrelationDataPayload::new(mqtt_params.to_owned(), subject, object);

        let corr_data = CorrelationData::SubscriptionDelete(corr_data_payload)
            .dump()
            .context("Failed to dump correlation data")
            .error(AppErrorKind::BrokerRequestFailed)?;

        let timing = ShortTermTimingProperties::until_now(context.start_timestamp());

        let props =
            mqtt_params.to_request("subscription.delete", &response_topic, &corr_data, timing);
        let to = &context.config().broker_id;
        let outgoing_request = OutgoingRequest::multicast(payload, props, to, API_VERSION);
        let mut response = Response::new(
            ResponseStatus::OK,
            json!({}),
            context.start_timestamp(),
            None,
        );
        response.add_message(Box::new(outgoing_request));
        context
            .metrics()
            .request_duration
            .room_leave
            .observe_timestamp(context.start_timestamp());

        Ok(response)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use serde::Deserialize;

    use super::AgentId;

    #[derive(Deserialize)]
    struct DynSubRequest {
        subject: AgentId,
        object: Vec<String>,
    }

    mod create {
        use std::ops::Bound;

        use chrono::Utc;
        use serde_json::json;

        use crate::db::group_agent::{GroupItem, Groups};
        use crate::{
            db::room::Object as Room,
            test_helpers::{db::TestDb, prelude::*},
        };

        use super::super::*;

        #[sqlx::test]
        async fn create(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            // Allow user to create rooms.
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            authz.allow(agent.account_id(), vec!["classrooms"], "create");

            // Make room.create request.
            let mut context = TestContext::new(db, authz).await;
            let time = (Bound::Unbounded, Bound::Unbounded);
            let classroom_id = Uuid::new_v4();

            let payload = CreateRequest {
                time,
                audience: USR_AUDIENCE.to_owned(),
                backend: None,
                rtc_sharing_policy: Some(db::rtc::SharingPolicy::Shared),
                reserve: Some(123),
                tags: Some(json!({ "foo": "bar" })),
                classroom_id,
            };

            let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect("Room creation failed");

            // Assert response.
            let (room, respp, _) = find_response::<Room>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(room.audience(), USR_AUDIENCE);
            assert_eq!(room.time(), time);
            assert_eq!(room.rtc_sharing_policy(), db::rtc::SharingPolicy::Shared);
            assert_eq!(room.reserve(), Some(123));
            assert_eq!(room.tags(), &json!({ "foo": "bar" }));
            assert_eq!(room.classroom_id(), classroom_id);

            // Assert notification.
            let (room, evp, topic) = find_event::<Room>(messages.as_slice());
            assert!(topic.ends_with(&format!("/audiences/{}/events", USR_AUDIENCE)));
            assert_eq!(evp.label(), "room.create");
            assert_eq!(room.audience(), USR_AUDIENCE);
            assert_eq!(room.time(), time);
            assert_eq!(room.rtc_sharing_policy(), db::rtc::SharingPolicy::Shared);
            assert_eq!(room.reserve(), Some(123));
            assert_eq!(room.tags(), &json!({ "foo": "bar" }));
            assert_eq!(room.classroom_id(), classroom_id);
        }

        #[sqlx::test]
        async fn create_room_unauthorized(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let mut context = TestContext::new(db, TestAuthz::new()).await;
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Make room.create request.
            let payload = CreateRequest {
                time: (Bound::Included(Utc::now()), Bound::Unbounded),
                audience: USR_AUDIENCE.to_owned(),
                backend: None,
                rtc_sharing_policy: Some(db::rtc::SharingPolicy::Shared),
                reserve: None,
                tags: None,
                classroom_id: Uuid::new_v4(),
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room creation");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }

        #[sqlx::test]
        async fn create_default_group(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            // Allow user to create rooms.
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            authz.allow(agent.account_id(), vec!["classrooms"], "create");

            // Make room.create request.
            let mut context = TestContext::new(db, authz).await;
            let time = (Bound::Unbounded, Bound::Unbounded);
            let classroom_id = Uuid::new_v4();

            let payload = CreateRequest {
                time: time.clone(),
                audience: USR_AUDIENCE.to_owned(),
                backend: None,
                rtc_sharing_policy: Some(db::rtc::SharingPolicy::Owned),
                reserve: Some(123),
                tags: Some(json!({ "foo": "bar" })),
                classroom_id,
            };

            let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect("Room creation failed");

            let (room, _, _) = find_response::<Room>(messages.as_slice());

            let mut conn = context
                .get_conn()
                .await
                .expect("failed to get db connection");

            let group_agent = db::group_agent::FindQuery::new(room.id())
                .execute(&mut conn)
                .await
                .expect("failed to get group");

            let groups = Groups::new(vec![GroupItem::new(0, vec![])]);
            assert_eq!(group_agent.groups(), groups);
        }
    }

    mod read {
        use crate::{
            db::room::Object as Room,
            test_helpers::{db::TestDb, prelude::*},
        };

        use super::super::*;

        #[sqlx::test]
        async fn read_room(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let room = {
                let mut conn = db.get_conn().await;
                // Create room.
                shared_helpers::insert_room(&mut conn).await
            };

            // Allow agent to read the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );

            // Make room.read request.
            let mut context = TestContext::new(db, authz).await;
            let payload = ReadRequest { id: room.id() };

            let messages = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect("Room reading failed");

            // Assert response.
            let (resp_room, respp, _) = find_response::<Room>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(resp_room.audience(), room.audience());
            assert_eq!(resp_room.time(), room.time());
            assert_eq!(resp_room.rtc_sharing_policy(), room.rtc_sharing_policy());
        }

        #[sqlx::test]
        async fn read_room_not_authorized(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let mut context = TestContext::new(db, TestAuthz::new()).await;
            let payload = ReadRequest { id: room.id() };

            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room reading");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }

        #[sqlx::test]
        async fn read_room_missing(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new()).await;
            let payload = ReadRequest {
                id: db::room::Id::random(),
            };

            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room reading");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }
    }

    mod update {
        use std::ops::Bound;

        use chrono::{Duration, SubsecRound, Utc};
        use serde_json::json;
        use uuid::Uuid;

        use crate::{
            db::room::Object as Room,
            test_helpers::{db::TestDb, find_event_by_predicate, prelude::*},
        };

        use super::super::*;

        #[sqlx::test]
        async fn update_room(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let now = Utc::now().trunc_subsecs(0);

            let room = {
                let mut conn = db.get_conn().await;

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Unbounded, Bound::Unbounded))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .insert(&mut conn)
                    .await
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make room.update request.
            let mut context = TestContext::new(db, authz).await;
            let classroom_id = Uuid::new_v4();

            let time = (
                Bound::Included(now + Duration::minutes(50)),
                Bound::Unbounded,
            );

            let payload = UpdateRequest {
                id: room.id(),
                time: Some(time),
                reserve: Some(Some(123)),
                tags: Some(json!({"foo": "bar"})),
                classroom_id: Some(classroom_id),
                host: Some(agent.agent_id().clone()),
            };

            let messages = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect("Room update failed");

            // Assert response.
            let (resp_room, respp, _) = find_response::<Room>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(resp_room.id(), room.id());
            assert_eq!(resp_room.audience(), room.audience());
            assert_eq!(resp_room.time(), time);
            assert_eq!(
                resp_room.rtc_sharing_policy(),
                db::rtc::SharingPolicy::Shared
            );
            assert_eq!(resp_room.reserve(), Some(123));
            assert_eq!(resp_room.tags(), &json!({"foo": "bar"}));
            assert_eq!(resp_room.classroom_id(), classroom_id);
            assert_eq!(resp_room.host(), Some(agent.agent_id()));
        }

        #[sqlx::test]
        async fn update_room_with_wrong_time(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let now = Utc::now().trunc_subsecs(0);

            let room = {
                let mut conn = db.get_conn().await;

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((
                        Bound::Included(now - Duration::hours(1)),
                        Bound::Excluded(now + Duration::hours(2)),
                    ))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .insert(&mut conn)
                    .await
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make room.update request.
            let mut context = TestContext::new(db, authz).await;

            let time = (
                Bound::Included(now + Duration::hours(3)),
                Bound::Excluded(now - Duration::hours(2)),
            );

            let payload = UpdateRequest {
                id: room.id(),
                time: Some(time),
                reserve: Some(Some(123)),
                tags: Some(json!({"foo": "bar"})),
                classroom_id: None,
                host: None,
            };

            handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Room update succeeded when it should've failed");
        }

        #[sqlx::test]
        async fn update_and_close_room(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let now = Utc::now().trunc_subsecs(0);

            let room = {
                let mut conn = db.get_conn().await;

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((
                        Bound::Included(now - Duration::hours(1)),
                        Bound::Excluded(now + Duration::hours(5)),
                    ))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .insert(&mut conn)
                    .await
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make room.update request.
            let mut context = TestContext::new(db, authz).await;

            let time = (
                Bound::Included(now - Duration::hours(1)),
                Bound::Excluded(now - Duration::seconds(5)),
            );

            let payload = UpdateRequest {
                id: room.id(),
                time: Some(time),
                reserve: Some(Some(123)),
                tags: Default::default(),
                classroom_id: Default::default(),
                host: None,
            };

            let messages = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect("Room update failed");

            assert_eq!(messages.len(), 4);

            let (closed_tenant_notification, _, _) =
                find_event_by_predicate::<JsonValue, _>(messages.as_slice(), |evp, _, topic| {
                    evp.label() == "room.close" && topic.contains("audiences")
                })
                .expect("Failed to find room.close event");

            assert_eq!(
                closed_tenant_notification
                    .get("id")
                    .and_then(|v| v.as_str()),
                Some(room.id().to_string()).as_deref()
            );

            let (closed_room_notification, _, _) =
                find_event_by_predicate::<JsonValue, _>(messages.as_slice(), |evp, _, topic| {
                    evp.label() == "room.close" && topic.contains("rooms")
                })
                .expect("Failed to find room.close event");

            assert_eq!(
                closed_room_notification.get("id").and_then(|v| v.as_str()),
                Some(room.id().to_string()).as_deref()
            );
        }

        #[sqlx::test]
        async fn update_and_close_unbounded_room(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let now = Utc::now().trunc_subsecs(0);

            let room = {
                let mut conn = db.get_conn().await;

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Included(now - Duration::hours(1)), Bound::Unbounded))
                    .insert(&mut conn)
                    .await
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make room.update request.
            let mut context = TestContext::new(db, authz).await;

            let time = (
                Bound::Included(now - Duration::hours(1)),
                Bound::Excluded(now - Duration::seconds(5)),
            );

            let payload = UpdateRequest {
                id: room.id(),
                time: Some(time),
                reserve: Default::default(),
                tags: Default::default(),
                classroom_id: Default::default(),
                host: None,
            };

            handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect("Room update failed");
        }

        #[sqlx::test]
        async fn update_room_missing(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let payload = UpdateRequest {
                id: db::room::Id::random(),
                time: Default::default(),
                reserve: Default::default(),
                tags: Default::default(),
                classroom_id: Default::default(),
                host: None,
            };

            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room update");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }

        #[sqlx::test]
        async fn update_room_closed(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db.get_conn().await;

                // Create closed room.
                shared_helpers::insert_closed_room(&mut conn).await
            };

            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let payload = UpdateRequest {
                id: room.id(),
                time: Some((Bound::Included(Utc::now()), Bound::Excluded(Utc::now()))),
                reserve: Default::default(),
                tags: Default::default(),
                classroom_id: Default::default(),
                host: None,
            };

            let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room update");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
        }
    }

    mod close {
        use std::ops::Bound;

        use chrono::{Duration, Utc};

        use crate::{
            db::room::Object as Room,
            test_helpers::{db::TestDb, prelude::*},
        };

        use super::super::*;

        #[sqlx::test]
        async fn close_room(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let room = {
                let mut conn = db.get_conn().await;

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Unbounded, Bound::Unbounded))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .insert(&mut conn)
                    .await
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make room.update request.
            let mut context = TestContext::new(db, authz).await;

            let payload = CloseRequest { id: room.id() };

            let messages = handle_request::<CloseHandler>(&mut context, &agent, payload)
                .await
                .expect("Room close failed");

            // Assert response.
            let (resp_room, respp, _) = find_response::<Room>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(resp_room.id(), room.id());
            let end = match resp_room.time().1 {
                Bound::Excluded(t) => t,
                _ => unreachable!("Wrong end in room close"),
            };
            assert!(end < Utc::now());
            assert_eq!(
                resp_room.rtc_sharing_policy(),
                db::rtc::SharingPolicy::Shared
            );
        }

        #[sqlx::test]
        async fn close_infinite_room(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let room = {
                let mut conn = db.get_conn().await;

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Unbounded, Bound::Unbounded))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .infinite()
                    .insert(&mut conn)
                    .await
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make room.update request.
            let mut context = TestContext::new(db, authz).await;

            let payload = CloseRequest { id: room.id() };

            handle_request::<CloseHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Room close succeeded even though it shouldn't");
        }

        #[sqlx::test]
        async fn close_bounded_room(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let room = {
                let mut conn = db.get_conn().await;

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((
                        Bound::Included(Utc::now() - Duration::hours(10)),
                        Bound::Excluded(Utc::now() + Duration::hours(10)),
                    ))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .insert(&mut conn)
                    .await
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "update",
            );

            // Make room.update request.
            let mut context = TestContext::new(db, authz).await;

            let payload = CloseRequest { id: room.id() };

            let messages = handle_request::<CloseHandler>(&mut context, &agent, payload)
                .await
                .expect("Room close failed");

            // Assert response.
            let (resp_room, respp, _) = find_response::<Room>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(resp_room.id(), room.id());
            let end = match resp_room.time().1 {
                Bound::Excluded(t) => t,
                _ => unreachable!("Wrong end in room close"),
            };
            assert!(end < Utc::now());
            assert_eq!(
                resp_room.rtc_sharing_policy(),
                db::rtc::SharingPolicy::Shared
            );
        }

        #[sqlx::test]
        async fn close_room_missing(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let payload = CloseRequest {
                id: db::room::Id::random(),
            };

            let err = handle_request::<CloseHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room close");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }

        #[sqlx::test]
        async fn close_room_closed(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db.get_conn().await;

                // Create closed room.
                shared_helpers::insert_closed_room(&mut conn).await
            };

            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let payload = CloseRequest { id: room.id() };

            let err = handle_request::<CloseHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room close");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
        }
    }

    mod enter {
        use chrono::{Duration, Utc};

        use crate::db::group_agent::{GroupItem, Groups};
        use crate::test_helpers::{db::TestDb, prelude::*, test_deps::LocalDeps};

        use super::super::*;

        #[sqlx::test]
        async fn enter_room(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let room = {
                let mut conn = db.get_conn().await;

                // Create room.
                shared_helpers::insert_room(&mut conn).await
            };

            // Allow agent to subscribe to the rooms' events.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id, "rtcs"],
                "create",
            );

            // Make room.enter request.
            let context = TestContext::new(db, authz).await;
            let payload = EnterRequest { id: room.id() };

            let reqp = RequestParams::Http {
                agent_id: &agent.agent_id(),
            };
            EnterHandler::handle(Arc::new(context), payload, reqp, Utc::now())
                .await
                .expect("Room entrance failed");
        }

        #[sqlx::test]
        async fn enter_room_not_authorized(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let context = TestContext::new(db, TestAuthz::new()).await;
            let payload = EnterRequest { id: room.id() };

            let reqp = RequestParams::Http {
                agent_id: &agent.agent_id(),
            };
            let err = EnterHandler::handle(Arc::new(context), payload, reqp, Utc::now())
                .await
                .err()
                .expect("Unexpected success on room entering");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }

        #[sqlx::test]
        async fn enter_room_missing(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let context = TestContext::new(db, TestAuthz::new()).await;

            let payload = EnterRequest {
                id: db::room::Id::random(),
            };

            let reqp = RequestParams::Http {
                agent_id: &agent.agent_id(),
            };
            let err = EnterHandler::handle(Arc::new(context), payload, reqp, Utc::now())
                .await
                .err()
                .expect("Unexpected success on room entering");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }

        #[sqlx::test]
        async fn enter_room_closed(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let room = {
                let mut conn = db.get_conn().await;
                // Create closed room.
                shared_helpers::insert_closed_room(&mut conn).await
            };

            // Allow agent to subscribe to the rooms' events.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );

            // Make room.enter request.
            let context = TestContext::new(db, authz).await;
            let payload = EnterRequest { id: room.id() };

            let reqp = RequestParams::Http {
                agent_id: &agent.agent_id(),
            };
            let err = EnterHandler::handle(Arc::new(context), payload, reqp, Utc::now())
                .await
                .err()
                .expect("Unexpected success on room entering");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
        }

        #[sqlx::test]
        async fn enter_room_with_no_opening_time(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let room = {
                let mut conn = db.get_conn().await;

                // Create room without time.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Unbounded, Bound::Unbounded))
                    .insert(&mut conn)
                    .await
            };

            // Allow agent to subscribe to the rooms' events.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );

            // Make room.enter request.
            let context = TestContext::new(db, authz).await;
            let payload = EnterRequest { id: room.id() };

            let reqp = RequestParams::Http {
                agent_id: &agent.agent_id(),
            };
            let err = EnterHandler::handle(Arc::new(context), payload, reqp, Utc::now())
                .await
                .err()
                .expect("Unexpected success on room entering");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
        }

        #[sqlx::test]
        async fn enter_room_that_opens_in_the_future(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let room = {
                let mut conn = db.get_conn().await;

                // Create room without time.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((
                        Bound::Included(Utc::now() + Duration::hours(1)),
                        Bound::Unbounded,
                    ))
                    .insert(&mut conn)
                    .await
            };

            // Allow agent to subscribe to the rooms' events.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );

            // Make room.enter request.
            let context = TestContext::new(db, authz).await;
            let payload = EnterRequest { id: room.id() };

            let reqp = RequestParams::Http {
                agent_id: &agent.agent_id(),
            };
            EnterHandler::handle(Arc::new(context), payload, reqp, Utc::now())
                .await
                .expect("Room entrance failed");
        }

        #[sqlx::test]
        async fn add_new_participant_to_default_group(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let mut conn = db.get_conn().await;

            // Create room.
            let room = shared_helpers::insert_room_with_owned(&mut conn).await;

            factory::GroupAgent::new(room.id(), Groups::new(vec![GroupItem::new(0, vec![])]))
                .upsert(&mut conn)
                .await;

            // Allow agent to subscribe to the rooms' events.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id, "rtcs"],
                "create",
            );

            // Make room.enter request.
            let context = TestContext::new(db, authz).await;
            let payload = EnterRequest { id: room.id() };

            let reqp = RequestParams::Http {
                agent_id: &agent.agent_id(),
            };
            EnterHandler::handle(Arc::new(context), payload, reqp, Utc::now())
                .await
                .expect("Room entrance failed");

            let group_agent = db::group_agent::FindQuery::new(room.id())
                .execute(&mut conn)
                .await
                .expect("failed to find a group agent");

            let groups = Groups::new(vec![GroupItem::new(0, vec![agent.agent_id().clone()])]);
            assert_eq!(group_agent.groups(), groups);
        }

        #[sqlx::test]
        async fn existed_participant_in_group(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let mut conn = db.get_conn().await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Create room.
            let room = shared_helpers::insert_room_with_owned(&mut conn).await;

            factory::GroupAgent::new(
                room.id(),
                Groups::new(vec![GroupItem::new(0, vec![agent.agent_id().clone()])]),
            )
            .upsert(&mut conn)
            .await;

            // Allow agent to subscribe to the rooms' events.
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );
            authz.allow(
                agent.account_id(),
                vec!["classrooms", &classroom_id, "rtcs"],
                "create",
            );

            // Make room.enter request.
            let context = TestContext::new(db, authz).await;
            let payload = EnterRequest { id: room.id() };

            let reqp = RequestParams::Http {
                agent_id: &agent.agent_id(),
            };
            EnterHandler::handle(Arc::new(context), payload, reqp, Utc::now())
                .await
                .expect("Room entrance failed");

            let group_agent = db::group_agent::FindQuery::new(room.id())
                .execute(&mut conn)
                .await
                .expect("failed to get group agents");

            assert_eq!(group_agent.groups().len(), 1);
        }

        #[sqlx::test]
        async fn create_reader_configs(pool: sqlx::PgPool) {
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
                Groups::new(vec![
                    GroupItem::new(0, vec![]),
                    GroupItem::new(1, vec![agent1.agent_id().clone()]),
                ]),
            )
            .upsert(&mut conn)
            .await;

            for agent in &[&agent1, &agent2] {
                factory::Rtc::new(room.id())
                    .created_by(agent.agent_id().to_owned())
                    .insert(&mut conn)
                    .await;
            }

            // Allow agent to subscribe to the rooms' events.
            let mut authz = TestAuthz::new();
            let classroom_id = room.classroom_id().to_string();
            authz.allow(
                agent2.account_id(),
                vec!["classrooms", &classroom_id],
                "read",
            );

            // Make room.enter request.
            let mut context = TestContext::new(db, authz).await;
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = EnterRequest { id: room.id() };

            let reqp = RequestParams::Http {
                agent_id: &agent2.agent_id(),
            };
            EnterHandler::handle(Arc::new(context.clone()), payload, reqp, Utc::now())
                .await
                .expect("Room entrance failed");

            let group_agent = db::group_agent::FindQuery::new(room.id())
                .execute(&mut conn)
                .await
                .expect("failed to get group agents");

            assert_eq!(group_agent.groups().len(), 2);

            let reader_configs = db::rtc_reader_config::ListWithRtcQuery::new(
                room.id(),
                &[agent1.agent_id(), agent2.agent_id()],
            )
            .execute(&mut conn)
            .await
            .expect("failed to get rtc reader configs");

            assert_eq!(reader_configs.len(), 2);

            context.janus_clients().remove_client(&backend);
        }
    }

    mod leave {
        use crate::test_helpers::{db::TestDb, prelude::*};

        use super::{super::*, DynSubRequest};

        #[sqlx::test]
        async fn leave_room(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;

            // Create room.
            let room = shared_helpers::insert_room(&mut conn).await;
            // Put agent online in the room.
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

            // Make room.leave request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;
            let payload = LeaveRequest { id: room.id() };

            let messages = handle_request::<LeaveHandler>(&mut context, &agent, payload)
                .await
                .expect("Room leaving failed");

            // Assert dynamic subscription request.
            let (payload, reqp, topic) = find_request::<DynSubRequest>(messages.as_slice());

            let expected_topic = format!(
                "agents/{}.{}/api/{}/out/{}",
                context.config().agent_label,
                context.config().id,
                API_VERSION,
                context.config().broker_id,
            );

            assert_eq!(topic, expected_topic);
            assert_eq!(reqp.method(), "subscription.delete");
            assert_eq!(&payload.subject, agent.agent_id());

            let room_id = room.id().to_string();
            assert_eq!(payload.object, vec!["rooms", &room_id, "events"]);
        }

        #[sqlx::test]
        async fn leave_room_while_not_entered(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let mut context = TestContext::new(db, TestAuthz::new()).await;
            let payload = LeaveRequest { id: room.id() };

            let err = handle_request::<LeaveHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room leaving");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
        }

        #[sqlx::test]
        async fn leave_room_missing(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let payload = LeaveRequest {
                id: db::room::Id::random(),
            };

            let err = handle_request::<LeaveHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room leaving");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }
    }
}
