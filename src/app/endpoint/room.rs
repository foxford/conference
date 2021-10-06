use anyhow::{anyhow, Context as AnyhowContext};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::ops::Bound;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingRequest, ResponseStatus,
        ShortTermTimingProperties, SubscriptionTopic,
    },
    Addressable, AgentId, Subscription,
};

use uuid::Uuid;

use crate::{
    app::{
        context::Context,
        endpoint::{prelude::*, subscription::CorrelationDataPayload},
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
        API_VERSION,
    },
    authz::AuthzObject,
    db,
    db::{room::RoomBackend, rtc::SharingPolicy as RtcSharingPolicy},
};
use tracing_attributes::instrument;

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
    time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    audience: String,
    // Deprecated in favor of `rtc_sharing_policy`.
    #[serde(default)]
    backend: Option<RoomBackend>,
    #[serde(default)]
    rtc_sharing_policy: Option<RtcSharingPolicy>,
    reserve: Option<i32>,
    tags: Option<JsonValue>,
    classroom_id: Option<Uuid>,
}

pub struct CreateHandler;

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;
    const ERROR_TITLE: &'static str = "Failed to create room";

    #[instrument(skip(context, payload, reqp))]
    async fn handle<C: Context>(
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
                AuthzObject::new(&["rooms"]).into(),
                "create".into(),
            )
            .await?;
        context.metrics().observe_auth(authz_time);
        // Create a room.
        let conn = context.get_conn().await?;
        let audience = payload.audience.clone();
        let room = crate::util::spawn_blocking({
            move || {
                let mut q =
                    db::room::InsertQuery::new(payload.time, &payload.audience, rtc_sharing_policy);

                if let Some(reserve) = payload.reserve {
                    q = q.reserve(reserve);
                }

                if let Some(ref tags) = payload.tags {
                    q = q.tags(tags);
                }

                if let Some(classroom_id) = payload.classroom_id {
                    q = q.classroom_id(classroom_id);
                }

                q.execute(&conn)
            }
        })
        .await?;

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
            &format!("audiences/{}/events", audience),
            room,
            context.start_timestamp(),
        );

        Ok(response)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct ReadRequest {
    id: db::room::Id,
}

pub struct ReadHandler;

#[async_trait]
impl RequestHandler for ReadHandler {
    type Payload = ReadRequest;
    const ERROR_TITLE: &'static str = "Failed to read room";

    #[instrument(skip(context, payload, reqp), fields(room_id = %payload.id))]
    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let conn = context.get_conn().await?;
        let room = crate::util::spawn_blocking(move || {
            helpers::find_room_by_id(payload.id, helpers::RoomTimeRequirement::Any, &conn)
        })
        .await?;

        // Authorize room reading on the tenant.
        let room_id = room.id().to_string();
        let object = AuthzObject::new(&["rooms", &room_id]).into();

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
pub struct UpdateHandler;

#[async_trait]
impl RequestHandler for UpdateHandler {
    type Payload = UpdateRequest;
    const ERROR_TITLE: &'static str = "Failed to update room";

    #[instrument(skip(context, payload, reqp), fields(room_id = %payload.id))]
    async fn handle<C: Context>(
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
        let conn = context.get_conn().await?;

        let room = crate::util::spawn_blocking({
            let id = payload.id;
            move || helpers::find_room_by_id(id, time_requirement, &conn)
        })
        .await?;

        // Authorize room updating on the tenant.
        let room_id = room.id().to_string();
        let object = AuthzObject::new(&["rooms", &room_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "update".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        let room_was_open = !room.is_closed();

        // Update room.
        let conn = context.get_conn().await?;
        let room =crate::util::spawn_blocking(move ||{

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
                        // Allow any change when no closing date specified.
                        (_, Bound::Unbounded) => Some(new_time),
                        (Bound::Included(o), Bound::Excluded(c)) if *c > Utc::now() => {
                            match new_time {
                                // Allow reschedule future closing.
                                (_, Bound::Excluded(nc)) => {
                                    let nc = std::cmp::max(nc, Utc::now());
                                    Some((Bound::Included(*o), Bound::Excluded(nc)))
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

            Ok::<_, AppError>(db::room::UpdateQuery::new(room.id())
                .time(time)
                .reserve(payload.reserve)
                .tags(payload.tags)
                .classroom_id(payload.classroom_id)
                .host(payload.host.as_ref())
                .execute(&conn)?)
        }).await?;

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
            if room_was_open && *closed_at <= Utc::now() {
                let room = crate::util::spawn_blocking({
                    let room_id = room.id();
                    let agent = reqp.as_agent_id().to_owned();
                    let conn = context.get_conn().await?;
                    move || db::room::set_closed_by(room_id, &agent, &conn)
                })
                .await?;
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
pub struct CloseHandler;

#[async_trait]
impl RequestHandler for CloseHandler {
    type Payload = CloseRequest;
    const ERROR_TITLE: &'static str = "Failed to close room";

    #[instrument(skip(context, payload, reqp), fields(room_id = %payload.id))]
    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let conn = context.get_conn().await?;

        let room = crate::util::spawn_blocking({
            let id = payload.id;
            move || {
                helpers::find_room_by_id(
                    id,
                    helpers::RoomTimeRequirement::NotClosedOrUnboundedOpen,
                    &conn,
                )
            }
        })
        .await?;

        // Authorize room updating on the tenant.
        let room_id = room.id().to_string();
        let object = AuthzObject::new(&["rooms", &room_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "update".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        // Update room.
        let room = crate::util::spawn_blocking({
            let room_id = room.id();
            let agent = reqp.as_agent_id().to_owned();
            let conn = context.get_conn().await?;
            move || db::room::set_closed_by(room_id, &agent, &conn)
        })
        .await?;

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

pub type EnterRequest = ReadRequest;
pub struct EnterHandler;

#[async_trait]
impl RequestHandler for EnterHandler {
    type Payload = EnterRequest;
    const ERROR_TITLE: &'static str = "Failed to enter room";

    #[instrument(skip(context, payload, reqp), fields(room_id = %payload.id))]
    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let mqtt_params = reqp.as_mqtt_params()?;
        let conn = context.get_conn().await?;
        let room = crate::util::spawn_blocking(move || {
            helpers::find_room_by_id(payload.id, helpers::RoomTimeRequirement::NotClosed, &conn)
        })
        .await?;

        // Authorize subscribing to the room's events.
        let room_id = room.id().to_string();
        let object = AuthzObject::new(&["rooms", &room_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "read".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        // Register agent in `in_progress` state.
        let conn = context.get_conn().await?;
        crate::util::spawn_blocking({
            let agent_id = reqp.as_agent_id().clone();
            move || db::agent::InsertQuery::new(&agent_id, room.id()).execute(&conn)
        })
        .await?;

        // Send dynamic subscription creation request to the broker.
        let subject = reqp.as_agent_id().to_owned();
        let object = vec!["rooms", &room_id, "events"];
        let object = object
            .into_iter()
            .map(|s| s.to_owned())
            .collect::<Vec<String>>();
        let payload = SubscriptionRequest::new(subject.clone(), object.clone());

        let broker_id = AgentId::new("nevermind", context.config().broker_id.to_owned());

        let response_topic = Subscription::unicast_responses_from(&broker_id)
            .subscription_topic(context.agent_id(), API_VERSION)
            .context("Failed to build response topic")
            .error(AppErrorKind::BrokerRequestFailed)?;

        let corr_data_payload =
            CorrelationDataPayload::new(mqtt_params.to_owned(), subject, object);

        let corr_data = CorrelationData::SubscriptionCreate(corr_data_payload)
            .dump()
            .context("Failed to dump correlation data")
            .error(AppErrorKind::BrokerRequestFailed)?;

        let mut timing = ShortTermTimingProperties::until_now(context.start_timestamp());
        timing.set_authorization_time(authz_time);

        let props =
            mqtt_params.to_request("subscription.create", &response_topic, &corr_data, timing);
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
            .room_enter
            .observe_timestamp(context.start_timestamp());

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
    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let mqtt_params = reqp.as_mqtt_params()?;
        let conn = context.get_conn().await?;
        let (room, presence) = crate::util::spawn_blocking({
            let agent_id = reqp.as_agent_id().clone();
            move || {
                let room =
                    helpers::find_room_by_id(payload.id, helpers::RoomTimeRequirement::Any, &conn)?;

                // Check room presence.
                let presence = db::agent::ListQuery::new()
                    .room_id(room.id())
                    .agent_id(&agent_id)
                    .execute(&conn)?;

                Ok::<_, AppError>((room, presence))
            }
        })
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

        use crate::{
            db::room::Object as Room,
            test_helpers::{prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn create() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            // Allow user to create rooms.
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            authz.allow(agent.account_id(), vec!["rooms"], "create");

            // Make room.create request.
            let mut context = TestContext::new(db.clone(), authz);
            let time = (Bound::Unbounded, Bound::Unbounded);
            let classroom_id = Uuid::new_v4();

            let payload = CreateRequest {
                time: time.clone(),
                audience: USR_AUDIENCE.to_owned(),
                backend: None,
                rtc_sharing_policy: Some(db::rtc::SharingPolicy::Shared),
                reserve: Some(123),
                tags: Some(json!({ "foo": "bar" })),
                classroom_id: Some(classroom_id),
            };

            let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect("Room creation failed");

            // Assert response.
            let (room, respp, _) = find_response::<Room>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(room.audience(), USR_AUDIENCE);
            assert_eq!(room.time(), &time);
            assert_eq!(room.rtc_sharing_policy(), db::rtc::SharingPolicy::Shared);
            assert_eq!(room.reserve(), Some(123));
            assert_eq!(room.tags(), &json!({ "foo": "bar" }));
            assert_eq!(room.classroom_id(), Some(classroom_id));

            // Assert notification.
            let (room, evp, topic) = find_event::<Room>(messages.as_slice());
            assert!(topic.ends_with(&format!("/audiences/{}/events", USR_AUDIENCE)));
            assert_eq!(evp.label(), "room.create");
            assert_eq!(room.audience(), USR_AUDIENCE);
            assert_eq!(room.time(), &time);
            assert_eq!(room.rtc_sharing_policy(), db::rtc::SharingPolicy::Shared);
            assert_eq!(room.reserve(), Some(123));
            assert_eq!(room.tags(), &json!({ "foo": "bar" }));
            assert_eq!(room.classroom_id(), Some(classroom_id));
        }

        #[tokio::test]
        async fn create_room_unauthorized() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let mut context = TestContext::new(db, TestAuthz::new());
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Make room.create request.
            let payload = CreateRequest {
                time: (Bound::Included(Utc::now()), Bound::Unbounded),
                audience: USR_AUDIENCE.to_owned(),
                backend: None,
                rtc_sharing_policy: Some(db::rtc::SharingPolicy::Shared),
                reserve: None,
                tags: None,
                classroom_id: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room creation");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }
    }

    mod read {
        use crate::{
            db::room::Object as Room,
            test_helpers::{prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn read_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                shared_helpers::insert_room(&conn)
            };

            // Allow agent to read the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            authz.allow(agent.account_id(), vec!["rooms", &room_id], "read");

            // Make room.read request.
            let mut context = TestContext::new(db, authz);
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

        #[tokio::test]
        async fn read_room_not_authorized() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                shared_helpers::insert_room(&conn)
            };

            let mut context = TestContext::new(db, TestAuthz::new());
            let payload = ReadRequest { id: room.id() };

            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room reading");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }

        #[tokio::test]
        async fn read_room_missing() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new());
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
            test_helpers::{find_event_by_predicate, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn update_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let now = Utc::now().trunc_subsecs(0);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Unbounded, Bound::Unbounded))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .insert(&conn)
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

            // Make room.update request.
            let mut context = TestContext::new(db, authz);
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
            assert_eq!(resp_room.time(), &time);
            assert_eq!(
                resp_room.rtc_sharing_policy(),
                db::rtc::SharingPolicy::Shared
            );
            assert_eq!(resp_room.reserve(), Some(123));
            assert_eq!(resp_room.tags(), &json!({"foo": "bar"}));
            assert_eq!(resp_room.classroom_id(), Some(classroom_id));
            assert_eq!(resp_room.host(), Some(agent.agent_id()));
        }

        #[tokio::test]
        async fn update_room_with_wrong_time() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let now = Utc::now().trunc_subsecs(0);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((
                        Bound::Included(now - Duration::hours(1)),
                        Bound::Excluded(now + Duration::hours(2)),
                    ))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .insert(&conn)
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

            // Make room.update request.
            let mut context = TestContext::new(db, authz);

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

        #[tokio::test]
        async fn update_and_close_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let now = Utc::now().trunc_subsecs(0);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((
                        Bound::Included(now - Duration::hours(1)),
                        Bound::Excluded(now + Duration::hours(5)),
                    ))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .insert(&conn)
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

            // Make room.update request.
            let mut context = TestContext::new(db, authz);

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

        #[tokio::test]
        async fn update_and_close_unbounded_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let now = Utc::now().trunc_subsecs(0);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Included(now - Duration::hours(1)), Bound::Unbounded))
                    .insert(&conn)
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

            // Make room.update request.
            let mut context = TestContext::new(db, authz);

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

        #[tokio::test]
        async fn update_room_missing() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new());

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

        #[tokio::test]
        async fn update_room_closed() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create closed room.
                shared_helpers::insert_closed_room(&conn)
            };

            let mut context = TestContext::new(db, TestAuthz::new());

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
            test_helpers::{prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn close_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Unbounded, Bound::Unbounded))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .insert(&conn)
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

            // Make room.update request.
            let mut context = TestContext::new(db, authz);

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

        #[tokio::test]
        async fn close_bounded_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((
                        Bound::Included(Utc::now() - Duration::hours(10)),
                        Bound::Excluded(Utc::now() + Duration::hours(10)),
                    ))
                    .rtc_sharing_policy(db::rtc::SharingPolicy::Shared)
                    .insert(&conn)
            };

            // Allow agent to update the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

            // Make room.update request.
            let mut context = TestContext::new(db, authz);

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

        #[tokio::test]
        async fn close_room_missing() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = CloseRequest {
                id: db::room::Id::random(),
            };

            let err = handle_request::<CloseHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room close");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }

        #[tokio::test]
        async fn close_room_closed() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create closed room.
                shared_helpers::insert_closed_room(&conn)
            };

            let mut context = TestContext::new(db, TestAuthz::new());

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

        use crate::test_helpers::{prelude::*, test_deps::LocalDeps};

        use super::{super::*, DynSubRequest};

        #[tokio::test]
        async fn enter_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                shared_helpers::insert_room(&conn)
            };

            // Allow agent to subscribe to the rooms' events.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();

            authz.allow(agent.account_id(), vec!["rooms", &room_id], "read");

            // Make room.enter request.
            let mut context = TestContext::new(db, authz);
            let payload = EnterRequest { id: room.id() };

            let messages = handle_request::<EnterHandler>(&mut context, &agent, payload)
                .await
                .expect("Room entrance failed");

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
            assert_eq!(reqp.method(), "subscription.create");
            assert_eq!(payload.subject, agent.agent_id().to_owned());
            assert_eq!(payload.object, vec!["rooms", &room_id, "events"]);
        }

        #[tokio::test]
        async fn enter_room_not_authorized() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                shared_helpers::insert_room(&conn)
            };

            let mut context = TestContext::new(db, TestAuthz::new());
            let payload = EnterRequest { id: room.id() };

            let err = handle_request::<EnterHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room entering");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }

        #[tokio::test]
        async fn enter_room_missing() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new());
            let payload = EnterRequest {
                id: db::room::Id::random(),
            };

            let err = handle_request::<EnterHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room entering");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }

        #[tokio::test]
        async fn enter_room_closed() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create closed room.
                shared_helpers::insert_closed_room(&conn)
            };

            // Allow agent to subscribe to the rooms' events.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();

            authz.allow(agent.account_id(), vec!["rooms", &room_id], "read");

            // Make room.enter request.
            let mut context = TestContext::new(db, authz);
            let payload = EnterRequest { id: room.id() };

            let err = handle_request::<EnterHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room entering");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
        }

        #[tokio::test]
        async fn enter_room_with_no_opening_time() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room without time.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((Bound::Unbounded, Bound::Unbounded))
                    .insert(&conn)
            };

            // Allow agent to subscribe to the rooms' events.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();

            authz.allow(agent.account_id(), vec!["rooms", &room_id], "read");

            // Make room.enter request.
            let mut context = TestContext::new(db, authz);
            let payload = EnterRequest { id: room.id() };

            let err = handle_request::<EnterHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room entering");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
        }

        #[tokio::test]
        async fn enter_room_that_opens_in_the_future() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room without time.
                factory::Room::new()
                    .audience(USR_AUDIENCE)
                    .time((
                        Bound::Included(Utc::now() + Duration::hours(1)),
                        Bound::Unbounded,
                    ))
                    .insert(&conn)
            };

            // Allow agent to subscribe to the rooms' events.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();

            authz.allow(agent.account_id(), vec!["rooms", &room_id], "read");

            // Make room.enter request.
            let mut context = TestContext::new(db, authz);
            let payload = EnterRequest { id: room.id() };

            handle_request::<EnterHandler>(&mut context, &agent, payload)
                .await
                .expect("Room entrance failed");
        }
    }

    mod leave {
        use crate::test_helpers::{prelude::*, test_deps::LocalDeps};

        use super::{super::*, DynSubRequest};

        #[tokio::test]
        async fn leave_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                let room = shared_helpers::insert_room(&conn);

                // Put agent online in the room.
                shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                room
            };

            // Make room.leave request.
            let mut context = TestContext::new(db, TestAuthz::new());
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

        #[tokio::test]
        async fn leave_room_while_not_entered() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                shared_helpers::insert_room(&conn)
            };

            let mut context = TestContext::new(db, TestAuthz::new());
            let payload = LeaveRequest { id: room.id() };

            let err = handle_request::<LeaveHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room leaving");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
        }

        #[tokio::test]
        async fn leave_room_missing() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new());
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
