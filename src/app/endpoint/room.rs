use std::ops::Bound;

use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{
    IncomingEventProperties, IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent,
    OutgoingEventProperties, OutgoingRequest, ResponseStatus, ShortTermTimingProperties,
};
use svc_agent::{Addressable, AgentId};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::endpoint::subscription::RoomEnterLeaveEvent;
use crate::db;
use crate::diesel::Connection;

///////////////////////////////////////////////////////////////////////////////

const MQTT_GW_API_VERSION: &str = "v1";

#[derive(Debug, Serialize)]
struct SubscriptionRequest {
    subject: AgentId,
    object: Vec<String>,
}

impl SubscriptionRequest {
    fn new(subject: AgentId, object: Vec<&str>) -> Self {
        Self {
            subject,
            object: object.iter().map(|&s| s.into()).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequest {
    #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
    time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    audience: String,
    #[serde(default = "CreateRequest::default_backend")]
    backend: db::room::RoomBackend,
    reserve: Option<i32>,
    tags: Option<JsonValue>,
}

impl CreateRequest {
    fn default_backend() -> db::room::RoomBackend {
        db::room::RoomBackend::None
    }
}

pub(crate) struct CreateHandler;

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        // Authorize room creation on the tenant.
        let authz_time = context
            .authz()
            .authorize(&payload.audience, reqp, vec!["rooms"], "create")
            .await?;

        // Create a room.
        let room = {
            let mut q =
                db::room::InsertQuery::new(payload.time, &payload.audience, payload.backend);

            if let Some(reserve) = payload.reserve {
                q = q.reserve(reserve);
            }

            if let Some(ref tags) = payload.tags {
                q = q.tags(tags);
            }

            let conn = context.get_conn()?;
            q.execute(&conn)?
        };

        helpers::add_room_logger_tags(context, &room);

        // Respond and broadcast to the audience topic.
        let response = helpers::build_response(
            // TODO: Change to `ResponseStatus::CREATED` (breaking).
            ResponseStatus::OK,
            room.clone(),
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        );

        let notification = helpers::build_notification(
            "room.create",
            &format!("audiences/{}/events", payload.audience),
            room,
            reqp,
            context.start_timestamp(),
        );

        Ok(Box::new(stream::from_iter(vec![response, notification])))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct ReadRequest {
    id: Uuid,
}

pub(crate) struct ReadHandler;

#[async_trait]
impl RequestHandler for ReadHandler {
    type Payload = ReadRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room =
            helpers::find_room_by_id(context, payload.id, helpers::RoomTimeRequirement::Any)?;

        // Authorize room reading on the tenant.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "read")
            .await?;

        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            room,
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        ))))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Default)]
pub(crate) struct UpdateRequest {
    id: Uuid,
    #[serde(default)]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<db::room::Time>,
    audience: Option<String>,
    backend: Option<db::room::RoomBackend>,
    reserve: Option<Option<i32>>,
    tags: Option<JsonValue>,
}
pub(crate) struct UpdateHandler;

#[async_trait]
impl RequestHandler for UpdateHandler {
    type Payload = UpdateRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room =
            helpers::find_room_by_id(context, payload.id, helpers::RoomTimeRequirement::NotClosed)?;

        // Authorize room updating on the tenant.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "update")
            .await?;

        let room_was_open = !room.is_closed();
        let mut room_closed_by_update = false;
        let mut delete_agents = false;

        // Update room.
        let (room, left_agents) = {
            let conn = context.get_conn()?;

            let time = match payload.time {
                None => None,
                Some(new_time) => {
                    let (new_opened_at, maybe_new_closed_at) = match new_time {
                        (Bound::Included(o), Bound::Excluded(c)) if o < c => (o, Some(c)),
                        (Bound::Included(o), Bound::Unbounded) => (o, None),
                        _ => {
                            return Err(anyhow!("Invalid room time"))
                                .error(AppErrorKind::InvalidRoomTime)
                        }
                    };

                    // If the room has been already opened
                    match room.time() {
                        (Bound::Included(o), old_bound_closed_at) if *o <= Utc::now() => {
                            let rtcs = db::rtc::ListQuery::new()
                                .room_id(room.id())
                                .execute(&conn)?;

                            // If we're moving the opening to the future remove active agents.
                            // Also if an RTC has already been created then forbid changing the time.
                            if new_opened_at > Utc::now() {
                                if rtcs.is_empty() {
                                    delete_agents = true;
                                } else {
                                    let err = anyhow!("Opening time changing forbidden since an RTC has already been created");
                                    return Err(err).error(AppErrorKind::RoomTimeChangingForbidden);
                                }
                            }

                            // Allow only closing it in the future or close it now.
                            let bounded_new_closed_at = match maybe_new_closed_at {
                                Some(c) if c <= Utc::now() => {
                                    room_closed_by_update = true;
                                    Bound::Excluded(Utc::now())
                                }
                                Some(c) => Bound::Excluded(c),
                                None => {
                                    if rtcs.is_empty() {
                                        Bound::Unbounded
                                    } else {
                                        // We can't make the end unbounded when an rtc is present
                                        // to avoid dropping the timeout.
                                        *old_bound_closed_at
                                    }
                                }
                            };

                            Some((Bound::Included(new_opened_at), bounded_new_closed_at))
                        }
                        _ => Some(new_time),
                    }
                }
            };

            conn.transaction::<_, diesel::result::Error, _>(|| {
                let room = db::room::UpdateQuery::new(room.id())
                    .time(time)
                    .audience(payload.audience)
                    .backend(payload.backend)
                    .reserve(payload.reserve)
                    .tags(payload.tags)
                    .execute(&conn)?;

                let agents = if delete_agents {
                    let agents = db::agent::ListQuery::new()
                        .room_id(room.id())
                        .execute(&conn)?;

                    db::agent::DeleteQuery::new()
                        .room_id(room.id())
                        .execute(&conn)?;

                    agents
                } else {
                    vec![]
                };

                Ok((room, agents))
            })?
        };

        // Respond and broadcast to the audience topic.
        let response = helpers::build_response(
            ResponseStatus::OK,
            room.clone(),
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        );

        let notification = helpers::build_notification(
            "room.update",
            &format!("audiences/{}/events", room.audience()),
            room.clone(),
            reqp,
            context.start_timestamp(),
        );

        let mut responses = vec![response, notification];

        // Publish room closed notification.
        if room_was_open && room_closed_by_update {
            responses.push(helpers::build_notification(
                "room.close",
                &format!("rooms/{}/events", room.id()),
                room.clone(),
                reqp,
                context.start_timestamp(),
            ));

            responses.push(helpers::build_notification(
                "room.close",
                &format!("audiences/{}/events", room.audience()),
                room.clone(),
                reqp,
                context.start_timestamp(),
            ));
        }

        // Notify about left agents if any.
        for agent in left_agents {
            responses.push(helpers::build_notification(
                "room.leave",
                &format!("rooms/{}/events", room.id()),
                RoomEnterLeaveEvent::new(room.id(), agent.agent_id().to_owned()),
                reqp,
                context.start_timestamp(),
            ));
        }

        Ok(Box::new(stream::from_iter(responses)))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) type DeleteRequest = ReadRequest;
pub(crate) struct DeleteHandler;

#[async_trait]
impl RequestHandler for DeleteHandler {
    type Payload = DeleteRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room =
            helpers::find_room_by_id(context, payload.id, helpers::RoomTimeRequirement::NotClosed)?;

        // Authorize room deletion on the tenant.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "delete")
            .await?;

        // Delete room.
        {
            let conn = context.get_conn()?;
            db::room::DeleteQuery::new(room.id()).execute(&conn)?;
        }

        // Respond and broadcast to the audience topic.
        let response = helpers::build_response(
            ResponseStatus::OK,
            room.clone(),
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        );

        let notification = helpers::build_notification(
            "room.delete",
            &format!("audiences/{}/events", room.audience()),
            room,
            reqp,
            context.start_timestamp(),
        );

        Ok(Box::new(stream::from_iter(vec![response, notification])))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) type EnterRequest = ReadRequest;
pub(crate) struct EnterHandler;

#[async_trait]
impl RequestHandler for EnterHandler {
    type Payload = EnterRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room =
            helpers::find_room_by_id(context, payload.id, helpers::RoomTimeRequirement::NotClosed)?;

        // Authorize subscribing to the room's events.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "events"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object.clone(), "subscribe")
            .await?;

        // Register agent in `in_progress` state.
        {
            let conn = context.get_conn()?;
            db::agent::InsertQuery::new(reqp.as_agent_id(), room.id()).execute(&conn)?;
        }

        // Send dynamic subscription creation request to the broker.
        let payload = SubscriptionRequest::new(reqp.as_agent_id().to_owned(), object);

        let mut short_term_timing = ShortTermTimingProperties::until_now(context.start_timestamp());
        short_term_timing.set_authorization_time(authz_time);

        let props = reqp.to_request(
            "subscription.create",
            reqp.response_topic(),
            reqp.correlation_data(),
            short_term_timing,
        );

        // FIXME: It looks like sending a request to the client but the broker intercepts it
        //        creates a subscription and replaces the request with the response.
        //        This is kind of ugly but it guaranties that the request will be processed by
        //        the broker node where the client is connected to. We need that because
        //        the request changes local state on that node.
        //        A better solution will be possible after resolution of this issue:
        //        https://github.com/vernemq/vernemq/issues/1326.
        //        Then we won't need the local state on the broker at all and will be able
        //        to send a multicast request to the broker.
        let outgoing_request = OutgoingRequest::unicast(payload, props, reqp, MQTT_GW_API_VERSION);
        let boxed_request = Box::new(outgoing_request) as Box<dyn IntoPublishableMessage + Send>;
        Ok(Box::new(stream::once(boxed_request)))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) type LeaveRequest = ReadRequest;
pub(crate) struct LeaveHandler;

#[async_trait]
impl RequestHandler for LeaveHandler {
    type Payload = LeaveRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let (room, presence) = {
            let room =
                helpers::find_room_by_id(context, payload.id, helpers::RoomTimeRequirement::Any)?;

            let conn = context.get_conn()?;

            // Check room presence.
            let presence = db::agent::ListQuery::new()
                .room_id(room.id())
                .agent_id(reqp.as_agent_id())
                .execute(&conn)?;

            (room, presence)
        };

        if presence.is_empty() {
            return Err(anyhow!("Agent is not online in the room"))
                .error(AppErrorKind::AgentNotEnteredTheRoom);
        }

        // Send dynamic subscription deletion request to the broker.
        let room_id = room.id().to_string();

        let payload = SubscriptionRequest::new(
            reqp.as_agent_id().to_owned(),
            vec!["rooms", &room_id, "events"],
        );

        let props = reqp.to_request(
            "subscription.delete",
            reqp.response_topic(),
            reqp.correlation_data(),
            ShortTermTimingProperties::until_now(context.start_timestamp()),
        );

        // FIXME: It looks like sending a request to the client but the broker intercepts it
        //        deletes the subscription and replaces the request with the response.
        //        This is kind of ugly but it guaranties that the request will be processed by
        //        the broker node where the client is connected to. We need that because
        //        the request changes local state on that node.
        //        A better solution will be possible after resolution of this issue:
        //        https://github.com/vernemq/vernemq/issues/1326.
        //        Then we won't need the local state on the broker at all and will be able
        //        to send a multicast request to the broker.
        let outgoing_request = OutgoingRequest::unicast(payload, props, reqp, MQTT_GW_API_VERSION);
        let boxed_request = Box::new(outgoing_request) as Box<dyn IntoPublishableMessage + Send>;
        Ok(Box::new(stream::once(boxed_request)))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct NotifyOpenedEvent {
    #[serde(default = "NotifyOpenedEvent::default_duration")]
    duration: i64,
}

impl NotifyOpenedEvent {
    fn default_duration() -> i64 {
        60
    }
}

pub(crate) struct NotifyOpenedHandler;

#[async_trait]
impl EventHandler for NotifyOpenedHandler {
    type Payload = NotifyOpenedEvent;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
    ) -> Result {
        // Get recently opened rooms.
        let rooms = {
            let time = Utc::now() - Duration::seconds(payload.duration);
            let conn = context.get_conn()?;

            db::room::ListQuery::new()
                .opened_since(time)
                .execute(&conn)?
        };

        let events = rooms
            .into_iter()
            .map(|room| {
                let url = format!("rooms/{}/events", room.id());
                let timing = ShortTermTimingProperties::until_now(context.start_timestamp());
                let mut props = OutgoingEventProperties::new("room.open", timing);
                props.set_tracking(evp.tracking().to_owned());
                let boxed_ev = Box::new(OutgoingEvent::broadcast(room, props, &url));
                boxed_ev as Box<dyn IntoPublishableMessage + Send>
            })
            .collect::<Vec<Box<dyn IntoPublishableMessage + Send>>>();

        Ok(Box::new(stream::from_iter(events)))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use serde_derive::Deserialize;

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

        use crate::db::room::Object as Room;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn create() {
            async_std::task::block_on(async {
                // Allow user to create rooms.
                let mut authz = TestAuthz::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                authz.allow(agent.account_id(), vec!["rooms"], "create");

                // Make room.create request.
                let mut context = TestContext::new(TestDb::new(), authz);
                let time = (Bound::Unbounded, Bound::Unbounded);

                let payload = CreateRequest {
                    time: time.clone(),
                    audience: USR_AUDIENCE.to_owned(),
                    backend: db::room::RoomBackend::Janus,
                    reserve: Some(123),
                    tags: Some(json!({ "foo": "bar" })),
                };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Room creation failed");

                // Assert response.
                let (room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(room.audience(), USR_AUDIENCE);
                assert_eq!(room.time(), &time);
                assert_eq!(room.backend(), db::room::RoomBackend::Janus);
                assert_eq!(room.reserve(), Some(123));
                assert_eq!(room.tags(), &json!({ "foo": "bar" }));

                // Assert notification.
                let (room, evp, topic) = find_event::<Room>(messages.as_slice());
                assert!(topic.ends_with(&format!("/audiences/{}/events", USR_AUDIENCE)));
                assert_eq!(evp.label(), "room.create");
                assert_eq!(room.audience(), USR_AUDIENCE);
                assert_eq!(room.time(), &time);
                assert_eq!(room.backend(), db::room::RoomBackend::Janus);
                assert_eq!(room.reserve(), Some(123));
                assert_eq!(room.tags(), &json!({ "foo": "bar" }));
            });
        }

        #[test]
        fn create_room_unauthorized() {
            async_std::task::block_on(async {
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                // Make room.create request.
                let payload = CreateRequest {
                    time: (Bound::Included(Utc::now()), Bound::Unbounded),
                    audience: USR_AUDIENCE.to_owned(),
                    backend: db::room::RoomBackend::Janus,
                    reserve: None,
                    tags: None,
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room creation");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }
    }

    mod read {
        use crate::db::room::Object as Room;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn read_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();

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
                let (resp_room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_room.audience(), room.audience());
                assert_eq!(resp_room.time(), room.time());
                assert_eq!(resp_room.backend(), room.backend());
            });
        }

        #[test]
        fn read_room_not_authorized() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

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
            });
        }

        #[test]
        fn read_room_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = ReadRequest { id: Uuid::new_v4() };

                let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room reading");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }

    mod update {
        use std::ops::Bound;

        use chrono::{Duration, SubsecRound, Utc};
        use serde_derive::Deserialize;
        use serde_json::json;
        use svc_agent::AgentId;
        use uuid::Uuid;

        use crate::db::agent::ListQuery as AgentListQuery;
        use crate::db::room::Object as Room;
        use crate::test_helpers::find_event_by_predicate;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[derive(Deserialize)]
        struct RoomLeavePayload {
            id: Uuid,
            agent_id: AgentId,
        }

        #[test]
        fn update_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();
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
                            Bound::Included(now + Duration::hours(1)),
                            Bound::Excluded(now + Duration::hours(2)),
                        ))
                        .backend(db::room::RoomBackend::Janus)
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
                    Bound::Included(now + Duration::hours(2)),
                    Bound::Excluded(now + Duration::hours(3)),
                );

                let payload = UpdateRequest {
                    id: room.id(),
                    time: Some(time),
                    reserve: Some(Some(123)),
                    tags: Some(json!({"foo": "bar"})),
                    audience: None,
                    backend: None,
                };

                let messages = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Room update failed");

                // Assert response.
                let (resp_room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_room.id(), room.id());
                assert_eq!(resp_room.audience(), room.audience());
                assert_eq!(resp_room.time(), &time);
                assert_eq!(resp_room.backend(), db::room::RoomBackend::Janus);
                assert_eq!(resp_room.reserve(), Some(123));
                assert_eq!(resp_room.tags(), &json!({"foo": "bar"}));
            });
        }

        #[test]
        fn update_room_with_wrong_time() {
            async_std::task::block_on(async {
                let db = TestDb::new();
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
                        .backend(db::room::RoomBackend::Janus)
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
                    audience: None,
                    backend: None,
                };

                handle_request::<UpdateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Room update succeeded when it should've failed");
            });
        }

        #[test]
        fn update_and_close_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();
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
                        .backend(db::room::RoomBackend::Janus)
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
                    audience: None,
                    backend: None,
                    tags: None,
                };

                let messages = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Room update failed");

                assert_eq!(messages.len(), 4);

                let (closed_tenant_notification, _, _) = find_event_by_predicate::<JsonValue, _>(
                    messages.as_slice(),
                    |evp, _, topic| evp.label() == "room.close" && topic.contains("audiences"),
                )
                .expect("Failed to find room.close event");

                assert_eq!(
                    closed_tenant_notification
                        .get("id")
                        .and_then(|v| v.as_str()),
                    Some(room.id().to_string()).as_deref()
                );

                let (closed_room_notification, _, _) = find_event_by_predicate::<JsonValue, _>(
                    messages.as_slice(),
                    |evp, _, topic| evp.label() == "room.close" && topic.contains("rooms"),
                )
                .expect("Failed to find room.close event");

                assert_eq!(
                    closed_room_notification.get("id").and_then(|v| v.as_str()),
                    Some(room.id().to_string()).as_deref()
                );
            });
        }

        #[test]
        fn update_room_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

                let payload = UpdateRequest {
                    id: Uuid::new_v4(),
                    ..Default::default()
                };

                let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room update");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }

        #[test]
        fn update_room_closed() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

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
                    ..Default::default()
                };

                let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room update");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_closed");
            });
        }

        #[test]
        fn update_room_move_opening_time_to_the_future() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let reader = TestAgent::new("web", "reader", USR_AUDIENCE);

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Insert a room with an active agent.
                    let room = shared_helpers::insert_room(&conn);
                    shared_helpers::insert_agent(&conn, reader.agent_id(), room.id());
                    room
                };

                // Allow updating the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();
                authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");
                let mut context = TestContext::new(db.clone(), authz);

                // Move the room for tomorrow.
                let opened_at = match room.time() {
                    (Bound::Included(dt), _) => *dt,
                    _ => panic!("Invalid room time"),
                };

                let payload = UpdateRequest {
                    id: room.id(),
                    time: Some((
                        Bound::Included(opened_at + Duration::days(1)),
                        Bound::Unbounded,
                    )),
                    ..Default::default()
                };

                let messages = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Failed to update room");

                // Assert `room.leave` notification to the agent.
                let (raw_payload, evp, topic) =
                    find_event_by_predicate::<serde_json::Value, _>(&messages, |evp, _, _| {
                        evp.label() == "room.leave"
                    })
                    .expect("Expected to find `room.leave` event");

                let payload: RoomLeavePayload = serde_json::from_value(raw_payload)
                    .expect("Failed to parse `room.leave` payload");

                assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
                assert_eq!(evp.label(), "room.leave");
                assert_eq!(payload.id, room.id());
                assert_eq!(&payload.agent_id, reader.agent_id());

                // Assert the agent to be deleted from the DB.
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                let agents = AgentListQuery::new()
                    .room_id(room.id())
                    .execute(&conn)
                    .expect("Failed to list agents");

                assert!(agents.is_empty());
            });
        }

        #[test]
        fn update_room_move_opening_time_to_the_future_with_rtc() {
            async_std::task::block_on(async {
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Insert a room with an rtc.
                    let room = shared_helpers::insert_room(&conn);
                    shared_helpers::insert_rtc_with_room(&conn, &room);
                    room
                };

                // Allow updating the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();
                authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");
                let mut context = TestContext::new(db, authz);

                // Move the room for tomorrow.
                let opened_at = match room.time() {
                    (Bound::Included(dt), _) => *dt,
                    _ => panic!("Invalid room time"),
                };

                let payload = UpdateRequest {
                    id: room.id(),
                    time: Some((
                        Bound::Included(opened_at + Duration::days(1)),
                        Bound::Unbounded,
                    )),
                    ..Default::default()
                };

                let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Expected room.update to fail");

                assert_eq!(err.status(), ResponseStatus::UNPROCESSABLE_ENTITY);
                assert_eq!(err.kind(), "room_time_changing_forbidden");
            });
        }
    }

    mod delete {
        use diesel::prelude::*;

        use crate::db::room::Object as Room;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn delete_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();

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
                authz.allow(agent.account_id(), vec!["rooms", &room_id], "delete");

                // Make room.delete request.
                let mut context = TestContext::new(db, authz);
                let payload = DeleteRequest { id: room.id() };

                let messages = handle_request::<DeleteHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Room deletion failed");

                // Assert response.
                let (resp_room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_room.audience(), room.audience());
                assert_eq!(resp_room.time(), room.time());
                assert_eq!(resp_room.backend(), room.backend());

                // Assert room absence in the DB.
                let conn = context.get_conn().unwrap();
                let query = crate::schema::room::table.find(room.id());
                assert_eq!(query.execute(&conn).unwrap(), 0);
            });
        }

        #[test]
        fn delete_room_not_authorized() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    shared_helpers::insert_room(&conn)
                };

                let mut context = TestContext::new(db, TestAuthz::new());
                let payload = DeleteRequest { id: room.id() };

                let err = handle_request::<DeleteHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room deletion");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }

        #[test]
        fn delete_room_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = DeleteRequest { id: Uuid::new_v4() };

                let err = handle_request::<DeleteHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room deletion");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }

    mod enter {
        use crate::test_helpers::prelude::*;

        use super::super::*;
        use super::DynSubRequest;

        #[test]
        fn enter_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();

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

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "events"],
                    "subscribe",
                );

                // Make room.enter request.
                let mut context = TestContext::new(db, authz);
                let payload = EnterRequest { id: room.id() };

                let messages = handle_request::<EnterHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Room entrance failed");

                // Assert dynamic subscription request.
                let (payload, reqp, topic) = find_request::<DynSubRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    agent.agent_id(),
                    MQTT_GW_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(reqp.method(), "subscription.create");
                assert_eq!(payload.subject, agent.agent_id().to_owned());
                assert_eq!(payload.object, vec!["rooms", &room_id, "events"]);
            });
        }

        #[test]
        fn enter_room_not_authorized() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

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
            });
        }

        #[test]
        fn enter_room_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = EnterRequest { id: Uuid::new_v4() };

                let err = handle_request::<EnterHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room entering");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }

        #[test]
        fn enter_room_closed() {
            async_std::task::block_on(async {
                let db = TestDb::new();

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

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "events"],
                    "subscribe",
                );

                // Make room.enter request.
                let mut context = TestContext::new(db, authz);
                let payload = EnterRequest { id: room.id() };

                let err = handle_request::<EnterHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room entering");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_closed");
            });
        }

        #[test]
        fn enter_room_with_no_opening_time() {
            async_std::task::block_on(async {
                let db = TestDb::new();

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

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "events"],
                    "subscribe",
                );

                // Make room.enter request.
                let mut context = TestContext::new(db, authz);
                let payload = EnterRequest { id: room.id() };

                let err = handle_request::<EnterHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room entering");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_closed");
            });
        }

        #[test]
        fn enter_room_that_opens_in_the_future() {
            async_std::task::block_on(async {
                let db = TestDb::new();

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

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "events"],
                    "subscribe",
                );

                // Make room.enter request.
                let mut context = TestContext::new(db, authz);
                let payload = EnterRequest { id: room.id() };

                handle_request::<EnterHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Room entrance failed");
            });
        }
    }

    mod leave {
        use crate::test_helpers::prelude::*;

        use super::super::*;
        use super::DynSubRequest;

        #[test]
        fn leave_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();
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
                    "agents/{}/api/{}/in/{}",
                    agent.agent_id(),
                    MQTT_GW_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(reqp.method(), "subscription.delete");
                assert_eq!(&payload.subject, agent.agent_id());

                let room_id = room.id().to_string();
                assert_eq!(payload.object, vec!["rooms", &room_id, "events"]);
            });
        }

        #[test]
        fn leave_room_while_not_entered() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

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
            });
        }

        #[test]
        fn leave_room_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = LeaveRequest { id: Uuid::new_v4() };

                let err = handle_request::<LeaveHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room leaving");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }

    mod notify_opened {
        use std::ops::Bound;

        use chrono::{Duration, Utc};

        use crate::app::API_VERSION;
        use crate::db::room::{Object as Room, RoomBackend};
        use crate::test_helpers::outgoing_envelope::OutgoingEnvelopeProperties;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn notify_opened() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let now = Utc::now();

                // Create rooms.
                let rooms = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let durations = vec![
                        Duration::hours(-1),
                        Duration::seconds(-30),
                        Duration::seconds(-20),
                        Duration::seconds(30),
                        Duration::minutes(10),
                    ];

                    durations
                        .into_iter()
                        .map(|duration| {
                            factory::Room::new()
                                .audience(USR_AUDIENCE)
                                .time((
                                    Bound::Included(now + duration),
                                    Bound::Excluded(now + Duration::hours(1)),
                                ))
                                .backend(RoomBackend::Janus)
                                .insert(&conn)
                        })
                        .collect::<Vec<Room>>()
                };

                // Send `room.notify_opened` event.
                let mut context = TestContext::new(db, TestAuthz::new());
                let agent = TestAgent::new("alpha", "kruonis", SVC_AUDIENCE);
                let payload = NotifyOpenedEvent { duration: 60 };

                let messages = handle_event::<NotifyOpenedHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Recently opened rooms notification failed");

                // Collect and assert notified rooms from outgoing events.
                let mut notified_room_ids = vec![];

                for message in messages {
                    if let OutgoingEnvelopeProperties::Event(evp) = message.properties() {
                        let room = message.payload::<Room>();
                        notified_room_ids.push(room.id());

                        let expected_topic = format!(
                            "apps/conference.{}/api/{}/rooms/{}/events",
                            SVC_AUDIENCE,
                            API_VERSION,
                            room.id()
                        );

                        assert_eq!(message.topic(), expected_topic);
                        assert_eq!(evp.label(), "room.open");
                    }
                }

                for expected_room_id in &[rooms[1].id(), rooms[2].id()] {
                    if !notified_room_ids.contains(expected_room_id) {
                        panic!("Room {} is not notified", expected_room_id);
                    }
                }
            });
        }
    }
}
