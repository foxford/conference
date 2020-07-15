use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use std::ops::Bound;
use svc_agent::mqtt::{
    IncomingRequestProperties, IntoPublishableMessage, OutgoingRequest, ResponseStatus,
    ShortTermTimingProperties,
};
use svc_agent::{Addressable, AgentId};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::db;

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
    subscribers_limit: Option<i32>,
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
    const ERROR_TITLE: &'static str = "Failed to create room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
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

            if let Some(subscribers_limit) = payload.subscribers_limit {
                q = q.subscribers_limit(subscribers_limit);
            }

            let conn = context.db().get()?;
            q.execute(&conn)?
        };

        // Respond and broadcast to the audience topic.
        let response = shared::build_response(
            // TODO: Change to `ResponseStatus::CREATED` (breaking).
            ResponseStatus::OK,
            room.clone(),
            reqp,
            start_timestamp,
            Some(authz_time),
        );

        let notification = shared::build_notification(
            "room.create",
            &format!("audiences/{}/events", payload.audience),
            room,
            reqp,
            start_timestamp,
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
    const ERROR_TITLE: &'static str = "Failed to read room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let conn = context.db().get()?;

            db::room::FindQuery::new()
                .id(payload.id)
                .execute(&conn)?
                .ok_or_else(|| format!("Room not found, id = '{}'", payload.id))
                .status(ResponseStatus::NOT_FOUND)?
        };

        // Authorize room reading on the tenant.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "read")
            .await?;

        Ok(Box::new(stream::once(shared::build_response(
            ResponseStatus::OK,
            room,
            reqp,
            start_timestamp,
            Some(authz_time),
        ))))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) type UpdateRequest = db::room::UpdateQuery;
pub(crate) struct UpdateHandler;

#[async_trait]
impl RequestHandler for UpdateHandler {
    type Payload = UpdateRequest;
    const ERROR_TITLE: &'static str = "Failed to create room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let conn = context.db().get()?;

            db::room::FindQuery::new()
                .time(db::room::since_now())
                .id(payload.id())
                .execute(&conn)?
                .ok_or_else(|| format!("Room not found, id = '{}' or closed", payload.id()))
                .status(ResponseStatus::NOT_FOUND)?
        };

        // Authorize room updating on the tenant.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "update")
            .await?;

        // Update room.
        let room = {
            let conn = context.db().get()?;
            payload.execute(&conn)?
        };

        // Respond and broadcast to the audience topic.
        let response = shared::build_response(
            ResponseStatus::OK,
            room.clone(),
            reqp,
            start_timestamp,
            Some(authz_time),
        );

        let notification = shared::build_notification(
            "room.update",
            &format!("audiences/{}/events", room.audience()),
            room,
            reqp,
            start_timestamp,
        );

        Ok(Box::new(stream::from_iter(vec![response, notification])))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) type DeleteRequest = ReadRequest;
pub(crate) struct DeleteHandler;

#[async_trait]
impl RequestHandler for DeleteHandler {
    type Payload = DeleteRequest;
    const ERROR_TITLE: &'static str = "Failed to delete room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let conn = context.db().get()?;

            db::room::FindQuery::new()
                .time(db::room::since_now())
                .id(payload.id)
                .execute(&conn)?
                .ok_or_else(|| format!("Room not found, id = '{}' or closed", payload.id))
                .status(ResponseStatus::NOT_FOUND)?
        };

        // Authorize room deletion on the tenant.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "delete")
            .await?;

        // Delete room.
        {
            let conn = context.db().get()?;
            db::room::DeleteQuery::new(room.id()).execute(&conn)?;
        }

        // Respond and broadcast to the audience topic.
        let response = shared::build_response(
            ResponseStatus::OK,
            room.clone(),
            reqp,
            start_timestamp,
            Some(authz_time),
        );

        let notification = shared::build_notification(
            "room.update",
            &format!("audiences/{}/events", room.audience()),
            room,
            reqp,
            start_timestamp,
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
    const ERROR_TITLE: &'static str = "Failed to enter room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let conn = context.db().get()?;

            // Find opened room.
            let room = db::room::FindQuery::new()
                .id(payload.id)
                .time(db::room::now())
                .execute(&conn)?
                .ok_or_else(|| format!("Room not found or closed, id = '{}'", payload.id))
                .status(ResponseStatus::NOT_FOUND)?;

            // Check if not full house in the room.
            if let Some(subscribers_limit) = room.subscribers_limit() {
                let agents_count = db::agent::RoomCountQuery::new(room.id()).execute(&conn)?;

                if agents_count > subscribers_limit as i64 {
                    let err = format!(
                        "Subscribers limit in the room has been reached ({})",
                        subscribers_limit
                    );

                    return Err(err).status(ResponseStatus::SERVICE_UNAVAILABLE)?;
                }
            }

            room
        };

        // Authorize subscribing to the room's events.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "events"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object.clone(), "subscribe")
            .await?;

        // Register agent in `in_progress` state.
        {
            let conn = context.db().get()?;
            db::agent::InsertQuery::new(reqp.as_agent_id(), room.id()).execute(&conn)?;
        }

        // Send dynamic subscription creation request to the broker.
        let payload = SubscriptionRequest::new(reqp.as_agent_id().to_owned(), object);

        let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
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
    const ERROR_TITLE: &'static str = "Failed to leave room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let (room, presence) = {
            let conn = context.db().get()?;

            let room = db::room::FindQuery::new()
                .id(payload.id)
                .execute(&conn)?
                .ok_or_else(|| format!("Room not found, id = '{}'", payload.id))
                .status(ResponseStatus::NOT_FOUND)?;

            // Check room presence.
            let presence = db::agent::ListQuery::new()
                .room_id(room.id())
                .agent_id(reqp.as_agent_id())
                .status(db::agent::Status::Ready)
                .execute(&conn)?;

            (room, presence)
        };

        if presence.is_empty() {
            return Err(format!(
                "agent = '{}' is not online in the room = '{}'",
                reqp.as_agent_id(),
                room.id()
            ))
            .status(ResponseStatus::NOT_FOUND);
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
            ShortTermTimingProperties::until_now(start_timestamp),
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

        use chrono::{SubsecRound, Utc};

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
                let context = TestContext::new(TestDb::new(), authz);
                let now = Utc::now().trunc_subsecs(0);
                let time = (Bound::Included(now), Bound::Unbounded);

                let payload = CreateRequest {
                    time: time.clone(),
                    audience: USR_AUDIENCE.to_owned(),
                    backend: db::room::RoomBackend::Janus,
                    subscribers_limit: Some(123),
                };

                let messages = handle_request::<CreateHandler>(&context, &agent, payload)
                    .await
                    .expect("Room creation failed");

                // Assert response.
                let (room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(room.audience(), USR_AUDIENCE);
                assert_eq!(room.time(), &time);
                assert_eq!(room.backend(), db::room::RoomBackend::Janus);
                assert_eq!(room.subscribers_limit(), Some(123));

                // Assert notification.
                let (room, evp, topic) = find_event::<Room>(messages.as_slice());
                assert!(topic.ends_with(&format!("/audiences/{}/events", USR_AUDIENCE)));
                assert_eq!(evp.label(), "room.create");
                assert_eq!(room.audience(), USR_AUDIENCE);
                assert_eq!(room.time(), &time);
                assert_eq!(room.backend(), db::room::RoomBackend::Janus);
                assert_eq!(room.subscribers_limit(), Some(123));
            });
        }

        #[test]
        fn create_room_unauthorized() {
            async_std::task::block_on(async {
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                // Make room.create request.
                let payload = CreateRequest {
                    time: (Bound::Included(Utc::now()), Bound::Unbounded),
                    audience: USR_AUDIENCE.to_owned(),
                    backend: db::room::RoomBackend::Janus,
                    subscribers_limit: None,
                };

                let err = handle_request::<CreateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room creation");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
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
                let context = TestContext::new(db, authz);
                let payload = ReadRequest { id: room.id() };

                let messages = handle_request::<ReadHandler>(&context, &agent, payload)
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

                let context = TestContext::new(db, TestAuthz::new());
                let payload = ReadRequest { id: room.id() };

                let err = handle_request::<ReadHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room reading");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn read_room_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = ReadRequest { id: Uuid::new_v4() };

                let err = handle_request::<ReadHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room reading");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }

    mod update {
        use std::ops::Bound;

        use chrono::{Duration, SubsecRound, Utc};

        use crate::db::room::Object as Room;
        use crate::test_helpers::prelude::*;

        use super::super::*;

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
                let context = TestContext::new(db, authz);

                let time = (
                    Bound::Included(now + Duration::hours(2)),
                    Bound::Excluded(now + Duration::hours(3)),
                );

                let payload = UpdateRequest::new(room.id())
                    .time(time)
                    .subscribers_limit(Some(123));

                let messages = handle_request::<UpdateHandler>(&context, &agent, payload)
                    .await
                    .expect("Room update failed");

                // Assert response.
                let (resp_room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_room.id(), room.id());
                assert_eq!(resp_room.audience(), room.audience());
                assert_eq!(resp_room.time(), &time);
                assert_eq!(resp_room.backend(), db::room::RoomBackend::Janus);
                assert_eq!(resp_room.subscribers_limit(), Some(123));
            });
        }

        #[test]
        fn update_room_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = UpdateRequest::new(Uuid::new_v4());

                let err = handle_request::<UpdateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room update");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
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

                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = UpdateRequest::new(room.id());

                let err = handle_request::<UpdateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room update");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
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
                let context = TestContext::new(db, authz);
                let payload = DeleteRequest { id: room.id() };

                let messages = handle_request::<DeleteHandler>(&context, &agent, payload)
                    .await
                    .expect("Room deletion failed");

                // Assert response.
                let (resp_room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_room.audience(), room.audience());
                assert_eq!(resp_room.time(), room.time());
                assert_eq!(resp_room.backend(), room.backend());

                // Assert room absence in the DB.
                let conn = context.db().get().unwrap();
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

                let context = TestContext::new(db, TestAuthz::new());
                let payload = DeleteRequest { id: room.id() };

                let err = handle_request::<DeleteHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room deletion");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn delete_room_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = DeleteRequest { id: Uuid::new_v4() };

                let err = handle_request::<DeleteHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room deletion");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }

    mod enter {
        use chrono::SubsecRound;

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
                let context = TestContext::new(db, authz);
                let payload = EnterRequest { id: room.id() };

                let messages = handle_request::<EnterHandler>(&context, &agent, payload)
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
        fn enter_room_full_house() {
            async_std::task::block_on(async {
                let db = TestDb::new();

                // Create room and fill it up with agents.
                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let now = Utc::now().trunc_subsecs(0);

                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((Bound::Included(now), Bound::Unbounded))
                        .backend(db::room::RoomBackend::Janus)
                        .subscribers_limit(1)
                        .insert(&conn);

                    let publisher = TestAgent::new("web", "publisher", USR_AUDIENCE);
                    shared_helpers::insert_agent(&conn, publisher.agent_id(), room.id());

                    let subscriber = TestAgent::new("web", "subscriber", USR_AUDIENCE);
                    shared_helpers::insert_agent(&conn, subscriber.agent_id(), room.id());

                    room
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
                let context = TestContext::new(db, authz);
                let payload = EnterRequest { id: room.id() };

                let err = handle_request::<EnterHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room entering");

                assert_eq!(err.status_code(), ResponseStatus::SERVICE_UNAVAILABLE);
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

                let context = TestContext::new(db, TestAuthz::new());
                let payload = EnterRequest { id: room.id() };

                let err = handle_request::<EnterHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room entering");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn enter_room_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = EnterRequest { id: Uuid::new_v4() };

                let err = handle_request::<EnterHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room entering");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
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
                let context = TestContext::new(db, TestAuthz::new());
                let payload = EnterRequest { id: room.id() };

                let err = handle_request::<EnterHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room entering");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
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
                let context = TestContext::new(db, TestAuthz::new());
                let payload = LeaveRequest { id: room.id() };

                let messages = handle_request::<LeaveHandler>(&context, &agent, payload)
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

                let context = TestContext::new(db, TestAuthz::new());
                let payload = LeaveRequest { id: room.id() };

                let err = handle_request::<LeaveHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room leaving");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }

        #[test]
        fn leave_room_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = LeaveRequest { id: Uuid::new_v4() };

                let err = handle_request::<LeaveHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room leaving");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }
}
