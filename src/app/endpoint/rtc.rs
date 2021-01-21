use std::fmt;

use anyhow::Context as AnyhowContext;
use async_std::stream;
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{IncomingRequestProperties, IntoPublishableMessage, OutgoingResponse, ResponseStatus},
    Addressable,
};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::db;
use crate::diesel::Connection;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct ConnectResponseData {}

impl ConnectResponseData {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

pub(crate) type ConnectResponse = OutgoingResponse<ConnectResponseData>;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequest {
    room_id: Uuid,
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
        let room =
            helpers::find_room_by_id(context, payload.room_id, helpers::RoomTimeRequirement::Open)?;

        // Authorize room creation.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "create")
            .await?;

        // Create an rtc.
        let rtc = {
            let conn = context.get_conn()?;

            conn.transaction::<_, diesel::result::Error, _>(|| {
                let rtc = db::rtc::InsertQuery::new(room.id()).execute(&conn)?;
                db::recording::InsertQuery::new(rtc.id()).execute(&conn)?;
                Ok(rtc)
            })?
        };

        context.add_logger_tags(o!("rtc_id" => rtc.id().to_string()));

        // Respond and broadcast to the room topic.
        let response = helpers::build_response(
            ResponseStatus::CREATED,
            rtc.clone(),
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        );

        let notification = helpers::build_notification(
            "rtc.create",
            &format!("rooms/{}/events", room.id()),
            rtc,
            reqp,
            context.start_timestamp(),
        );

        Ok(Box::new(stream::from_iter(vec![response, notification])))
    }
}

////////////////////////////////////////////////////////////////////////////////

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
            helpers::find_room_by_rtc_id(context, payload.id, helpers::RoomTimeRequirement::Open)?;

        // Authorize rtc reading.
        let rtc_id = payload.id.to_string();
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs", &rtc_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "read")
            .await?;

        // Return rtc.
        let rtc = {
            let conn = context.get_conn()?;

            db::rtc::FindQuery::new()
                .id(payload.id)
                .execute(&conn)?
                .ok_or_else(|| anyhow!("RTC not found"))
                .error(AppErrorKind::RtcNotFound)?
        };

        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            rtc,
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        ))))
    }
}

////////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 25;

#[derive(Debug, Deserialize)]
pub(crate) struct ListRequest {
    room_id: Uuid,
    offset: Option<i64>,
    limit: Option<i64>,
}

pub(crate) struct ListHandler;

#[async_trait]
impl RequestHandler for ListHandler {
    type Payload = ListRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room =
            helpers::find_room_by_id(context, payload.room_id, helpers::RoomTimeRequirement::Open)?;

        // Authorize rtc listing.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "list")
            .await?;

        // Return rtc list.
        let mut query = db::rtc::ListQuery::new().room_id(payload.room_id);

        if let Some(offset) = payload.offset {
            query = query.offset(offset);
        }

        let limit = std::cmp::min(payload.limit.unwrap_or_else(|| MAX_LIMIT), MAX_LIMIT);
        query = query.limit(limit);

        let rtcs = {
            let conn = context.get_conn()?;
            query.execute(&conn)?
        };

        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            rtcs,
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        ))))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ConnectIntent {
    Read,
    Write,
}

impl fmt::Display for ConnectIntent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Read => write!(f, "read"),
            Self::Write => write!(f, "write"),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConnectRequest {
    id: Uuid,
    #[serde(default = "ConnectRequest::default_intent")]
    intent: ConnectIntent,
    label: Option<String>,
}

impl ConnectRequest {
    fn default_intent() -> ConnectIntent {
        ConnectIntent::Read
    }
}

pub(crate) struct ConnectHandler;

#[async_trait]
impl RequestHandler for ConnectHandler {
    type Payload = ConnectRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        context.add_logger_tags(o!(
            "rtc_id" => payload.id.to_string(),
            "intent" => payload.intent.to_string(),
        ));

        // Find room.
        let room =
            helpers::find_room_by_rtc_id(context, payload.id, helpers::RoomTimeRequirement::Open)?;

        context.add_logger_tags(o!("room_id" => room.id().to_string()));

        // Find agent connection and backend.
        let (agent_connection, backend) =
            helpers::find_agent_connection_with_backend(context, reqp.as_agent_id(), &room)?;

        // Authorize connecting to the rtc.
        let rtc_id = payload.id.to_string();
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs", &rtc_id];

        let action = match payload.intent {
            ConnectIntent::Read => "read",
            ConnectIntent::Write => "update",
        };

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, action)
            .await?;

        let req = match payload.intent {
            // Make `stream.read` request for reader.
            ConnectIntent::Read => context
                .janus_client()
                .read_stream_request(
                    reqp.clone(),
                    backend.id(),
                    backend.session_id(),
                    agent_connection.handle_id(),
                    payload.id,
                    context.start_timestamp(),
                    authz_time,
                )
                .map(|req| Box::new(req) as Box<dyn IntoPublishableMessage + Send>)
                .context("Error creating a backend request")
                .error(AppErrorKind::MessageBuildingFailed)?,
            // Create janus_rtc_stream and make `stream.create` request for writer.
            ConnectIntent::Write => {
                let janus_rtc_stream = {
                    let label = payload
                        .label
                        .as_ref()
                        .ok_or_else(|| anyhow!("Missing label"))
                        .error(AppErrorKind::InvalidPayload)?;

                    let conn = context.get_conn()?;

                    db::janus_rtc_stream::InsertQuery::new(
                        Uuid::new_v4(),
                        agent_connection.handle_id(),
                        payload.id,
                        backend.id(),
                        label,
                        reqp.as_agent_id(),
                    )
                    .execute(&conn)?
                };

                context.add_logger_tags(o!(
                    "rtc_stream_id" => janus_rtc_stream.id().to_string(),
                    "rtc_stream_label" => janus_rtc_stream.label().to_owned(),
                ));

                context
                    .janus_client()
                    .create_stream_request(
                        reqp.clone(),
                        backend.id(),
                        backend.session_id(),
                        agent_connection.handle_id(),
                        payload.id,
                        context.start_timestamp(),
                        authz_time,
                    )
                    .map(|req| Box::new(req) as Box<dyn IntoPublishableMessage + Send>)
                    .context("Error creating a backend request")
                    .error(AppErrorKind::MessageBuildingFailed)?
            }
        };

        Ok(Box::new(stream::once(req)))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod create {
        use crate::db::rtc::Object as Rtc;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn create() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert a room.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| shared_helpers::insert_room(&conn))
                    .unwrap();

                // Allow user to create rtcs in the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = room.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs"];
                authz.allow(agent.account_id(), object, "create");

                // Make rtc.create request.
                let mut context = TestContext::new(db, authz);
                let payload = CreateRequest { room_id: room.id() };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc creation failed");

                // Assert response.
                let (rtc, respp) = find_response::<Rtc>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::CREATED);
                assert_eq!(rtc.room_id(), room.id());

                // Assert notification.
                let (rtc, evp, topic) = find_event::<Rtc>(messages.as_slice());
                assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
                assert_eq!(evp.label(), "rtc.create");
                assert_eq!(rtc.room_id(), room.id());
            });
        }

        #[test]
        fn create_rtc_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = CreateRequest {
                    room_id: Uuid::new_v4(),
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }

        #[test]
        fn create_rtc_duplicate() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert a room.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| shared_helpers::insert_room(&conn))
                    .unwrap();

                // Allow user to create rtcs in the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = room.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs"];
                authz.allow(agent.account_id(), object, "create");

                // Make rtc.create request.
                let mut context = TestContext::new(db, authz);
                let payload = CreateRequest { room_id: room.id() };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc creation failed");

                // Assert response.
                let (rtc, respp) = find_response::<Rtc>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::CREATED);
                assert_eq!(rtc.room_id(), room.id());

                // Make rtc.create request second time.
                let payload = CreateRequest { room_id: room.id() };
                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                // This should fail with already exists
                assert_eq!(err.status(), ResponseStatus::UNPROCESSABLE_ENTITY);
                assert_eq!(err.kind(), "database_query_failed");
            });
        }

        #[test]
        fn create_rtc_unauthorized() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                // Insert a room.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| shared_helpers::insert_room(&conn))
                    .unwrap();

                // Make rtc.create request.
                let mut context = TestContext::new(db, TestAuthz::new());
                let payload = CreateRequest { room_id: room.id() };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }
    }

    mod read {
        use crate::db::rtc::Object as Rtc;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn read_rtc() {
            async_std::task::block_on(async {
                let db = TestDb::new();

                let rtc = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create rtc.
                    shared_helpers::insert_rtc(&conn)
                };

                // Allow agent to read the rtc.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(agent.account_id(), object, "read");

                // Make rtc.read request.
                let mut context = TestContext::new(db, authz);
                let payload = ReadRequest { id: rtc.id() };

                let messages = handle_request::<ReadHandler>(&mut context, &agent, payload)
                    .await
                    .expect("RTC reading failed");

                // Assert response.
                let (resp_rtc, respp) = find_response::<Rtc>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_rtc.room_id(), rtc.room_id());
            });
        }

        #[test]
        fn read_rtc_not_authorized() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let rtc = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    shared_helpers::insert_rtc(&conn)
                };

                let mut context = TestContext::new(db, TestAuthz::new());
                let payload = ReadRequest { id: rtc.id() };

                let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc reading");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }

        #[test]
        fn read_rtc_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = ReadRequest { id: Uuid::new_v4() };

                let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc reading");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }

    mod list {
        use crate::db::rtc::Object as Rtc;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn list_rtcs() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let rtc = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create rtc.
                    shared_helpers::insert_rtc(&conn)
                };

                // Allow agent to list rtcs in the room.
                let mut authz = TestAuthz::new();
                let room_id = rtc.room_id().to_string();
                let object = vec!["rooms", &room_id, "rtcs"];
                authz.allow(agent.account_id(), object, "list");

                // Make rtc.list request.
                let mut context = TestContext::new(db, authz);

                let payload = ListRequest {
                    room_id: rtc.room_id(),
                    offset: None,
                    limit: None,
                };

                let messages = handle_request::<ListHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc listing failed");

                // Assert response.
                let (rtcs, respp) = find_response::<Vec<Rtc>>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(rtcs.len(), 1);
                assert_eq!(rtcs[0].id(), rtc.id());
                assert_eq!(rtcs[0].room_id(), rtc.room_id());
            });
        }

        #[test]
        fn list_rtcs_not_authorized() {
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

                let payload = ListRequest {
                    room_id: room.id(),
                    offset: None,
                    limit: None,
                };

                let err = handle_request::<ListHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc listing");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }

        #[test]
        fn list_rtcs_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

                let payload = ListRequest {
                    room_id: Uuid::new_v4(),
                    offset: None,
                    limit: None,
                };

                let err = handle_request::<ListHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc listing");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }

    mod connect {
        use std::ops::Bound;

        use chrono::{Duration, Utc};
        use rand::Rng;
        use svc_agent::AgentId;

        use crate::app::API_VERSION;
        use crate::backend::janus;
        use crate::db::room::RoomBackend;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[derive(Deserialize)]
        struct JanusRequest {
            janus: String,
            session_id: i64,
            handle_id: i64,
            body: JanusRequestBody,
        }

        #[derive(Deserialize)]
        struct JanusRequestBody {
            method: String,
            id: Uuid,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct RtcConnectTransaction {
            rtc_id: String,
            session_id: i64,
            reqp: RtcConnectTransactionReqp,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct RtcConnectTransactionReqp {
            method: String,
            agent_id: AgentId,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct JanusAttachRequest {
            janus: String,
            plugin: String,
            session_id: i64,
            transaction: String,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct JanusAttachRequestTransaction {
            rtc_id: String,
            session_id: i64,
            reqp: JanusAttachRequestTransactionReqp,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct JanusAttachRequestTransactionReqp {
            method: String,
            agent_id: AgentId,
        }

        #[test]
        fn connect_to_rtc_as_reader() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                // Create an rtc.
                let (backend, rtc) = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);
                    shared_helpers::insert_connected_agent(&conn, agent.agent_id(), room.id());
                    (backend, rtc)
                };

                // Allow agent to read the rtc.
                let mut authz = TestAuthz::new();
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "rtcs", &rtc_id],
                    "read",
                );

                // Make `rtc.connect` request.
                let mut context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                    label: None,
                };

                let messages = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc connection failed");

                // Assert outgoing request to Janus.
                let (payload, _reqp, topic) = find_request::<JanusRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    backend.id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(payload.janus, "message");
                assert_eq!(payload.handle_id, 123);
                assert_eq!(payload.session_id, backend.session_id());
                assert_eq!(payload.body.method, "stream.read");
                assert_eq!(payload.body.id, rtc.id());
            });
        }

        #[test]
        fn connect_to_rtc_as_writer() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                // Create an rtc.
                let (backend, rtc) = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);
                    shared_helpers::insert_connected_agent(&conn, agent.agent_id(), room.id());
                    (backend, rtc)
                };

                // Allow agent to update the rtc.
                let mut authz = TestAuthz::new();
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "rtcs", &rtc_id],
                    "update",
                );

                // Make `rtc.connect` request.
                let mut context = TestContext::new(db.clone(), authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Write,
                    label: Some(String::from("test")),
                };

                let messages = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc connection failed");

                // Assert outgoing request to Janus.
                let (payload, _reqp, topic) = find_request::<JanusRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    backend.id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(payload.janus, "message");
                assert_eq!(payload.handle_id, 123);
                assert_eq!(payload.session_id, backend.session_id());
                assert_eq!(payload.body.method, "stream.create");
                assert_eq!(payload.body.id, rtc.id());

                // Assert janus rtc stream.
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                let janus_rtc_streams = db::janus_rtc_stream::ListQuery::new()
                    .rtc_id(rtc.id())
                    .execute(&conn)
                    .expect("Failed to get janus rtc stream");

                assert_eq!(janus_rtc_streams.len(), 1);
                assert_eq!(janus_rtc_streams[0].label(), "test");
                assert_eq!(janus_rtc_streams[0].sent_by(), agent.agent_id());
                assert_eq!(janus_rtc_streams[0].backend_id(), backend.id());
            });
        }

        #[test]
        fn connect_to_rtc_reserve_overflow() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                let new_reader = TestAgent::new("web", "new-reader", USR_AUDIENCE);

                let (rtcs, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        let now = Utc::now();

                        // Lets say we have a single backend with cap=800
                        // Somehow reserves of all rooms that were allocated to it overflow its capacity
                        // We should allow users to connect to rooms with reserves if reserve and cap allows them
                        // But not allow to connect to room with no reserve

                        // Insert alpha backend.
                        let backend = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            let id = agent.agent_id().to_owned();
                            factory::JanusBackend::new(id, rng.gen(), rng.gen())
                                .balancer_capacity(700)
                                .capacity(800)
                                .insert(&conn)
                        };

                        // Setup three rooms with 500, 600 and none.
                        let room1 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .backend(RoomBackend::Janus)
                            .backend_id(backend.id())
                            .reserve(500)
                            .insert(&conn);

                        let room2 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .reserve(600)
                            .backend(RoomBackend::Janus)
                            .backend_id(backend.id())
                            .insert(&conn);

                        let room3 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .backend(RoomBackend::Janus)
                            .backend_id(backend.id())
                            .insert(&conn);

                        // Insert rtcs for each room.
                        let rtc1 = shared_helpers::insert_rtc_with_room(&conn, &room1);
                        let rtc2 = shared_helpers::insert_rtc_with_room(&conn, &room2);
                        let rtc3 = shared_helpers::insert_rtc_with_room(&conn, &room3);

                        // Insert writer for room 1
                        let agent = TestAgent::new("web", "writer1", USR_AUDIENCE);
                        shared_helpers::insert_connected_agent(&conn, agent.agent_id(), room1.id());

                        // Insert 450 readers for room 1
                        for i in 0..450 {
                            let agent =
                                TestAgent::new("web", &format!("reader1-{}", i), USR_AUDIENCE);

                            shared_helpers::insert_connected_agent(
                                &conn,
                                agent.agent_id(),
                                room1.id(),
                            );
                        }

                        // Insert writer for room 3
                        let agent = TestAgent::new("web", "writer3", USR_AUDIENCE);
                        shared_helpers::insert_connected_agent(&conn, agent.agent_id(), room2.id());

                        // Insert reader for room 3
                        let agent = TestAgent::new("web", "reader3", USR_AUDIENCE);
                        shared_helpers::insert_connected_agent(&conn, agent.agent_id(), room3.id());

                        // Connect agent.
                        for room in &[room1, room2, room3] {
                            shared_helpers::insert_connected_agent(
                                &conn,
                                new_reader.agent_id(),
                                room.id(),
                            );
                        }

                        ([rtc1, rtc2, rtc3], backend)
                    })
                    .unwrap();

                // Allow user to read the rtcs.
                for rtc in rtcs.iter() {
                    let room_id = rtc.room_id().to_string();
                    let rtc_id = rtc.id().to_string();
                    let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                    authz.allow(new_reader.account_id(), object, "read");
                }

                let mut context = TestContext::new(db, authz);

                // First two rooms have reserves AND there is free capacity so we can connect to them
                for rtc in rtcs.iter().take(2) {
                    let payload = ConnectRequest {
                        id: rtc.id(),
                        intent: ConnectIntent::Read,
                        label: None,
                    };

                    // Make an rtc.connect request.
                    let messages =
                        handle_request::<ConnectHandler>(&mut context, &new_reader, payload)
                            .await
                            .expect("RTC connect failed");

                    // Assert outgoing request goes to the expected backend.
                    let (_req, _reqp, topic) = find_request::<JanusRequest>(messages.as_slice());

                    let expected_topic = format!(
                        "agents/{}/api/{}/in/{}",
                        backend.id(),
                        janus::JANUS_API_VERSION,
                        context.config().id,
                    );

                    assert_eq!(topic, &expected_topic);
                }

                let payload = ConnectRequest {
                    id: rtcs[2].id(),
                    intent: ConnectIntent::Read,
                    label: None,
                };

                // Last room has NO reserve AND there is free capacity BUT it was exhausted by first two rooms
                // So in theory we should not be able to connect to this room due to capacity_exceeded error
                // But we still let the user through because:
                //   1. we almost never fill any server with users upto max capacity
                //   2. thus there are unused slots anyway
                // So its better to let them in
                handle_request::<ConnectHandler>(&mut context, &new_reader, payload)
                    .await
                    .expect("RTC connect failed");
            });
        }

        #[test]
        fn connect_to_rtc_not_authorized() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let rtc = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_connected_agent(&conn, agent.agent_id(), room.id());
                    shared_helpers::insert_rtc_with_room(&conn, &room)
                };

                let mut context = TestContext::new(db, TestAuthz::new());

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                    label: None,
                };

                let err = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc connecting");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }

        #[test]
        fn connect_to_rtc_not_signaled() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let rtc = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    shared_helpers::insert_rtc_with_room(&conn, &room)
                };

                // Allow agent to update the rtc.
                let mut authz = TestAuthz::new();
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "rtcs", &rtc_id],
                    "update",
                );

                let mut context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                    label: None,
                };

                let err = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc connecting");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "agent_not_connected");
            });
        }

        #[test]
        fn connect_to_rtc_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

                let payload = ConnectRequest {
                    id: Uuid::new_v4(),
                    intent: ConnectIntent::Read,
                    label: None,
                };

                let err = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc connecting");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }
}
