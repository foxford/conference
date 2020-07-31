use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::mqtt::{
    IncomingRequestProperties, IntoPublishableMessage, OutgoingResponse, ResponseStatus,
};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::handle_id::HandleId;
use crate::app::janus;
use crate::db;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct ConnectResponseData {
    handle_id: HandleId,
}

impl ConnectResponseData {
    pub(crate) fn new(handle_id: HandleId) -> Self {
        Self { handle_id }
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
    const ERROR_TITLE: &'static str = "Failed to create rtc";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let conn = context.db().get()?;

            db::room::FindQuery::new()
                .time(db::room::now())
                .id(payload.room_id)
                .execute(&conn)?
                .ok_or_else(|| format!("the room = '{}' is not found", payload.room_id))
                .status(ResponseStatus::NOT_FOUND)?
        };

        // Authorize room creation.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "create")
            .await?;

        // Create an rtc.
        let rtc = {
            let conn = context.db().get()?;
            db::rtc::InsertQuery::new(room.id()).execute(&conn)?
        };

        // Respond and broadcast to the room topic.
        let response = shared::build_response(
            ResponseStatus::CREATED,
            rtc.clone(),
            reqp,
            start_timestamp,
            Some(authz_time),
        );

        let notification = shared::build_notification(
            "room.create",
            &format!("rooms/{}/events", room.id()),
            rtc,
            reqp,
            start_timestamp,
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
    const ERROR_TITLE: &'static str = "Failed to read rtc";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let conn = context.db().get()?;

            db::room::FindQuery::new()
                .time(db::room::now())
                .rtc_id(payload.id)
                .execute(&conn)?
                .ok_or_else(|| format!("a room for the rtc = '{}' is not found", payload.id))
                .status(ResponseStatus::NOT_FOUND)?
        };

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
            let conn = context.db().get()?;

            db::rtc::FindQuery::new()
                .id(payload.id)
                .execute(&conn)?
                .ok_or_else(|| format!("RTC not found, id = '{}'", payload.id))
                .status(ResponseStatus::NOT_FOUND)?
        };

        Ok(Box::new(stream::once(shared::build_response(
            ResponseStatus::OK,
            rtc,
            reqp,
            start_timestamp,
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
    const ERROR_TITLE: &'static str = "Failed to list rtcs";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let conn = context.db().get()?;

            db::room::FindQuery::new()
                .time(db::room::now())
                .id(payload.room_id)
                .execute(&conn)?
                .ok_or_else(|| format!("the room = '{}' is not found", payload.room_id))
                .status(ResponseStatus::NOT_FOUND)?
        };

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
            let conn = context.db().get()?;
            query.execute(&conn)?
        };

        Ok(Box::new(stream::once(shared::build_response(
            ResponseStatus::OK,
            rtcs,
            reqp,
            start_timestamp,
            Some(authz_time),
        ))))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct ConnectRequest {
    id: Uuid,
}

pub(crate) struct ConnectHandler;

#[async_trait]
impl RequestHandler for ConnectHandler {
    type Payload = ConnectRequest;
    const ERROR_TITLE: &'static str = "Failed to connect to rtc";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let conn = context.db().get()?;

            db::room::FindQuery::new()
                .time(db::room::now())
                .rtc_id(payload.id)
                .execute(&conn)?
                .ok_or_else(|| format!("a room for the rtc = '{}' is not found", payload.id))
                .status(ResponseStatus::NOT_FOUND)?
        };

        // Authorize connecting to the rtc.
        if room.backend() != db::room::RoomBackend::Janus {
            return Err(format!(
                "'rtc.connect' is not implemented for the backend = '{}'.",
                room.backend(),
            ))
            .status(ResponseStatus::NOT_IMPLEMENTED);
        }

        let rtc_id = payload.id.to_string();
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs", &rtc_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "read")
            .await?;

        let backend = {
            let conn = context.db().get()?;

            // If there is an active stream choose its backend since Janus doesn't support
            // clustering so all agents within one rtc must be sent to the same node. If there's no
            // active stream it means we're connecting as publisher and going to create it.
            // Then select the least loaded node: the one with the least active rtc streams count.
            let maybe_rtc_stream = db::janus_rtc_stream::FindQuery::new()
                .rtc_id(payload.id)
                .active(true)
                .execute(&conn)?;

            match maybe_rtc_stream {
                Some(ref stream) => db::janus_backend::FindQuery::new()
                    .id(stream.backend_id().to_owned())
                    .execute(&conn)?
                    .ok_or("no backend found for stream")
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?,
                None => db::janus_backend::least_loaded(&conn)?
                    .ok_or("no available backends")
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?,
            }
        };

        // Send janus handle creation request.
        let janus_request_result = janus::create_rtc_handle_request(
            reqp.clone(),
            Uuid::new_v4(),
            payload.id,
            backend.session_id(),
            backend.id(),
            context.agent_id(),
            start_timestamp,
            authz_time,
        );

        match janus_request_result {
            Ok(req) => {
                let boxed_request = Box::new(req) as Box<dyn IntoPublishableMessage + Send>;
                Ok(Box::new(stream::once(boxed_request)))
            }
            Err(err) => Err(format!("error creating a backend request: {}", err))
                .status(ResponseStatus::UNPROCESSABLE_ENTITY),
        }
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
                let context = TestContext::new(db, authz);
                let payload = CreateRequest { room_id: room.id() };

                let messages = handle_request::<CreateHandler>(&context, &agent, payload)
                    .await
                    .expect("Rtc creation failed");

                // Assert response.
                let (rtc, respp) = find_response::<Rtc>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::CREATED);
                assert_eq!(rtc.room_id(), room.id());

                // Assert notification.
                let (rtc, evp, topic) = find_event::<Rtc>(messages.as_slice());
                assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
                assert_eq!(evp.label(), "room.create");
                assert_eq!(rtc.room_id(), room.id());
            });
        }

        #[test]
        fn create_rtc_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = CreateRequest {
                    room_id: Uuid::new_v4(),
                };

                let err = handle_request::<CreateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
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
                let context = TestContext::new(db, TestAuthz::new());
                let payload = CreateRequest { room_id: room.id() };

                let err = handle_request::<CreateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
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
                let context = TestContext::new(db, authz);
                let payload = ReadRequest { id: rtc.id() };

                let messages = handle_request::<ReadHandler>(&context, &agent, payload)
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

                let context = TestContext::new(db, TestAuthz::new());
                let payload = ReadRequest { id: rtc.id() };

                let err = handle_request::<ReadHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc reading");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn read_rtc_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = ReadRequest { id: Uuid::new_v4() };

                let err = handle_request::<ReadHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc reading");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
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
                let context = TestContext::new(db, authz);

                let payload = ListRequest {
                    room_id: rtc.room_id(),
                    offset: None,
                    limit: None,
                };

                let messages = handle_request::<ListHandler>(&context, &agent, payload)
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

                let context = TestContext::new(db, TestAuthz::new());

                let payload = ListRequest {
                    room_id: room.id(),
                    offset: None,
                    limit: None,
                };

                let err = handle_request::<ListHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc listing");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn list_rtcs_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());

                let payload = ListRequest {
                    room_id: Uuid::new_v4(),
                    offset: None,
                    limit: None,
                };

                let err = handle_request::<ListHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc listing");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }

    mod connect {
        use serde_json::Value as JsonValue;
        use svc_agent::{AccountId, AgentId};

        use crate::backend::janus;
        use crate::test_helpers::prelude::*;
        use crate::util::from_base64;

        use super::super::*;

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
        fn connect_to_rtc() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert an rtc and janus backend.
                let (rtc, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert janus backends.
                        let backend1 = shared_helpers::insert_janus_backend(&conn);
                        let backend2 = shared_helpers::insert_janus_backend(&conn);

                        // The first backend has 1 active stream.
                        let stream1 = factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend1)
                            .insert(&conn);

                        crate::db::janus_rtc_stream::start(stream1.id(), &conn).unwrap();

                        // The second backend has 1 stream that is not started
                        // so it's free and should be selected by the balancer.
                        let _stream2 = factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend2)
                            .insert(&conn);

                        let rtc = shared_helpers::insert_rtc(&conn);
                        (rtc, backend2)
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(agent.account_id(), object, "read");

                // Make rtc.connect request.
                let context = TestContext::new(db, authz);
                let payload = ConnectRequest { id: rtc.id() };

                let messages = handle_request::<ConnectHandler>(&context, &agent, payload)
                    .await
                    .expect("RTC reading failed");

                // Assert outgoing request to Janus.
                let (req, _reqp, topic) = find_request::<JanusAttachRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    backend.id(),
                    janus::JANUS_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, &expected_topic);
                assert_eq!(req.janus, "attach");
                assert_eq!(req.plugin, "janus.plugin.conference");
                assert_eq!(req.session_id, backend.session_id());

                // `transaction` field is base64 encoded JSON. Decode and assert.
                let txn_wrap: JsonValue = from_base64(&req.transaction).unwrap();
                let txn_value = txn_wrap.get("CreateRtcHandle").unwrap().to_owned();
                let txn: RtcConnectTransaction = serde_json::from_value(txn_value).unwrap();

                assert_eq!(
                    txn,
                    RtcConnectTransaction {
                        rtc_id: rtc.id().to_string(),
                        session_id: backend.session_id(),
                        reqp: RtcConnectTransactionReqp {
                            method: "ignore".to_string(),
                            agent_id: AgentId::new("web", AccountId::new("user123", USR_AUDIENCE)),
                        }
                    }
                )
            });
        }

        #[test]
        fn connect_to_rtc_with_existing_stream() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert an rtc and janus backend.
                let (rtc, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        let rtc = shared_helpers::insert_rtc(&conn);

                        // Insert janus backends.
                        let _backend1 = shared_helpers::insert_janus_backend(&conn);
                        let backend2 = shared_helpers::insert_janus_backend(&conn);

                        // The second backend has an active stream already.
                        let stream = factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend2)
                            .rtc(&rtc)
                            .insert(&conn);

                        crate::db::janus_rtc_stream::start(stream.id(), &conn).unwrap();
                        (rtc, backend2)
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(agent.account_id(), object, "read");

                // Make rtc.connect request.
                let context = TestContext::new(db, authz);
                let payload = ConnectRequest { id: rtc.id() };

                let messages = handle_request::<ConnectHandler>(&context, &agent, payload)
                    .await
                    .expect("rtc reading failed");

                // Ensure we're balanced to the backend with the stream.
                let (req, _reqp, topic) = find_request::<JanusAttachRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    backend.id(),
                    janus::JANUS_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(req.session_id, backend.session_id());
            });
        }

        #[test]
        fn connect_to_rtc_migration_to_another_backend() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                let (rtc, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert rtcs and janus backends.
                        let rtc1 = shared_helpers::insert_rtc(&conn);
                        let rtc2 = shared_helpers::insert_rtc(&conn);
                        let backend1 = shared_helpers::insert_janus_backend(&conn);
                        let backend2 = shared_helpers::insert_janus_backend(&conn);

                        // The first backend has a finished stream for the first rtc…
                        let stream1 = factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend1)
                            .rtc(&rtc1)
                            .insert(&conn);

                        crate::db::janus_rtc_stream::start(stream1.id(), &conn).unwrap();
                        crate::db::janus_rtc_stream::stop_by_agent_id(stream1.sent_by(), &conn)
                            .unwrap();

                        // …and an active stream for the second rtc.
                        let stream2 = factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend1)
                            .rtc(&rtc2)
                            .insert(&conn);

                        crate::db::janus_rtc_stream::start(stream2.id(), &conn).unwrap();

                        // A new stream for the first rtc should start on the second backend.
                        (rtc1, backend2)
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(agent.account_id(), object, "read");

                // Make rtc.connect request.
                let context = TestContext::new(db, authz);
                let payload = ConnectRequest { id: rtc.id() };

                let messages = handle_request::<ConnectHandler>(&context, &agent, payload)
                    .await
                    .expect("rtc reading failed");

                // Ensure we're balanced to the least loaded backend.
                let (req, _reqp, topic) = find_request::<JanusAttachRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    backend.id(),
                    janus::JANUS_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(req.session_id, backend.session_id());
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

                    shared_helpers::insert_rtc(&conn)
                };

                let context = TestContext::new(db, TestAuthz::new());
                let payload = ConnectRequest { id: rtc.id() };

                let err = handle_request::<ConnectHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc connecting");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn connect_to_rtc_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = ConnectRequest { id: Uuid::new_v4() };

                let err = handle_request::<ConnectHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc connecting");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }
}
