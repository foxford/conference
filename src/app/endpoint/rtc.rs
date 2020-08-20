use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{IncomingRequestProperties, IntoPublishableMessage, OutgoingResponse, ResponseStatus},
    Addressable,
};
use svc_error::Error as SvcError;
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

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ConnectIntent {
    Read,
    Write,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConnectRequest {
    id: Uuid,
    #[serde(default = "ConnectRequest::default_intent")]
    intent: ConnectIntent,
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

        let action = match payload.intent {
            ConnectIntent::Read => "read",
            ConnectIntent::Write => "update",
        };

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, action)
            .await?;

        // Choose backend to connect.
        let backend = {
            let conn = context.db().get()?;

            // There are 3 cases:
            // 1. Connecting as writer with no previous stream. Select the least loaded backend
            //    that is capable to host the room's reservation.
            // 2. Connecting as reader with existing stream. Choose the backend of the active
            //    stream because Janus doesn't support clustering and it must be the same server
            //    that the stream's writer is connected to.
            // 3. Reconnecting as writer with previous stream. Select the backend of the previous
            //    stream to avoid partitioning the record across multiple servers.
            let maybe_rtc_stream = db::janus_rtc_stream::FindQuery::new()
                .rtc_id(payload.id)
                .execute(&conn)?;

            let backend = match maybe_rtc_stream {
                Some(ref stream) => db::janus_backend::FindQuery::new()
                    .id(stream.backend_id().to_owned())
                    .execute(&conn)?
                    .ok_or("no backend found for stream")
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?,
                None => db::janus_backend::least_loaded(room.id(), &conn)?
                    .ok_or("no available backends")
                    .status(ResponseStatus::SERVICE_UNAVAILABLE)?,
            };

            // Check that the backend's capacity is not exceeded for readers.
            if payload.intent == ConnectIntent::Read {
                if let Some(capacity) = backend.capacity() {
                    let agents_count = db::janus_backend::agents_count(backend.id(), &conn)?;

                    if agents_count >= capacity.into() {
                        let err = SvcError::builder()
                            .status(ResponseStatus::SERVICE_UNAVAILABLE)
                            .kind("capacity_exceeded", "Capacity exceeded")
                            .detail("active agents number on the backend exceeded its capacity")
                            .build();

                        return Err(err);
                    }
                }
            }

            backend
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
                let conn = context.db().get()?;

                db::agent::UpdateQuery::new(reqp.as_agent_id(), room.id())
                    .status(db::agent::Status::Connected)
                    .execute(&conn)?;

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
        use std::ops::Bound;

        use chrono::{Duration, Utc};
        use rand::Rng;
        use serde_json::Value as JsonValue;
        use svc_agent::{AccountId, AgentId};

        use crate::backend::janus;
        use crate::db::agent::Status as AgentStatus;
        use crate::db::room::RoomBackend;
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

                        // The first backend has 1 active stream with 1 agent.
                        let rtc1 = shared_helpers::insert_rtc(&conn);

                        let stream1 = factory::JanusRtcStream::new(USR_AUDIENCE)
                            .rtc(&rtc1)
                            .backend(&backend1)
                            .insert(&conn);

                        crate::db::janus_rtc_stream::start(stream1.id(), &conn).unwrap();

                        let s1a1 = TestAgent::new("web", "s1a1", USR_AUDIENCE);
                        shared_helpers::insert_agent(&conn, s1a1.agent_id(), rtc1.room_id());

                        // The second backend has 1 stream with 2 agents but it's not started
                        // so it doesn't make any load on the backend and should be selected
                        // by the balancer.
                        let rtc2 = shared_helpers::insert_rtc(&conn);

                        let _stream2 = factory::JanusRtcStream::new(USR_AUDIENCE)
                            .rtc(&rtc2)
                            .backend(&backend2)
                            .insert(&conn);

                        let s2a1 = TestAgent::new("web", "s2a1", USR_AUDIENCE);
                        shared_helpers::insert_agent(&conn, s2a1.agent_id(), rtc2.room_id());

                        let s2a2 = TestAgent::new("web", "s2a2", USR_AUDIENCE);
                        shared_helpers::insert_agent(&conn, s2a2.agent_id(), rtc2.room_id());

                        // The new rtc for which we will balance the stream.
                        let rtc3 = shared_helpers::insert_rtc(&conn);

                        (rtc3, backend2)
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

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                let messages = handle_request::<ConnectHandler>(&context, &agent, payload)
                    .await
                    .expect("RTC connect failed");

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

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                let messages = handle_request::<ConnectHandler>(&context, &agent, payload)
                    .await
                    .expect("RTC connect failed");

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
        fn connect_to_rtc_with_reservation() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                let (rtc, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        let now = Utc::now();

                        // Insert room with reserve.
                        let room1 = shared_helpers::insert_room(&conn);

                        let room2 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .backend(RoomBackend::Janus)
                            .reserve(15)
                            .insert(&conn);

                        // Insert rtcs.
                        let rtc1 = factory::Rtc::new(room1.id()).insert(&conn);
                        let rtc2 = factory::Rtc::new(room2.id()).insert(&conn);

                        // The first backend is big enough but has some load.
                        let backend1_id = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        let backend1 =
                            factory::JanusBackend::new(backend1_id, rng.gen(), rng.gen())
                                .capacity(20)
                                .insert(&conn);

                        // The second backend is too small but has no load.
                        let backend2_id = {
                            let agent = TestAgent::new("beta", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        factory::JanusBackend::new(backend2_id, rng.gen(), rng.gen())
                            .capacity(5)
                            .insert(&conn);

                        // Insert stream.
                        factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend1)
                            .rtc(&rtc1)
                            .insert(&conn);

                        // Insert active agent.
                        let agent = TestAgent::new("web", "user456", USR_AUDIENCE);
                        shared_helpers::insert_agent(&conn, agent.agent_id(), rtc1.room_id());

                        // It should balance to the first one despite of the load.
                        (rtc2, backend1)
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

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                let messages = handle_request::<ConnectHandler>(&context, &agent, payload)
                    .await
                    .expect("RTC connect failed");

                // Ensure we're balanced to the right backend.
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
        fn connect_to_rtc_as_last_reader() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                let writer = TestAgent::new("web", "writer", USR_AUDIENCE);
                let reader = TestAgent::new("web", "reader", USR_AUDIENCE);

                let rtc = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert rtc.
                        let rtc = shared_helpers::insert_rtc(&conn);

                        // Insert backend.
                        let backend_id = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        let backend = factory::JanusBackend::new(backend_id, rng.gen(), rng.gen())
                            .capacity(2)
                            .insert(&conn);

                        // Insert active stream.
                        factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend)
                            .rtc(&rtc)
                            .insert(&conn);

                        // Insert active agents.
                        shared_helpers::insert_agent(&conn, writer.agent_id(), rtc.room_id());

                        factory::Agent::new()
                            .agent_id(reader.agent_id())
                            .room_id(rtc.room_id())
                            .status(AgentStatus::Ready)
                            .insert(&conn);

                        rtc
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(reader.account_id(), object, "read");

                // Make rtc.connect request.
                let context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                handle_request::<ConnectHandler>(&context, &reader, payload)
                    .await
                    .expect("RTC connect failed");
            });
        }

        #[test]
        fn connect_to_rtc_full_server_as_reader() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                let writer = TestAgent::new("web", "writer", USR_AUDIENCE);
                let reader1 = TestAgent::new("web", "reader1", USR_AUDIENCE);
                let reader2 = TestAgent::new("web", "reader2", USR_AUDIENCE);

                let rtc = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert rtc.
                        let rtc = shared_helpers::insert_rtc(&conn);

                        // Insert backend.
                        let backend_id = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        let backend = factory::JanusBackend::new(backend_id, rng.gen(), rng.gen())
                            .capacity(2)
                            .insert(&conn);

                        // Insert active stream.
                        let stream = factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend)
                            .rtc(&rtc)
                            .insert(&conn);

                        crate::db::janus_rtc_stream::start(stream.id(), &conn).unwrap();

                        // Insert active agents.
                        shared_helpers::insert_agent(&conn, writer.agent_id(), rtc.room_id());
                        shared_helpers::insert_agent(&conn, reader1.agent_id(), rtc.room_id());

                        factory::Agent::new()
                            .agent_id(reader2.agent_id())
                            .room_id(rtc.room_id())
                            .status(AgentStatus::Ready)
                            .insert(&conn);

                        rtc
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(reader2.account_id(), object, "read");

                // Make rtc.connect request.
                let context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                let err = handle_request::<ConnectHandler>(&context, &reader2, payload)
                    .await
                    .expect_err("Unexpected success on rtc connecting");

                assert_eq!(err.status_code(), ResponseStatus::SERVICE_UNAVAILABLE);
                assert_eq!(err.kind(), "capacity_exceeded");
            });
        }

        #[test]
        fn connect_to_rtc_full_server_as_writer() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                let writer = TestAgent::new("web", "writer", USR_AUDIENCE);
                let reader = TestAgent::new("web", "reader", USR_AUDIENCE);

                let rtc = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert rtc.
                        let rtc = shared_helpers::insert_rtc(&conn);

                        // Insert backend.
                        let backend_id = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        let backend = factory::JanusBackend::new(backend_id, rng.gen(), rng.gen())
                            .capacity(1)
                            .insert(&conn);

                        // Insert active stream.
                        factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend)
                            .rtc(&rtc)
                            .insert(&conn);

                        // Insert active agents.
                        shared_helpers::insert_agent(&conn, reader.agent_id(), rtc.room_id());

                        factory::Agent::new()
                            .agent_id(writer.agent_id())
                            .room_id(rtc.room_id())
                            .status(AgentStatus::Ready)
                            .insert(&conn);

                        rtc
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(writer.account_id(), object, "update");

                // Make rtc.connect request.
                let context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Write,
                };

                handle_request::<ConnectHandler>(&context, &writer, payload)
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

                    shared_helpers::insert_rtc(&conn)
                };

                let context = TestContext::new(db, TestAuthz::new());

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

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

                let payload = ConnectRequest {
                    id: Uuid::new_v4(),
                    intent: ConnectIntent::Read,
                };

                let err = handle_request::<ConnectHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc connecting");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }
}
