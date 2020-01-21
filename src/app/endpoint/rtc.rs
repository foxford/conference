use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{IncomingRequest, OutgoingResponse, ResponseStatus, ShortTermTimingProperties},
    AgentId,
};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint::shared;
use crate::app::{endpoint, API_VERSION};
use crate::db::{janus_backend, janus_rtc_stream, room, rtc, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 25;

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    room_id: Uuid,
}

pub(crate) type ReadRequest = IncomingRequest<ReadRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ReadRequestData {
    id: Uuid,
}

pub(crate) type ListRequest = IncomingRequest<ListRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ListRequestData {
    room_id: Uuid,
    offset: Option<i64>,
    limit: Option<i64>,
}

pub(crate) type ConnectRequest = IncomingRequest<ConnectRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ConnectRequestData {
    id: Uuid,
}

#[derive(Debug, Serialize)]
pub(crate) struct ConnectResponseData {
    handle_id: super::rtc_signal::HandleId,
}

impl ConnectResponseData {
    pub(crate) fn new(handle_id: super::rtc_signal::HandleId) -> Self {
        Self { handle_id }
    }
}

pub(crate) type ConnectResponse = OutgoingResponse<ConnectResponseData>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
    agent_id: AgentId,
}

impl State {
    pub(crate) fn new(authz: svc_authz::ClientMap, db: ConnectionPool, agent_id: AgentId) -> Self {
        Self {
            authz,
            db,
            agent_id,
        }
    }
}

impl State {
    pub(crate) async fn create(
        &self,
        inreq: CreateRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        let room_id = inreq.payload().room_id;

        // Authorization: room's owner has to allow the action
        let authz_time = {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .time(room::now())
                .id(room_id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?;

            let room_id = room.id().to_string();

            self.authz
                .authorize(
                    room.audience(),
                    inreq.properties(),
                    vec!["rooms", &room_id, "rtcs"],
                    "create",
                )
                .await
                .map_err(|err| SvcError::from(err))?
        };

        // Creating a Real-Time Connection
        let object = {
            let conn = self.db.get()?;
            rtc::InsertQuery::new(room_id).execute(&conn)?
        };

        shared::respond(
            &inreq,
            object,
            Some(("rtc.create", &format!("rooms/{}/events", room_id))),
            start_timestamp,
            authz_time,
        )
    }

    pub(crate) async fn connect(
        &self,
        inreq: ConnectRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        let id = inreq.payload().id;

        // Authorization
        let authz_time = {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .time(room::now())
                .rtc_id(id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("a room for the rtc = '{}' is not found", &id))
                        .build()
                })?;

            if room.backend() != &room::RoomBackend::Janus {
                return SvcError::builder()
                    .status(ResponseStatus::NOT_IMPLEMENTED)
                    .detail(&format!(
                        "'rtc.connect' is not implemented for the backend = '{}'.",
                        room.backend()
                    ))
                    .build()
                    .into();
            }

            let rtc_id = id.to_string();
            let room_id = room.id().to_string();

            self.authz
                .authorize(
                    room.audience(),
                    inreq.properties(),
                    vec!["rooms", &room_id, "rtcs", &rtc_id],
                    "read",
                )
                .await
                .map_err(|err| SvcError::from(err))?
        };

        let backend = {
            let conn = self.db.get()?;

            // If there is an active stream choose its backend since Janus doesn't support
            // clustering so all agents within one rtc must be sent to the same node. If there's no
            // active stream it means we're connecting as publisher and going to create it.
            // Then select the least loaded node: the one with the least active rtc streams count.
            match janus_rtc_stream::FindQuery::new()
                .rtc_id(id)
                .execute(&conn)?
            {
                Some(ref stream) => janus_backend::FindQuery::new()
                    .id(stream.backend_id().to_owned())
                    .execute(&conn)?
                    .ok_or_else(|| {
                        SvcError::builder()
                            .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                            .detail("no backend found for stream")
                            .build()
                    })?,
                None => janus_backend::least_loaded(&conn)?.ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                        .detail("no available backends")
                        .build()
                })?,
            }
        };

        // Building a Create Janus Gateway Handle request
        crate::app::janus::create_rtc_handle_request(
            inreq.properties().clone(),
            Uuid::new_v4(),
            id,
            backend.session_id(),
            backend.id(),
            &self.agent_id,
            start_timestamp,
            authz_time,
        )
        .map_err(|_| {
            SvcError::builder()
                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                .detail("error creating a backend request")
                .build()
        })?
        .into()
    }

    pub(crate) async fn read(
        &self,
        inreq: ReadRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        let id = inreq.payload().id;

        // Authorization
        let authz_time = {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .time(room::now())
                .rtc_id(id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("a room for the rtc = '{}' is not found", &id))
                        .build()
                })?;

            let rtc_id = id.to_string();
            let room_id = room.id().to_string();

            self.authz
                .authorize(
                    room.audience(),
                    inreq.properties(),
                    vec!["rooms", &room_id, "rtcs", &rtc_id],
                    "read",
                )
                .await
                .map_err(|err| SvcError::from(err))?
        };

        // Returning Real-Time connection
        let object = {
            let conn = self.db.get()?;
            rtc::FindQuery::new()
                .id(id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the rtc = '{}' is not found", &id))
                        .build()
                })?
        };

        let mut timing = ShortTermTimingProperties::until_now(start_timestamp);
        timing.set_authorization_time(authz_time);
        inreq
            .to_response(object, ResponseStatus::OK, timing, API_VERSION)
            .into()
    }

    pub(crate) async fn list(
        &self,
        inreq: ListRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        let room_id = inreq.payload().room_id;

        // Authorization: room's owner has to allow the action
        let authz_time = {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .time(room::now())
                .id(room_id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?;

            let room_id = room.id().to_string();

            self.authz
                .authorize(
                    room.audience(),
                    inreq.properties(),
                    vec!["rooms", &room_id, "rtcs"],
                    "list",
                )
                .await
                .map_err(|err| SvcError::from(err))?
        };

        // Looking up for Real-Time Connections
        let objects = {
            let conn = self.db.get()?;
            rtc::ListQuery::from((
                Some(room_id),
                inreq.payload().offset,
                Some(std::cmp::min(
                    inreq.payload().limit.unwrap_or_else(|| MAX_LIMIT),
                    MAX_LIMIT,
                )),
            ))
            .execute(&conn)?
        };

        let mut timing = ShortTermTimingProperties::until_now(start_timestamp);
        timing.set_authorization_time(authz_time);

        inreq
            .to_response(objects, ResponseStatus::OK, timing, API_VERSION)
            .into()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Try;

    use diesel::prelude::*;
    use serde_json::{json, Value as JsonValue};
    use svc_agent::{AccountId, AgentId};

    use crate::app::API_VERSION;
    use crate::db::janus_rtc_stream;
    use crate::test_helpers::{
        agent::TestAgent,
        authz::{no_authz, TestAuthz},
        db::TestDb,
        factory::{insert_janus_backend, insert_room, insert_rtc, JanusRtcStream},
        Message, AUDIENCE,
    };
    use crate::util::from_base64;

    use super::*;

    #[derive(Debug, PartialEq, Deserialize)]
    struct RtcResponse {
        id: Uuid,
        room_id: Uuid,
        created_at: i64,
    }

    fn build_state(authz: svc_authz::ClientMap, db: ConnectionPool) -> State {
        let me = TestAgent::new("alpha", "conference", AUDIENCE);
        State::new(authz, db, me.agent_id().to_owned())
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn create_rtc() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Allow user to create rtcs in the room.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = room.id().to_string();
            let object = vec!["rooms", &room_id, "rtcs"];
            authz.allow(agent.account_id(), object, "create");

            // Make rtc.create request.
            let state = build_state(authz.into(), db.connection_pool().clone());
            let payload = json!({"room_id": room.id()});
            let request: CreateRequest = agent.build_request("rtc.create", &payload).unwrap();
            let mut result = state
                .create(request, Utc::now())
                .await
                .into_result()
                .unwrap();

            // Assert response.
            let resp = Message::<RtcResponse>::from_publishable(result.remove(0))
                .expect("Failed to parse message");

            assert_eq!(resp.properties().kind(), "response");
            assert_eq!(resp.payload().room_id, room.id());

            // Assert notification.
            let evt = Message::<RtcResponse>::from_publishable(result.remove(0))
                .expect("Failed to parse message");

            assert_eq!(
                evt.topic(),
                format!(
                    "apps/conference.{}/api/{}/rooms/{}/events",
                    AUDIENCE,
                    API_VERSION,
                    room.id(),
                )
            );

            assert_eq!(evt.properties().kind(), "event");
            assert_eq!(evt.payload().room_id, room.id());

            // Assert rtc presence in the DB.
            let conn = db.connection_pool().get().unwrap();
            let query = crate::schema::rtc::table.find(resp.payload().id);
            assert_eq!(query.execute(&conn).unwrap(), 1);
        });
    }

    #[test]
    fn create_rtc_missing_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make rtc.create request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let state = build_state(no_authz(AUDIENCE), db.connection_pool().clone());
            let payload = json!({ "room_id": Uuid::new_v4() });
            let request: CreateRequest = agent.build_request("rtc.create", &payload).unwrap();
            let result = state.create(request, Utc::now()).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected rtc.create to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn create_rtc_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make rtc.create request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let state = build_state(authz.into(), db.connection_pool().clone());
            let payload = json!({"room_id": room.id()});
            let request: CreateRequest = agent.build_request("rtc.create", &payload).unwrap();
            let result = state.create(request, Utc::now()).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected rtc.create to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn read_rtc() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert an rtc.
            let rtc = db
                .connection_pool()
                .get()
                .map(|conn| insert_rtc(&conn, AUDIENCE))
                .unwrap();

            // Allow user to read the rtc.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = rtc.room_id().to_string();
            let rtc_id = rtc.id().to_string();
            let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc.read request.
            let state = build_state(authz.into(), db.connection_pool().clone());
            let payload = json!({"id": rtc.id()});
            let request: ReadRequest = agent.build_request("rtc.read", &payload).unwrap();
            let mut result = state.read(request, Utc::now()).await.into_result().unwrap();

            // Assert response.
            let resp = Message::<RtcResponse>::from_publishable(result.remove(0))
                .expect("Failed to parse message");

            assert_eq!(resp.payload().id, rtc.id());
        });
    }

    #[test]
    fn read_rtc_missing() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make rtc.read request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "id": Uuid::new_v4() });
            let state = build_state(no_authz(AUDIENCE), db.connection_pool().clone());
            let request: ReadRequest = agent.build_request("rtc.read", &payload).unwrap();
            let result = state.read(request, Utc::now()).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected rtc.read to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn read_rtc_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert an rtc.
            let rtc = db
                .connection_pool()
                .get()
                .map(|conn| insert_rtc(&conn, AUDIENCE))
                .unwrap();

            // Make rtc.read request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "id": rtc.id() });
            let state = build_state(authz.into(), db.connection_pool().clone());
            let request: ReadRequest = agent.build_request("rtc.read", &payload).unwrap();
            let result = state.read(request, Utc::now()).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected rtc.read to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn list_rtcs() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert rtcs.
            let rtc = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let rtc = insert_rtc(&conn, AUDIENCE);
                    let _other_rtc = insert_rtc(&conn, AUDIENCE);
                    rtc
                })
                .unwrap();

            // Allow user to list rtcs in the room.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = rtc.room_id().to_string();
            let object = vec!["rooms", &room_id, "rtcs"];
            authz.allow(agent.account_id(), object, "list");

            // Make rtc.list request.
            let state = build_state(authz.into(), db.connection_pool().clone());
            let payload = json!({"room_id": rtc.room_id()});
            let request: ListRequest = agent.build_request("rtc.list", &payload).unwrap();
            let mut result = state.list(request, Utc::now()).await.into_result().unwrap();

            // Assert response.
            let resp = Message::<Vec<RtcResponse>>::from_publishable(result.remove(0))
                .expect("Failed to parse message");

            assert_eq!(resp.payload().len(), 1);
            assert_eq!(resp.payload().first().unwrap().id, rtc.id());
        });
    }

    #[test]
    fn list_rtcs_missing_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make rtc.list request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let state = build_state(no_authz(AUDIENCE), db.connection_pool().clone());
            let payload = json!({ "room_id": Uuid::new_v4() });
            let request: ListRequest = agent.build_request("rtc.list", &payload).unwrap();
            let result = state.list(request, Utc::now()).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected rtc.list to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn list_rtcs_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert an rtc.
            let rtc = db
                .connection_pool()
                .get()
                .map(|conn| insert_rtc(&conn, AUDIENCE))
                .unwrap();

            // Make rtc.list request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "room_id": rtc.room_id() });
            let state = build_state(authz.into(), db.connection_pool().clone());
            let request: ListRequest = agent.build_request("rtc.list", &payload).unwrap();
            let result = state.list(request, Utc::now()).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected rtc.list to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[derive(Debug, PartialEq, Deserialize)]
    struct RtcConnectResponse {
        janus: String,
        plugin: String,
        session_id: i64,
        transaction: String,
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

    #[test]
    fn connect_to_rtc() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert an rtc and janus backend.
            let (rtc, backend) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    // Insert janus backends.
                    let backend1 = insert_janus_backend(&conn, AUDIENCE);
                    let backend2 = insert_janus_backend(&conn, AUDIENCE);

                    // The first backend has 1 active stream.
                    let stream1 = JanusRtcStream::new(AUDIENCE)
                        .backend(&backend1)
                        .insert(&conn)
                        .unwrap();

                    janus_rtc_stream::start(*stream1.id(), &conn).unwrap();

                    // The second backend has 1 stream that is not started
                    // so it's free and should be selected by the balancer.
                    let _stream2 = JanusRtcStream::new(AUDIENCE)
                        .backend(&backend2)
                        .insert(&conn)
                        .unwrap();

                    let rtc = insert_rtc(&conn, AUDIENCE);
                    (rtc, backend2)
                })
                .unwrap();

            // Allow user to read the rtc.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = rtc.room_id().to_string();
            let rtc_id = rtc.id().to_string();
            let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc.connect request.
            let state = build_state(authz.into(), db.connection_pool().clone());
            let payload = json!({"id": rtc.id()});
            let request: ConnectRequest = agent.build_request("rtc.connect", &payload).unwrap();

            let mut result = state
                .connect(request, Utc::now())
                .await
                .into_result()
                .unwrap();

            // Assert outgoing request to Janus.
            let resp = Message::<RtcConnectResponse>::from_publishable(result.remove(0))
                .expect("Failed to parse message");

            assert_eq!(resp.payload().janus, "attach");
            assert_eq!(resp.payload().plugin, "janus.plugin.conference");
            assert_eq!(resp.payload().session_id, backend.session_id());

            // `transaction` field is base64 encoded JSON. Decode and assert.
            let txn_wrap: JsonValue = from_base64(&resp.payload().transaction).unwrap();
            let txn_value = txn_wrap.get("CreateRtcHandle").unwrap().to_owned();
            let txn: RtcConnectTransaction = serde_json::from_value(txn_value).unwrap();

            assert_eq!(
                txn,
                RtcConnectTransaction {
                    rtc_id: rtc.id().to_string(),
                    session_id: backend.session_id(),
                    reqp: RtcConnectTransactionReqp {
                        method: "rtc.connect".to_string(),
                        agent_id: AgentId::new("web", AccountId::new("user123", AUDIENCE)),
                    }
                }
            )
        });
    }

    #[test]
    fn connect_to_rtc_with_existing_stream() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert an rtc and janus backend.
            let (rtc, backend) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let rtc = insert_rtc(&conn, AUDIENCE);

                    // Insert janus backends.
                    let _backend1 = insert_janus_backend(&conn, AUDIENCE);
                    let backend2 = insert_janus_backend(&conn, AUDIENCE);

                    // The second backend has an active stream already.
                    let stream = JanusRtcStream::new(AUDIENCE)
                        .backend(&backend2)
                        .rtc(&rtc)
                        .insert(&conn)
                        .unwrap();

                    janus_rtc_stream::start(*stream.id(), &conn).unwrap();
                    (rtc, backend2)
                })
                .unwrap();

            // Allow user to read the rtc.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = rtc.room_id().to_string();
            let rtc_id = rtc.id().to_string();
            let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc.connect request.
            let state = build_state(authz.into(), db.connection_pool().clone());
            let payload = json!({"id": rtc.id()});
            let request: ConnectRequest = agent.build_request("rtc.connect", &payload).unwrap();

            let mut result = state
                .connect(request, Utc::now())
                .await
                .into_result()
                .unwrap();

            // Ensure we're balanced to the backend with the stream.
            let resp = Message::<RtcConnectResponse>::from_publishable(result.remove(0))
                .expect("Failed to parse message");

            assert_eq!(resp.payload().session_id, backend.session_id());
        });
    }

    #[test]
    fn connect_to_rtc_missing() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make rtc.connect request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "id": Uuid::new_v4() });
            let state = build_state(no_authz(AUDIENCE), db.connection_pool().clone());
            let request: ConnectRequest = agent.build_request("rtc.connect", &payload).unwrap();
            let result = state.connect(request, Utc::now()).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected rtc.connect to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn connect_to_rtc_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert an rtc.
            let rtc = db
                .connection_pool()
                .get()
                .map(|conn| insert_rtc(&conn, AUDIENCE))
                .unwrap();

            // Make rtc.connect request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "id": rtc.id() });
            let state = build_state(authz.into(), db.connection_pool().clone());
            let request: ConnectRequest = agent.build_request("rtc.connect", &payload).unwrap();
            let result = state.connect(request, Utc::now()).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected rtc.connect to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }
}
