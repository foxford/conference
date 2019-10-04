use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use std::ops::Bound;
use svc_agent::mqtt::{
    Connection, IncomingRequest, OutgoingRequest, OutgoingRequestProperties, ResponseStatus,
};
use svc_agent::Addressable;
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
use crate::app::endpoint::shared::check_room_presence;
use crate::db::{room, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
    time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    audience: String,
    #[serde(default = "CreateRequestData::default_backend")]
    backend: room::RoomBackend,
}

impl CreateRequestData {
    fn default_backend() -> room::RoomBackend {
        room::RoomBackend::None
    }
}

pub(crate) type ReadRequest = IncomingRequest<ReadRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ReadRequestData {
    id: Uuid,
}

pub(crate) type DeleteRequest = ReadRequest;

pub(crate) type UpdateRequest = IncomingRequest<room::UpdateQuery>;

pub(crate) type LeaveRequest = IncomingRequest<LeaveRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct LeaveRequestData {
    id: Uuid,
}

pub(crate) type EnterRequest = IncomingRequest<EnterRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct EnterRequestData {
    id: Uuid,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
struct SubscriptionRequest {
    subject: Connection,
    object: Vec<String>,
}

impl SubscriptionRequest {
    fn new(subject: Connection, object: Vec<&str>) -> Self {
        Self {
            subject: subject,
            object: object.iter().map(|&s| s.into()).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub(crate) struct State {
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(authz: svc_authz::ClientMap, db: ConnectionPool) -> Self {
        Self { authz, db }
    }
}

impl State {
    pub(crate) async fn create(&self, inreq: CreateRequest) -> endpoint::Result {
        // Authorization: future room's owner has to allow the action
        self.authz
            .authorize(
                &inreq.payload().audience,
                inreq.properties(),
                vec!["rooms"],
                "create",
            )
            .map_err(|err| SvcError::from(err))?;

        // Creating a Room
        let object = {
            let conn = self.db.get()?;
            room::InsertQuery::new(
                inreq.payload().time,
                &inreq.payload().audience,
                inreq.payload().backend,
            )
            .execute(&conn)?
        };

        endpoint::respond_and_notify(&inreq, object, "room.create", "rooms")
    }

    pub(crate) async fn read(&self, inreq: ReadRequest) -> endpoint::Result {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .id(inreq.payload().id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?
        };

        // Authorization: room's owner has to allow the action
        self.authz
            .authorize(
                object.audience(),
                inreq.properties(),
                vec!["rooms", &room_id],
                "read",
            )
            .map_err(|err| SvcError::from(err))?;

        inreq.to_response(object, ResponseStatus::OK).into()
    }

    pub(crate) async fn update(&self, inreq: UpdateRequest) -> endpoint::Result {
        let room_id = inreq.payload().id().to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .time(room::since_now())
                .id(inreq.payload().id())
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?
        };

        // Authorization: room's owner has to allow the action
        self.authz
            .authorize(
                object.audience(),
                inreq.properties(),
                vec!["rooms", &room_id],
                "update",
            )
            .map_err(|err| SvcError::from(err))?;

        let object = {
            let conn = self.db.get()?;
            inreq.payload().execute(&conn)?
        };

        endpoint::respond_and_notify(
            &inreq,
            object,
            "room.update",
            &format!("rooms/{}/events", room_id),
        )
    }

    pub(crate) async fn delete(&self, inreq: DeleteRequest) -> endpoint::Result {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .time(room::since_now())
                .id(inreq.payload().id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?
        };

        // Authorization: room's owner has to allow the action
        self.authz
            .authorize(
                object.audience(),
                inreq.properties(),
                vec!["rooms", &room_id],
                "delete",
            )
            .map_err(|err| SvcError::from(err))?;

        {
            let conn = self.db.get()?;
            room::DeleteQuery::new(inreq.payload().id).execute(&conn)?
        };

        endpoint::respond_and_notify(&inreq, object, "room.delete", "rooms")
    }

    pub(crate) async fn enter(&self, inreq: EnterRequest) -> endpoint::Result {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .time(room::now())
                .id(inreq.payload().id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?
        };

        // Authorization: room's owner has to allow the action
        self.authz
            .authorize(
                object.audience(),
                inreq.properties(),
                vec!["rooms", &room_id, "events"],
                "subscribe",
            )
            .map_err(|err| SvcError::from(err))?;

        let payload = SubscriptionRequest::new(
            inreq.properties().to_connection(),
            vec!["rooms", &room_id, "events"],
        );

        let props = OutgoingRequestProperties::new(
            "subscription.create",
            inreq.properties().response_topic(),
            inreq.properties().correlation_data(),
        );

        OutgoingRequest::unicast(payload, props, inreq.properties().broker()).into()
    }

    pub(crate) async fn leave(&self, inreq: LeaveRequest) -> endpoint::Result {
        let room_id = inreq.payload().id.to_string();

        {
            let conn = self.db.get()?;

            let room = room::FindQuery::new()
                .time(room::since_now())
                .id(inreq.payload().id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?;

            check_room_presence(&room, &inreq.properties().as_agent_id(), &conn)?;
        }

        let payload = SubscriptionRequest::new(
            inreq.properties().to_connection(),
            vec!["rooms", &room_id, "events"],
        );

        let props = OutgoingRequestProperties::new(
            "subscription.delete",
            inreq.properties().response_topic(),
            inreq.properties().correlation_data(),
        );

        OutgoingRequest::unicast(payload, props, inreq.properties().broker()).into()
    }
}

#[cfg(test)]
mod test {
    use std::ops::{Bound, Try};

    use chrono::Utc;
    use diesel::prelude::*;
    use failure::format_err;
    use serde_json::{json, Value as JsonValue};
    use svc_agent::Destination;
    use svc_authn::Authenticable;

    use crate::test_helpers::{
        agent::TestAgent,
        authz::{no_authz, TestAuthz},
        db::TestDb,
        extract_payload,
        factory::{self, insert_room},
        parse_payload,
    };

    use super::*;

    const AUDIENCE: &str = "dev.svc.example.org";

    #[derive(Debug, PartialEq, Deserialize)]
    struct RoomResponse {
        id: Uuid,
        time: Vec<Option<i64>>,
        audience: String,
        created_at: i64,
        backend: String,
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn create_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Allow user to create rooms.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            authz.allow(agent.account_id(), vec!["rooms"], "create");

            // Make room.create request.
            let now = Utc::now().timestamp();

            let payload = json!({
                "time": vec![Some(now), None],
                "audience": AUDIENCE,
                "backend": "janus",
            });

            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let request: CreateRequest = agent.build_request("room.create", &payload).unwrap();
            let mut result = state.create(request).await.into_result().unwrap();

            // Assert response.
            let message = result.remove(0);
            assert_eq!(message.message_type(), "response");

            let resp: RoomResponse = extract_payload(message).unwrap();
            assert_eq!(resp.time, vec![Some(now), None]);
            assert_eq!(resp.audience, AUDIENCE);
            assert!(resp.created_at >= now);
            assert_eq!(resp.backend, "janus");

            // Assert notification.
            let message = result.remove(0);
            assert_eq!(message.message_type(), "event");

            match message.destination() {
                Destination::Broadcast(destination) => assert_eq!(destination, "rooms"),
                _ => panic!("Expected broadcast destination"),
            }

            let payload: RoomResponse = extract_payload(message).unwrap();
            assert_eq!(payload.time, vec![Some(now), None]);
            assert_eq!(payload.audience, AUDIENCE);
            assert!(payload.created_at >= now);
            assert_eq!(payload.backend, "janus");

            // Assert room presence in the DB.
            let conn = db.connection_pool().get().unwrap();
            let query = crate::schema::room::table.find(resp.id);
            assert_eq!(query.execute(&conn).unwrap(), 1);
        });
    }

    #[test]
    fn create_room_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Make room.create request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let now = Utc::now().timestamp();

            let payload = json!({
                "time": vec![Some(now), None],
                "audience": AUDIENCE,
                "backend": "janus",
            });

            let state = State::new(authz.into(), db.connection_pool().clone());
            let request: CreateRequest = agent.build_request("room.create", &payload).unwrap();
            let result = state.create(request).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected room.create to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn read_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Allow user to read the room.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = room.id().to_string();
            authz.allow(agent.account_id(), vec!["rooms", &room_id], "read");

            // Make room.read request.
            let state = State::new(authz.into(), db.connection_pool().clone());
            let payload = json!({"id": room.id()});
            let request: ReadRequest = agent.build_request("room.read", &payload).unwrap();
            let mut result = state.read(request).await.into_result().unwrap();
            let message = result.remove(0);

            // Assert response.
            let resp: RoomResponse = extract_payload(message).unwrap();

            let start = match room.time().0 {
                Bound::Included(val) => val,
                _ => panic!("Bad room time"),
            };

            assert_eq!(
                resp,
                RoomResponse {
                    id: room.id().to_owned(),
                    time: vec![Some(start.timestamp()), None],
                    audience: AUDIENCE.to_string(),
                    created_at: room.created_at().timestamp(),
                    backend: "janus".to_string(),
                }
            );
        });
    }

    #[test]
    fn read_room_missing() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make room.read request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "id": Uuid::new_v4() });
            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let request: ReadRequest = agent.build_request("room.read", &payload).unwrap();
            let result = state.read(request).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected room.read to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn read_room_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make room.read request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "id": room.id() });
            let state = State::new(authz.into(), db.connection_pool().clone());
            let request: ReadRequest = agent.build_request("room.read", &payload).unwrap();
            let result = state.read(request).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected room.read to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn update_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Allow user to update the room.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = room.id().to_string();
            authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

            // Make room.update request.
            let now = Utc::now().timestamp();

            let payload = json!({
                "id": room.id(),
                "time": vec![Some(now), None],
                "audience": "dev.svc.example.net",
                "backend": "none",
            });

            let state = State::new(authz.into(), db.connection_pool().clone());
            let request: UpdateRequest = agent.build_request("room.update", &payload).unwrap();
            let mut result = state.update(request).await.into_result().unwrap();

            // Assert response.
            let message = result.remove(0);
            assert_eq!(message.message_type(), "response");

            let resp: RoomResponse = extract_payload(message).unwrap();

            let expected_payload = RoomResponse {
                id: room.id().to_owned(),
                time: vec![Some(now), None],
                audience: "dev.svc.example.net".to_string(),
                created_at: room.created_at().timestamp(),
                backend: "none".to_string(),
            };

            assert_eq!(resp, expected_payload);

            // Assert notification.
            let message = result.remove(0);
            assert_eq!(message.message_type(), "event");

            match message.destination() {
                Destination::Broadcast(destination) => {
                    assert_eq!(destination, &format!("rooms/{}/events", room.id()))
                }
                _ => panic!("Expected broadcast destination"),
            }

            let payload: RoomResponse = extract_payload(message).unwrap();
            assert_eq!(payload, expected_payload);
        });
    }

    #[test]
    fn update_room_missing() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make room.update request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);

            let payload = json!({
                "id":  Uuid::new_v4(),
                "time": vec![Some(Utc::now().timestamp()), None],
                "audience": "dev.svc.example.net",
                "backend": "none",
            });

            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let request: UpdateRequest = agent.build_request("room.update", &payload).unwrap();
            let result = state.update(request).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected room.update to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn update_room_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make room.update request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);

            let payload = json!({
                "id": room.id(),
                "time": vec![Some(Utc::now().timestamp()), None],
                "audience": "dev.svc.example.net",
                "backend": "none",
            });

            let state = State::new(authz.into(), db.connection_pool().clone());
            let request: UpdateRequest = agent.build_request("room.update", &payload).unwrap();
            let result = state.update(request).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected room.update to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn delete_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Allow user to delete the room.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = room.id().to_string();
            authz.allow(agent.account_id(), vec!["rooms", &room_id], "delete");

            // Make room.delete request.
            let state = State::new(authz.into(), db.connection_pool().clone());
            let payload = json!({"id": room.id()});
            let request: DeleteRequest = agent.build_request("room.delete", &payload).unwrap();
            let mut result = state.delete(request).await.into_result().unwrap();

            // Assert response.
            let message = result.remove(0);
            assert_eq!(message.message_type(), "response");

            let payload: RoomResponse = extract_payload(message).unwrap();
            assert_eq!(payload.id, room.id());

            // Assert notification.
            let message = result.remove(0);
            assert_eq!(message.message_type(), "event");

            match message.destination() {
                Destination::Broadcast(destination) => assert_eq!(destination, "rooms"),
                _ => panic!("Expected broadcast destination"),
            }

            let payload: RoomResponse = extract_payload(message).unwrap();
            assert_eq!(payload.id, room.id());

            // Assert room abscence in the DB.
            let conn = db.connection_pool().get().unwrap();
            let query = crate::schema::room::table.find(room.id());
            assert_eq!(query.execute(&conn).unwrap(), 0);
        });
    }

    #[test]
    fn delete_room_missing() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make room.delete request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "id": Uuid::new_v4() });
            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let request: DeleteRequest = agent.build_request("room.delete", &payload).unwrap();
            let result = state.delete(request).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected room.delete to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn delete_room_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make room.delete request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({"id": room.id()});
            let state = State::new(authz.into(), db.connection_pool().clone());
            let request: DeleteRequest = agent.build_request("room.delete", &payload).unwrap();
            let result = state.delete(request).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected room.delete to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[derive(Debug, PartialEq, Deserialize)]
    struct RoomEnterLeaveBrokerRequest {
        subject: String,
        object: Vec<String>,
    }

    #[test]
    fn enter_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Allow user to subscribe to room's events
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = room.id().to_string();
            let object = vec!["rooms", &room_id, "events"];
            authz.allow(agent.account_id(), object, "subscribe");

            // Make room.enter request.
            let state = State::new(authz.into(), db.connection_pool().clone());
            let payload = json!({"id": room.id()});
            let request: EnterRequest = agent.build_request("room.enter", &payload).unwrap();
            let mut result = state.enter(request).await.into_result().unwrap();
            let message = result.remove(0);

            // Assert outgoing broker request.
            match message.destination() {
                &Destination::Unicast(ref agent_id) => {
                    assert_eq!(agent_id.label(), "alpha");
                    assert_eq!(agent_id.as_account_id().label(), "mqtt-gateway");
                    assert_eq!(agent_id.as_account_id().audience(), AUDIENCE);
                }
                _ => panic!("Expected unicast destination"),
            }

            let message_bytes = message.into_bytes().unwrap();

            let message_value =
                serde_json::from_slice::<JsonValue>(message_bytes.as_bytes()).unwrap();

            let properties = message_value.get("properties").unwrap();
            let method = properties.get("method").unwrap().as_str().unwrap();
            assert_eq!(method, "subscription.create");

            let resp: RoomEnterLeaveBrokerRequest = parse_payload(message_value).unwrap();
            assert_eq!(resp.subject, format!("v1/agents/{}", agent.agent_id()));
            assert_eq!(
                resp.object,
                vec!["rooms", room.id().to_string().as_str(), "events"]
            );
        });
    }

    #[test]
    fn enter_room_missing() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make room.enter request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "id": Uuid::new_v4() });
            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let request: EnterRequest = agent.build_request("room.enter", &payload).unwrap();
            let result = state.enter(request).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected room.enter to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn enter_room_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make room.enter request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({"id": room.id()});
            let state = State::new(authz.into(), db.connection_pool().clone());
            let request: EnterRequest = agent.build_request("room.enter", &payload).unwrap();
            let result = state.enter(request).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected room.enter to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn leave_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", AUDIENCE);

            // Insert a room with online agent.
            let room = db
                .connection_pool()
                .get()
                .map_err(|err| format_err!("Failed to get DB connection: {}", err))
                .and_then(|conn| {
                    let room = insert_room(&conn, AUDIENCE);

                    let agent_factory = factory::Agent::new().room_id(room.id());
                    agent_factory.agent_id(agent.agent_id()).insert(&conn)?;

                    Ok(room)
                })
                .unwrap();

            // Make room.leave request.
            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let payload = json!({"id": room.id()});
            let request: LeaveRequest = agent.build_request("room.leave", &payload).unwrap();
            let mut result = state.leave(request).await.into_result().unwrap();
            let message = result.remove(0);

            // Assert outgoing broker request.
            match message.destination() {
                &Destination::Unicast(ref agent_id) => {
                    assert_eq!(agent_id.label(), "alpha");
                    assert_eq!(agent_id.as_account_id().label(), "mqtt-gateway");
                    assert_eq!(agent_id.as_account_id().audience(), AUDIENCE);
                }
                _ => panic!("Expected unicast destination"),
            }

            let message_bytes = message.into_bytes().unwrap();

            let message_value =
                serde_json::from_slice::<JsonValue>(message_bytes.as_bytes()).unwrap();

            let properties = message_value.get("properties").unwrap();
            let method = properties.get("method").unwrap().as_str().unwrap();
            assert_eq!(method, "subscription.delete");

            let resp: RoomEnterLeaveBrokerRequest = parse_payload(message_value).unwrap();
            assert_eq!(resp.subject, format!("v1/agents/{}", agent.agent_id()));
            assert_eq!(
                resp.object,
                vec!["rooms", room.id().to_string().as_str(), "events"]
            );
        });
    }

    #[test]
    fn leave_room_missing() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make room.leave request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "id": Uuid::new_v4() });
            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let request: LeaveRequest = agent.build_request("room.leave", &payload).unwrap();
            let result = state.leave(request).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected room.leave to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn leave_room_when_not_in_the_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make room.leave request.
            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let payload = json!({"id": room.id()});
            let request: LeaveRequest = agent.build_request("room.leave", &payload).unwrap();
            let result = state.leave(request).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected room.leave to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }
}
