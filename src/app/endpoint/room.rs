use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use std::ops::Bound;
use svc_agent::mqtt::{
    IncomingRequest, OutgoingRequest, ResponseStatus, ShortTermTimingProperties,
};
use svc_agent::{Addressable, AgentId};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
use crate::app::endpoint::shared;
use crate::db::{agent, room, ConnectionPool};

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
    pub(crate) async fn create(
        &self,
        inreq: CreateRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        // Authorization: future room's owner has to allow the action
        let authz_time = self
            .authz
            .authorize(
                &inreq.payload().audience,
                inreq.properties(),
                vec!["rooms"],
                "create",
            )
            .await
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

        shared::respond(
            &inreq,
            object,
            Some(("room.create", "rooms")),
            start_timestamp,
            authz_time,
        )
    }

    pub(crate) async fn read(
        &self,
        inreq: ReadRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
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
        let authz_time = self
            .authz
            .authorize(
                object.audience(),
                inreq.properties(),
                vec!["rooms", &room_id],
                "read",
            )
            .await
            .map_err(|err| SvcError::from(err))?;

        shared::respond(&inreq, object, None, start_timestamp, authz_time)
    }

    pub(crate) async fn update(
        &self,
        inreq: UpdateRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
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
        let authz_time = self
            .authz
            .authorize(
                object.audience(),
                inreq.properties(),
                vec!["rooms", &room_id],
                "update",
            )
            .await
            .map_err(|err| SvcError::from(err))?;

        let object = {
            let conn = self.db.get()?;
            inreq.payload().execute(&conn)?
        };

        shared::respond(
            &inreq,
            object,
            Some(("room.update", &format!("rooms/{}/events", room_id))),
            start_timestamp,
            authz_time,
        )
    }

    pub(crate) async fn delete(
        &self,
        inreq: DeleteRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
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
        let authz_time = self
            .authz
            .authorize(
                object.audience(),
                inreq.properties(),
                vec!["rooms", &room_id],
                "delete",
            )
            .await
            .map_err(|err| SvcError::from(err))?;

        {
            let conn = self.db.get()?;
            room::DeleteQuery::new(inreq.payload().id).execute(&conn)?
        };

        shared::respond(
            &inreq,
            object,
            Some(("room.delete", "rooms")),
            start_timestamp,
            authz_time,
        )
    }

    pub(crate) async fn enter(
        &self,
        inreq: EnterRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        let room_id = inreq.payload().id.to_string();
        let conn = self.db.get()?;

        let object = room::FindQuery::new()
            .time(room::now())
            .id(inreq.payload().id)
            .execute(&conn)?
            .ok_or_else(|| {
                SvcError::builder()
                    .status(ResponseStatus::NOT_FOUND)
                    .detail(&format!("the room = '{}' is not found", &room_id))
                    .build()
            })?;

        // Authorization: room's owner has to allow the action
        let authz_time = self
            .authz
            .authorize(
                object.audience(),
                inreq.properties(),
                vec!["rooms", &room_id, "events"],
                "subscribe",
            )
            .await
            .map_err(|err| SvcError::from(err))?;

        agent::InsertQuery::new(inreq.properties().as_agent_id(), object.id()).execute(&conn)?;

        let payload = SubscriptionRequest::new(
            inreq.properties().as_agent_id().to_owned(),
            vec!["rooms", &room_id, "events"],
        );

        let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
        short_term_timing.set_authorization_time(authz_time);

        let props = inreq.properties().to_request(
            "subscription.create",
            inreq.properties().response_topic(),
            inreq.properties().correlation_data(),
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
        OutgoingRequest::unicast(payload, props, inreq.properties()).into()
    }

    pub(crate) async fn leave(
        &self,
        inreq: LeaveRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
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

            shared::check_room_presence(&room, &inreq.properties().as_agent_id(), &conn)?;
        }

        let payload = SubscriptionRequest::new(
            inreq.properties().as_agent_id().to_owned(),
            vec!["rooms", &room_id, "events"],
        );

        let props = inreq.properties().to_request(
            "subscription.delete",
            inreq.properties().response_topic(),
            inreq.properties().correlation_data(),
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
        OutgoingRequest::unicast(payload, props, inreq.properties()).into()
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
            let mut result = state
                .create(request, Utc::now())
                .await
                .into_result()
                .unwrap();

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
            let result = state.create(request, Utc::now()).await.into_result();

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
            let mut result = state.read(request, Utc::now()).await.into_result().unwrap();
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
            let result = state.read(request, Utc::now()).await.into_result();

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
            let result = state.read(request, Utc::now()).await.into_result();

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
            let mut result = state
                .update(request, Utc::now())
                .await
                .into_result()
                .unwrap();

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
            let result = state.update(request, Utc::now()).await.into_result();

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
            let result = state.update(request, Utc::now()).await.into_result();

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
            let mut result = state
                .delete(request, Utc::now())
                .await
                .into_result()
                .unwrap();

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
            let result = state.delete(request, Utc::now()).await.into_result();

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
            let result = state.delete(request, Utc::now()).await.into_result();

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
            let mut result = state
                .enter(request, Utc::now())
                .await
                .into_result()
                .unwrap();
            let message = result.remove(0);

            // Assert outgoing broker request.
            match message.destination() {
                &Destination::Unicast(ref agent_id) => {
                    assert_eq!(agent_id.label(), "web");
                    assert_eq!(agent_id.as_account_id().label(), "user123");
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
            assert_eq!(resp.subject, agent.agent_id().to_string());
            assert_eq!(
                resp.object,
                vec!["rooms", room.id().to_string().as_str(), "events"]
            );

            // Assert agent presence in the DB.
            use crate::db::agent::{Object, Status};
            use crate::schema::agent as agent_schema;

            let conn = db.connection_pool().get().unwrap();

            let agent_object = agent_schema::table
                .filter(agent_schema::agent_id.eq(agent.agent_id()))
                .filter(agent_schema::room_id.eq(room.id()))
                .get_result::<Object>(&conn)
                .expect("Agent not found in the DB");

            assert_eq!(*agent_object.status(), Status::InProgress);
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
            let result = state.enter(request, Utc::now()).await.into_result();

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
            let result = state.enter(request, Utc::now()).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected room.enter to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    #[test]
    fn enter_room_twice() {
        use crate::db::agent::{Object, Status};
        use crate::schema::agent as agent_schema;

        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);
            let agent = TestAgent::new("web", "user123", AUDIENCE);

            // Insert a room and an agent in `ready` status.
            let room = db
                .connection_pool()
                .get()
                .map_err(|err| format_err!("Failed to get DB connection: {}", err))
                .and_then(|conn| {
                    let room = insert_room(&conn, AUDIENCE);

                    factory::Agent::new()
                        .audience(AUDIENCE)
                        .room_id(room.id())
                        .agent_id(agent.agent_id())
                        .status(Status::Ready)
                        .insert(&conn)?;

                    Ok(room)
                })
                .unwrap();

            // Allow the agent to enter the room.
            let room_id = room.id().to_string();
            let object = vec!["rooms", &room_id, "events"];
            authz.allow(agent.account_id(), object, "subscribe");

            // Make room.enter request.
            let payload = json!({"id": room.id()});
            let state = State::new(authz.into(), db.connection_pool().clone());
            let request: EnterRequest = agent.build_request("room.enter", &payload).unwrap();
            state
                .enter(request, Utc::now())
                .await
                .into_result()
                .unwrap();

            // Assert agent is in `in_progress` state in the DB.
            let conn = db.connection_pool().get().unwrap();

            let agent_object = agent_schema::table
                .filter(agent_schema::agent_id.eq(agent.agent_id()))
                .filter(agent_schema::room_id.eq(room.id()))
                .get_result::<Object>(&conn)
                .expect("Agent not found in the DB");

            assert_eq!(*agent_object.status(), Status::InProgress);
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
            let mut result = state
                .leave(request, Utc::now())
                .await
                .into_result()
                .unwrap();
            let message = result.remove(0);

            // Assert outgoing broker request.
            match message.destination() {
                &Destination::Unicast(ref agent_id) => {
                    assert_eq!(agent_id.label(), "web");
                    assert_eq!(agent_id.as_account_id().label(), "user123");
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
            assert_eq!(resp.subject, agent.agent_id().to_string());
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
            let result = state.leave(request, Utc::now()).await.into_result();

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
            let result = state.leave(request, Utc::now()).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected room.leave to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }
}
