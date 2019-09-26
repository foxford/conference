use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use std::ops::Bound;
use svc_agent::mqtt::{
    Connection, IncomingRequest, OutgoingRequest, OutgoingRequestProperties, ResponseStatus,
};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
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
    broker_account_id: svc_agent::AccountId,
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(
        broker_account_id: svc_agent::AccountId,
        authz: svc_authz::ClientMap,
        db: ConnectionPool,
    ) -> Self {
        Self {
            broker_account_id,
            authz,
            db,
        }
    }
}

impl State {
    pub(crate) async fn create(&self, inreq: CreateRequest) -> endpoint::Result {
        // Authorization: future room's owner has to allow the action
        endpoint::authorize(
            &self.authz,
            &inreq.payload().audience,
            inreq.properties(),
            vec!["rooms"],
            "create",
        )
        .await?;

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

        inreq.to_response(object, ResponseStatus::OK).into()
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
        endpoint::authorize(
            &self.authz,
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id],
            "read",
        )
        .await?;

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
        endpoint::authorize(
            &self.authz,
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id],
            "update",
        )
        .await?;

        let object = {
            let conn = self.db.get()?;
            inreq.payload().execute(&conn)?
        };

        inreq.to_response(object, ResponseStatus::OK).into()
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
        endpoint::authorize(
            &self.authz,
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id],
            "delete",
        )
        .await?;

        {
            let conn = self.db.get()?;
            room::DeleteQuery::new(inreq.payload().id).execute(&conn)?
        };

        inreq.to_response(object, ResponseStatus::OK).into()
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
        endpoint::authorize(
            &self.authz,
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id, "events"],
            "subscribe",
        )
        .await?;

        let payload = SubscriptionRequest::new(
            inreq.properties().to_connection(),
            vec!["rooms", &room_id, "events"],
        );

        let props = OutgoingRequestProperties::new(
            "subscription.create",
            inreq.properties().response_topic(),
            inreq.properties().correlation_data(),
        );

        OutgoingRequest::multicast(payload, props, &self.broker_account_id).into()
    }

    pub(crate) async fn leave(&self, inreq: LeaveRequest) -> endpoint::Result {
        let room_id = inreq.payload().id.to_string();

        let _object = {
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

        let payload = SubscriptionRequest::new(
            inreq.properties().to_connection(),
            vec!["rooms", &room_id, "events"],
        );

        let props = OutgoingRequestProperties::new(
            "subscription.delete",
            inreq.properties().response_topic(),
            inreq.properties().correlation_data(),
        );

        OutgoingRequest::multicast(payload, props, &self.broker_account_id).into()
    }
}

#[cfg(test)]
mod test {
    use std::ops::{Bound, Try};

    use chrono::Utc;
    use diesel::prelude::*;
    use serde_json::{json, Value as JsonValue};

    use crate::test_helpers::{
        agent::TestAgent, authz::no_authz, db::TestDb, extract_payload, factory::insert_room,
        parse_payload,
    };

    use super::*;

    const AUDIENCE: &str = "dev.svc.example.org";

    fn build_state(db: &TestDb) -> State {
        let account_id = svc_agent::AccountId::new("mqtt-gateway", AUDIENCE);

        State::new(account_id, no_authz(AUDIENCE), db.connection_pool().clone())
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct RoomResponse {
        id: Uuid,
        time: Vec<Option<i64>>,
        audience: String,
        created_at: i64,
        backend: String,
    }

    #[test]
    fn create_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let state = build_state(&db);

            // Make room.create request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let now = Utc::now().timestamp();

            let payload = json!({
                "time": vec![Some(now), None],
                "audience": AUDIENCE,
                "backend": "janus",
            });

            let request: CreateRequest = agent.build_request("room.create", &payload).unwrap();
            let mut result = state.create(request).await.into_result().unwrap();
            let message = result.remove(0);

            // Assert response.
            let resp: RoomResponse = extract_payload(message).unwrap();
            assert_eq!(resp.time, vec![Some(now), None]);
            assert_eq!(resp.audience, AUDIENCE);
            assert!(resp.created_at >= now);
            assert_eq!(resp.backend, "janus");

            // Assert room presence in the DB.
            let conn = db.connection_pool().get().unwrap();
            let query = crate::schema::room::table.find(resp.id);
            assert_eq!(query.execute(&conn).unwrap(), 1);
        });
    }

    #[test]
    fn read_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make room.read request.
            let state = build_state(&db);
            let agent = TestAgent::new("web", "user123", AUDIENCE);
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
    fn update_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make room.update request.
            let state = build_state(&db);
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let now = Utc::now().timestamp();

            let payload = json!({
                "id": room.id(),
                "time": vec![Some(now), None],
                "audience": "dev.svc.example.net",
                "backend": "none",
            });

            let request: UpdateRequest = agent.build_request("room.update", &payload).unwrap();
            let mut result = state.update(request).await.into_result().unwrap();
            let message = result.remove(0);

            // Assert response.
            let resp: RoomResponse = extract_payload(message).unwrap();

            assert_eq!(
                resp,
                RoomResponse {
                    id: room.id().to_owned(),
                    time: vec![Some(now), None],
                    audience: "dev.svc.example.net".to_string(),
                    created_at: room.created_at().timestamp(),
                    backend: "none".to_string(),
                }
            );
        });
    }

    #[test]
    fn delete_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make room.delete request.
            let state = build_state(&db);
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({"id": room.id()});
            let request: DeleteRequest = agent.build_request("room.delete", &payload).unwrap();
            state.delete(request).await.into_result().unwrap();

            // Assert room abscence in the DB.
            let conn = db.connection_pool().get().unwrap();
            let query = crate::schema::room::table.find(room.id());
            assert_eq!(query.execute(&conn).unwrap(), 0);
        });
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct RoomEnterLeaveBrokerRequest {
        subject: String,
        object: Vec<String>,
    }

    #[test]
    fn enter_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", AUDIENCE);

            // Insert a room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make room.enter request.
            let state = build_state(&db);
            let payload = json!({"id": room.id()});
            let request: EnterRequest = agent.build_request("room.enter", &payload).unwrap();
            let mut result = state.enter(request).await.into_result().unwrap();
            let message = result.remove(0);

            // Assert outgoing broker request.
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
    fn leave_room() {
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
            let state = build_state(&db);
            let payload = json!({"id": room.id()});
            let request: LeaveRequest = agent.build_request("room.leave", &payload).unwrap();
            let mut result = state.leave(request).await.into_result().unwrap();
            let message = result.remove(0);

            // Assert outgoing broker request.
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
}
