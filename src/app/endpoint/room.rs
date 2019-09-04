use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use std::ops::Bound;
use svc_agent::mqtt::{
    Connection, IncomingRequest, OutgoingRequest, OutgoingRequestProperties, Publishable,
    ResponseStatus,
};
use svc_error::Error as SvcError;
use uuid::Uuid;

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
    pub(crate) async fn create(
        &self,
        inreq: CreateRequest,
    ) -> Result<Vec<Box<dyn Publishable>>, SvcError> {
        // Authorization: future room's owner has to allow the action
        self.authz.authorize(
            &inreq.payload().audience,
            inreq.properties(),
            vec!["rooms"],
            "create",
        )?;

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

        let message = inreq.to_response(object, ResponseStatus::OK);
        Ok(vec![Box::new(message) as Box<dyn Publishable>])
    }

    pub(crate) async fn read(
        &self,
        inreq: ReadRequest,
    ) -> Result<Vec<Box<dyn Publishable>>, SvcError> {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .time(room::upto_now())
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
        self.authz.authorize(
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id],
            "read",
        )?;

        let message = inreq.to_response(object, ResponseStatus::OK);
        Ok(vec![Box::new(message) as Box<dyn Publishable>])
    }

    pub(crate) async fn update(
        &self,
        inreq: UpdateRequest,
    ) -> Result<Vec<Box<dyn Publishable>>, SvcError> {
        let room_id = inreq.payload().id().to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .time(room::upto_now())
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
        self.authz.authorize(
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id],
            "update",
        )?;

        let object = {
            let conn = self.db.get()?;
            inreq.payload().execute(&conn)?
        };

        let message = inreq.to_response(object, ResponseStatus::OK);
        Ok(vec![Box::new(message) as Box<dyn Publishable>])
    }

    pub(crate) async fn delete(
        &self,
        inreq: DeleteRequest,
    ) -> Result<Vec<Box<dyn Publishable>>, SvcError> {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .time(room::upto_now())
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
        self.authz.authorize(
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id],
            "delete",
        )?;

        let _ = {
            let conn = self.db.get()?;
            room::DeleteQuery::new(inreq.payload().id).execute(&conn)?
        };

        let message = inreq.to_response(object, ResponseStatus::OK);
        Ok(vec![Box::new(message) as Box<dyn Publishable>])
    }

    pub(crate) async fn enter(
        &self,
        inreq: EnterRequest,
    ) -> Result<Vec<Box<dyn Publishable>>, SvcError> {
        let room_id = inreq.payload().id.to_string();

        let object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .time(room::upto_now())
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
        self.authz.authorize(
            object.audience(),
            inreq.properties(),
            vec!["rooms", &room_id, "events"],
            "subscribe",
        )?;

        let brokerreq = {
            let payload = SubscriptionRequest::new(
                inreq.properties().to_connection(),
                vec!["rooms", &room_id, "events"],
            );
            let props = OutgoingRequestProperties::new(
                "subscription.create",
                inreq.properties().response_topic(),
                inreq.properties().correlation_data(),
            );
            OutgoingRequest::multicast(payload, props, &self.broker_account_id)
        };

        Ok(vec![Box::new(brokerreq) as Box<dyn Publishable>])
    }

    pub(crate) async fn leave(
        &self,
        inreq: LeaveRequest,
    ) -> Result<Vec<Box<dyn Publishable>>, SvcError> {
        let room_id = inreq.payload().id.to_string();

        let _object = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .time(room::upto_now())
                .id(inreq.payload().id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?
        };

        let brokerreq = {
            let payload = SubscriptionRequest::new(
                inreq.properties().to_connection(),
                vec!["rooms", &room_id, "events"],
            );
            let props = OutgoingRequestProperties::new(
                "subscription.delete",
                inreq.properties().response_topic(),
                inreq.properties().correlation_data(),
            );
            OutgoingRequest::multicast(payload, props, &self.broker_account_id)
        };

        Ok(vec![Box::new(brokerreq) as Box<dyn Publishable>])
    }
}

#[cfg(test)]
mod test {
    use std::ops::Bound;

    use chrono::Utc;
    use diesel::pg::PgConnection;
    use diesel::prelude::*;
    use serde_json::{json, Value as JsonValue};
    use svc_agent::mqtt::Publishable;

    use crate::db::room::{InsertQuery, Object as Room, RoomBackend};

    use crate::test_helpers::{
        build_authz, extract_payload, extract_properties, test_agent::TestAgent, test_db::TestDb,
    };

    use super::*;

    const AUDIENCE: &str = "dev.svc.example.org";

    fn build_state(db: &TestDb) -> State {
        let account_id = svc_agent::AccountId::new("mqtt-gateway", AUDIENCE);

        State::new(
            account_id,
            build_authz(AUDIENCE),
            db.connection_pool().clone(),
        )
    }

    fn insert_room(conn: &PgConnection) -> Room {
        let time = (Bound::Included(Utc::now()), Bound::Unbounded);

        InsertQuery::new(time, AUDIENCE, RoomBackend::Janus)
            .execute(conn)
            .expect("Failed to insert room")
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
            let result = await!(state.create(request)).unwrap();
            let outgoing_envelope = result.first().unwrap();
            let resp = extract_payload(outgoing_envelope).expect("Failed to extract payload");

            // Assert response.
            assert!(resp.get("id").is_some());
            assert_eq!(resp.get("time").unwrap(), payload.get("time").unwrap());
            assert_eq!(resp.get("audience").unwrap().as_str().unwrap(), AUDIENCE);
            assert!(resp.get("created_at").unwrap().as_i64().unwrap() >= now);
            assert_eq!(resp.get("backend").unwrap().as_str().unwrap(), "janus");

            // Assert room presence in the DB.
            let conn = db.connection_pool().get().unwrap();
            let id = Uuid::parse_str(resp.get("id").unwrap().as_str().unwrap()).unwrap();
            let query = crate::schema::room::table.find(id);
            assert_eq!(query.execute(&conn).unwrap(), 1);
        });
    }

    #[test]
    fn read_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Insert a room.
            let conn = db.connection_pool().get().unwrap();
            let room = insert_room(&conn);
            drop(conn);

            // Make room.read request.
            let state = build_state(&db);
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({"id": room.id()});
            let request: ReadRequest = agent.build_request("room.read", &payload).unwrap();
            let result = await!(state.read(request)).unwrap();
            let outgoing_envelope = result.first().unwrap();
            let resp = extract_payload(outgoing_envelope).expect("Failed to extract payload");

            // Assert response.
            assert_eq!(
                resp.get("id").unwrap().as_str().unwrap(),
                room.id().to_string()
            );

            let start = match room.time().0 {
                Bound::Included(val) => val,
                _ => panic!("Bad room time"),
            };

            assert_eq!(resp.get("time").unwrap().get(0).unwrap(), start.timestamp());

            assert!(resp.get("time").unwrap().get(1).unwrap().is_null());
            assert_eq!(resp.get("audience").unwrap().as_str().unwrap(), AUDIENCE);

            assert_eq!(
                resp.get("created_at").unwrap().as_i64().unwrap(),
                room.created_at().timestamp()
            );

            assert_eq!(resp.get("backend").unwrap().as_str().unwrap(), "janus");
        });
    }

    #[test]
    fn update_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Insert a room.
            let conn = db.connection_pool().get().unwrap();
            let room = insert_room(&conn);
            drop(conn);

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
            let result = await!(state.update(request)).unwrap();
            let outgoing_envelope = result.first().unwrap();
            let resp = extract_payload(outgoing_envelope).expect("Failed to extract payload");

            // Assert response.
            assert_eq!(
                resp.get("id").unwrap().as_str().unwrap(),
                room.id().to_string()
            );

            let start = match room.time().0 {
                Bound::Included(val) => val,
                _ => panic!("Bad room time"),
            };

            assert_eq!(resp.get("time").unwrap().get(0).unwrap(), start.timestamp());

            assert!(resp.get("time").unwrap().get(1).unwrap().is_null());
            assert_eq!(
                resp.get("audience").unwrap().as_str().unwrap(),
                "dev.svc.example.net"
            );
            assert_eq!(resp.get("backend").unwrap().as_str().unwrap(), "none");
        });
    }

    #[test]
    fn delete_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Insert a room.
            let conn = db.connection_pool().get().unwrap();
            let room = insert_room(&conn);
            drop(conn);

            // Make room.delete request.
            let state = build_state(&db);
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({"id": room.id()});
            let request: DeleteRequest = agent.build_request("room.delete", &payload).unwrap();
            await!(state.delete(request)).unwrap();

            // Assert room abscence in the DB.
            let conn = db.connection_pool().get().unwrap();
            let query = crate::schema::room::table.find(room.id());
            assert_eq!(query.execute(&conn).unwrap(), 0);
        });
    }

    #[test]
    fn enter_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", AUDIENCE);

            // Insert a room.
            let conn = db.connection_pool().get().unwrap();
            let room = insert_room(&conn);
            drop(conn);

            // Make room.enter request.
            let state = build_state(&db);
            let payload = json!({"id": room.id()});
            let request: EnterRequest = agent.build_request("room.enter", &payload).unwrap();
            let result = await!(state.enter(request)).unwrap();
            let outgoing_envelope = result.first().unwrap();
            let resp = extract_payload(outgoing_envelope).expect("Failed to extract payload");

            // Assert outgoing broker request.
            let object = resp.get("object").unwrap().as_array().unwrap();
            let object: Vec<&str> = object.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(
                object,
                vec!["rooms", room.id().to_string().as_str(), "events"]
            );

            let subject = resp.get("subject").unwrap().as_str().unwrap();
            assert_eq!(subject, format!("v1/agents/{}", agent.agent_id()));

            let props = extract_properties(outgoing_envelope).expect("Failed to extract props");
            assert_eq!(props.get("method").unwrap(), "subscription.create");
        });
    }

    #[test]
    fn leave_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", AUDIENCE);

            // Insert a room.
            let conn = db.connection_pool().get().unwrap();
            let room = insert_room(&conn);
            drop(conn);

            // Make room.leave request.
            let state = build_state(&db);
            let payload = json!({"id": room.id()});
            let request: LeaveRequest = agent.build_request("room.leave", &payload).unwrap();
            let result = await!(state.leave(request)).unwrap();
            let outgoing_envelope = result.first().unwrap();

            // Assert outgoing broker request.
            let resp = extract_payload(outgoing_envelope).expect("Failed to extract payload");
            let object = resp.get("object").unwrap().as_array().unwrap();

            assert_eq!(
                object.iter().map(|v| v.as_str().unwrap()).collect::<Vec<&str>>(),
                vec!["rooms", room.id().to_string().as_str(), "events"]
            );

            let subject = resp.get("subject").unwrap().as_str().unwrap();
            assert_eq!(subject, format!("v1/agents/{}", agent.agent_id()));

            let props = extract_properties(outgoing_envelope).expect("Failed to extract props");
            assert_eq!(props.get("method").unwrap(), "subscription.delete");
        });
    }
}
