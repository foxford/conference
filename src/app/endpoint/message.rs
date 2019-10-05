use diesel::pg::PgConnection;
use failure::Error;
use serde_derive::Deserialize;
use serde_json::{json, Value as JsonValue};
use svc_agent::mqtt::{
    IncomingRequest, IncomingRequestProperties, IncomingResponse, OutgoingEvent,
    OutgoingEventProperties, OutgoingRequest, OutgoingRequestProperties, OutgoingResponse,
    OutgoingResponseProperties, Publishable, ResponseStatus, SubscriptionTopic,
};
use svc_agent::{Addressable, AgentId, Subscription};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
use crate::app::endpoint::shared::check_room_presence;
use crate::db::{room, ConnectionPool};
use crate::util::{from_base64, to_base64};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type UnicastRequest = IncomingRequest<UnicastRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct UnicastRequestData {
    agent_id: AgentId,
    room_id: Uuid,
    data: JsonValue,
}

pub(crate) type UnicastIncomingResponse = IncomingResponse<JsonValue>;

pub(crate) type BroadcastRequest = IncomingRequest<BroadcastRequestData>;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct BroadcastRequestData {
    room_id: Uuid,
    data: JsonValue,
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    me: AgentId,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(me: AgentId, db: ConnectionPool) -> Self {
        Self { me, db }
    }
}

impl State {
    pub(crate) async fn broadcast(&self, inreq: BroadcastRequest) -> endpoint::Result {
        let conn = self.db.get()?;
        let room = find_room(inreq.payload().room_id, &conn)?;
        check_room_presence(&room, &inreq.properties().as_agent_id(), &conn)?;

        let resp = inreq.to_response(json!({}), ResponseStatus::OK);
        let resp_box = Box::new(resp) as Box<dyn Publishable>;

        let payload = inreq.payload().data.to_owned();
        let props = OutgoingEventProperties::new("message.broadcast");
        let to_uri = format!("rooms/{}/events", inreq.payload().room_id);
        let event = OutgoingEvent::broadcast(payload, props, &to_uri);
        let event_box = Box::new(event) as Box<dyn Publishable>;

        vec![resp_box, event_box].into()
    }

    pub(crate) async fn unicast(&self, inreq: UnicastRequest) -> endpoint::Result {
        let conn = self.db.get()?;
        let room = find_room(inreq.payload().room_id, &conn)?;
        check_room_presence(&room, &inreq.properties().as_agent_id(), &conn)?;
        check_room_presence(&room, &inreq.payload().agent_id, &conn)?;

        let to = &inreq.payload().agent_id;
        let payload = &inreq.payload().data;

        let response_topic = Subscription::multicast_requests_from(to)
            .subscription_topic(&self.me)
            .map_err(|_| {
                SvcError::builder()
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                    .detail("error building responses subscription topic")
                    .build()
            })?;

        let correlation_data = to_base64(inreq.properties()).map_err(|_| {
            SvcError::builder()
                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                .detail("error encoding incoming request properties")
                .build()
        })?;

        let props = OutgoingRequestProperties::new(
            inreq.properties().method(),
            &response_topic,
            &correlation_data,
        );

        OutgoingRequest::unicast(payload.to_owned(), props, to).into()
    }

    pub(crate) async fn callback(
        &self,
        inresp: UnicastIncomingResponse,
    ) -> Result<Vec<Box<dyn Publishable>>, Error> {
        let reqp =
            from_base64::<IncomingRequestProperties>(inresp.properties().correlation_data())?;
        let payload = inresp.payload();

        let props =
            OutgoingResponseProperties::new(inresp.properties().status(), reqp.correlation_data());

        let message = OutgoingResponse::unicast(payload.to_owned(), props, &reqp);
        Ok(vec![Box::new(message) as Box<dyn Publishable>])
    }
}

fn find_room(id: Uuid, conn: &PgConnection) -> Result<room::Object, SvcError> {
    room::FindQuery::new()
        .time(room::now())
        .id(id)
        .execute(&conn)?
        .ok_or_else(|| {
            SvcError::builder()
                .status(ResponseStatus::NOT_FOUND)
                .detail(&format!("the room = '{}' is not found", id))
                .build()
        })
}

#[cfg(test)]
mod test {
    use std::ops::Try;

    use failure::format_err;
    use serde_json::{json, Value as JsonValue};
    use svc_agent::{mqtt::ResponseStatus, Destination};

    use super::*;
    use crate::test_helpers::{
        agent::TestAgent, db::TestDb, extract_payload, factory, factory::insert_room,
    };

    const AGENT_LABEL: &str = "web";
    const AUDIENCE: &str = "dev.svc.example.org";

    #[test]
    fn unicast_message() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let sender = TestAgent::new(AGENT_LABEL, "sender", AUDIENCE);
            let receiver = TestAgent::new(AGENT_LABEL, "receiver", AUDIENCE);

            // Insert room with online both sender and receiver.
            let room = db
                .connection_pool()
                .get()
                .map_err(|err| format_err!("Failed to get DB connection: {}", err))
                .and_then(|conn| {
                    let room = insert_room(&conn, AUDIENCE);

                    factory::Agent::new()
                        .room_id(room.id())
                        .agent_id(sender.agent_id())
                        .insert(&conn)?;

                    factory::Agent::new()
                        .room_id(room.id())
                        .agent_id(receiver.agent_id())
                        .insert(&conn)?;

                    Ok(room)
                })
                .expect("Failed to insert room");

            let payload = json!({
                "agent_id": receiver.agent_id().to_string(),
                "room_id": room.id(),
                "data": {"key": "value"},
            });

            let request: UnicastRequest =
                sender.build_request("message.unicast", &payload).unwrap();

            let state = State::new(sender.agent_id().clone(), db.connection_pool().clone());
            let mut result = state.unicast(request).await.into_result().unwrap();
            let message = result.remove(0);

            match message.destination() {
                &Destination::Unicast(ref agent_id) => assert_eq!(agent_id, receiver.agent_id()),
                _ => panic!("Expected unicast destination"),
            }

            let payload: JsonValue = extract_payload(message).unwrap();
            assert_eq!(payload, json!({"key": "value"}));
        });
    }

    #[test]
    fn unicast_message_to_missing_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let sender = TestAgent::new(AGENT_LABEL, "sender", AUDIENCE);
            let receiver = TestAgent::new(AGENT_LABEL, "receiver", AUDIENCE);

            // Send message.unicast request.
            let payload = json!({
                "agent_id": receiver.agent_id().to_string(),
                "room_id": Uuid::new_v4(),
                "data": {"key": "value"},
            });

            let request: UnicastRequest =
                sender.build_request("message.unicast", &payload).unwrap();

            let state = State::new(sender.agent_id().clone(), db.connection_pool().clone());

            // Assert 404 response.
            match state.unicast(request).await.into_result() {
                Ok(_) => panic!("Expected message.unicast to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn unicast_message_when_sender_is_not_in_the_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let sender = TestAgent::new(AGENT_LABEL, "sender", AUDIENCE);
            let receiver = TestAgent::new(AGENT_LABEL, "receiver", AUDIENCE);

            // Insert room with online receiver only.
            let room = db
                .connection_pool()
                .get()
                .map_err(|err| format_err!("Failed to get DB connection: {}", err))
                .and_then(|conn| {
                    let room = insert_room(&conn, AUDIENCE);

                    factory::Agent::new()
                        .room_id(room.id())
                        .agent_id(receiver.agent_id())
                        .insert(&conn)?;

                    Ok(room)
                })
                .expect("Failed to insert room");

            // Send message.unicast request.
            let payload = json!({
                "agent_id": receiver.agent_id().to_string(),
                "room_id": room.id(),
                "data": {"key": "value"},
            });

            let request: UnicastRequest =
                sender.build_request("message.unicast", &payload).unwrap();

            let state = State::new(sender.agent_id().clone(), db.connection_pool().clone());

            // Assert 404 response.
            match state.unicast(request).await.into_result() {
                Ok(_) => panic!("Expected message.unicast to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn unicast_message_when_receiver_is_not_in_the_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let sender = TestAgent::new(AGENT_LABEL, "sender", AUDIENCE);
            let receiver = TestAgent::new(AGENT_LABEL, "receiver", AUDIENCE);

            // Insert room with online sender only.
            let room = db
                .connection_pool()
                .get()
                .map_err(|err| format_err!("Failed to get DB connection: {}", err))
                .and_then(|conn| {
                    let room = insert_room(&conn, AUDIENCE);

                    factory::Agent::new()
                        .room_id(room.id())
                        .agent_id(sender.agent_id())
                        .insert(&conn)?;

                    Ok(room)
                })
                .expect("Failed to insert room");

            // Send message.unicast request.
            let payload = json!({
                "agent_id": receiver.agent_id().to_string(),
                "room_id": room.id(),
                "data": {"key": "value"},
            });

            let request: UnicastRequest =
                sender.build_request("message.unicast", &payload).unwrap();

            let state = State::new(sender.agent_id().clone(), db.connection_pool().clone());

            // Assert 404 response.
            match state.unicast(request).await.into_result() {
                Ok(_) => panic!("Expected message.unicast to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn broadcast_message() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let sender = TestAgent::new(AGENT_LABEL, "sender", AUDIENCE);

            // Insert room with online agent.
            let room = db
                .connection_pool()
                .get()
                .map_err(|err| format_err!("Failed to get DB connection: {}", err))
                .and_then(|conn| {
                    let room = insert_room(&conn, AUDIENCE);
                    let agent_factory = factory::Agent::new().room_id(room.id());
                    agent_factory.agent_id(sender.agent_id()).insert(&conn)?;
                    Ok(room)
                })
                .expect("Failed to insert room");

            // Send message.broadcast request.
            let payload = json!({
                "room_id": room.id(),
                "data": {"key": "value"},
            });

            let request: BroadcastRequest =
                sender.build_request("message.broadcast", &payload).unwrap();

            let state = State::new(sender.agent_id().clone(), db.connection_pool().clone());
            let mut result = state.broadcast(request).await.into_result().unwrap();

            // Assert response.
            let message = result.remove(0);
            assert_eq!(message.message_type(), "response");

            // Assert broadcast event.
            let message = result.remove(0);
            assert_eq!(message.message_type(), "event");

            match message.destination() {
                Destination::Broadcast(destination) => {
                    assert_eq!(destination, &format!("rooms/{}/events", room.id()))
                }
                _ => panic!("Expected broadcast destination"),
            }

            let payload: JsonValue = extract_payload(message).unwrap();
            assert_eq!(payload, json!({"key": "value"}));
        });
    }

    #[test]
    fn broadcast_message_to_missing_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let sender = TestAgent::new(AGENT_LABEL, "sender", AUDIENCE);

            // Send message.broadcast request.
            let payload = json!({
                "room_id": Uuid::new_v4(),
                "data": {"key": "value"},
            });

            let request: BroadcastRequest =
                sender.build_request("message.broadcast", &payload).unwrap();

            let state = State::new(sender.agent_id().clone(), db.connection_pool().clone());

            // Assert 404 response.
            match state.broadcast(request).await.into_result() {
                Ok(_) => panic!("Expected message.broadcast to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn broadcast_message_when_not_in_the_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let sender = TestAgent::new(AGENT_LABEL, "sender", AUDIENCE);

            // Insert room with online agent.
            let room = db
                .connection_pool()
                .get()
                .map_err(|err| format_err!("Failed to get DB connection: {}", err))
                .map(|conn| insert_room(&conn, AUDIENCE))
                .expect("Failed to insert room");

            // Send message.broadcast request.
            let payload = json!({
                "room_id": room.id(),
                "data": {"key": "value"},
            });

            let request: BroadcastRequest =
                sender.build_request("message.broadcast", &payload).unwrap();

            let state = State::new(sender.agent_id().clone(), db.connection_pool().clone());

            // Assert 404 response.
            match state.broadcast(request).await.into_result() {
                Ok(_) => panic!("Expected message.broadcast to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }
}
