use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use failure::Error;
use serde_derive::Deserialize;
use serde_json::{json, Value as JsonValue};
use svc_agent::mqtt::{
    IncomingRequest, IncomingRequestProperties, IncomingResponse, IntoPublishableDump,
    OutgoingEvent, OutgoingRequest, OutgoingResponse, OutgoingResponseProperties, ResponseStatus,
    ShortTermTimingProperties, SubscriptionTopic,
};
use svc_agent::{Addressable, AgentId, Subscription};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint::shared;
use crate::app::{endpoint, API_VERSION};
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
    pub(crate) async fn broadcast(
        &self,
        inreq: BroadcastRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        let conn = self.db.get()?;
        let room = find_room(inreq.payload().room_id, &conn)?;
        shared::check_room_presence(&room, &inreq.properties().as_agent_id(), &conn)?;

        let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);

        let resp = inreq.to_response(
            json!({}),
            ResponseStatus::OK,
            short_term_timing.clone(),
            API_VERSION,
        );

        let resp_box = Box::new(resp) as Box<dyn IntoPublishableDump>;
        let payload = inreq.payload().data.to_owned();

        let props = inreq
            .properties()
            .to_event("message.broadcast", short_term_timing);

        let to_uri = format!("rooms/{}/events", inreq.payload().room_id);
        let event = OutgoingEvent::broadcast(payload, props, &to_uri);
        let event_box = Box::new(event) as Box<dyn IntoPublishableDump>;

        vec![resp_box, event_box].into()
    }

    pub(crate) async fn unicast(
        &self,
        inreq: UnicastRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        let conn = self.db.get()?;
        let room = find_room(inreq.payload().room_id, &conn)?;
        shared::check_room_presence(&room, &inreq.properties().as_agent_id(), &conn)?;
        shared::check_room_presence(&room, &inreq.payload().agent_id, &conn)?;

        let to = &inreq.payload().agent_id;
        let payload = &inreq.payload().data;

        let response_topic = Subscription::multicast_requests_from(to, Some(API_VERSION))
            .subscription_topic(&self.me, API_VERSION)
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

        let props = inreq.properties().to_request(
            inreq.properties().method(),
            &response_topic,
            &correlation_data,
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        OutgoingRequest::unicast(payload.to_owned(), props, to, API_VERSION).into()
    }

    pub(crate) async fn callback(
        &self,
        inresp: UnicastIncomingResponse,
        start_timestamp: DateTime<Utc>,
    ) -> Result<Vec<Box<dyn IntoPublishableDump>>, Error> {
        let reqp =
            from_base64::<IncomingRequestProperties>(inresp.properties().correlation_data())?;

        let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);

        let long_term_timing = inresp
            .properties()
            .long_term_timing()
            .clone()
            .update_cumulative_timings(&short_term_timing);

        let props = OutgoingResponseProperties::new(
            inresp.properties().status(),
            reqp.correlation_data(),
            long_term_timing,
            short_term_timing,
            inresp.properties().tracking().clone(),
        );

        let message =
            OutgoingResponse::unicast(inresp.payload().to_owned(), props, &reqp, API_VERSION);

        Ok(vec![Box::new(message) as Box<dyn IntoPublishableDump>])
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
    use svc_agent::mqtt::ResponseStatus;

    use super::*;
    use crate::app::API_VERSION;
    use crate::test_helpers::{
        agent::TestAgent, db::TestDb, factory, factory::insert_room, Message, AUDIENCE,
    };

    const AGENT_LABEL: &str = "web";

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

            let mut result = state
                .unicast(request, Utc::now())
                .await
                .into_result()
                .unwrap();

            let evt = Message::<JsonValue>::from_publishable(result.remove(0))
                .expect("Failed to parse message");

            assert_eq!(
                evt.topic(),
                format!(
                    "agents/{}/api/{}/in/conference.{}",
                    receiver.agent_id(),
                    API_VERSION,
                    AUDIENCE,
                ),
            );

            assert_eq!(*evt.payload(), json!({"key": "value"}));
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
            match state.unicast(request, Utc::now()).await.into_result() {
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
            match state.unicast(request, Utc::now()).await.into_result() {
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
            match state.unicast(request, Utc::now()).await.into_result() {
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

            let mut result = state
                .broadcast(request, Utc::now())
                .await
                .into_result()
                .unwrap();

            // Assert response.
            let resp = Message::<JsonValue>::from_publishable(result.remove(0))
                .expect("Failed to parse message");

            assert_eq!(resp.properties().kind(), "response");

            // Assert broadcast event.
            let evt = Message::<JsonValue>::from_publishable(result.remove(0))
                .expect("Failed to parse message");

            assert_eq!(evt.properties().kind(), "event");

            assert_eq!(
                evt.topic(),
                format!(
                    "apps/conference.{}/api/{}/rooms/{}/events",
                    AUDIENCE,
                    API_VERSION,
                    room.id()
                ),
            );

            assert_eq!(*evt.payload(), json!({"key": "value"}));
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
            match state.broadcast(request, Utc::now()).await.into_result() {
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
            match state.broadcast(request, Utc::now()).await.into_result() {
                Ok(_) => panic!("Expected message.broadcast to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }
}
