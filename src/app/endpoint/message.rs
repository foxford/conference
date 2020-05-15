use std::result::Result as StdResult;

use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use serde_derive::Deserialize;
use serde_json::{json, Value as JsonValue};
use svc_agent::mqtt::{
    IncomingRequestProperties, IncomingResponseProperties, IntoPublishableDump, OutgoingRequest,
    OutgoingResponse, OutgoingResponseProperties, ResponseStatus, ShortTermTimingProperties,
    SubscriptionTopic,
};
use svc_agent::{Addressable, AgentId, Subscription};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::API_VERSION;
use crate::db::{self, room::Object as Room};
use crate::util::{from_base64, to_base64};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct UnicastRequest {
    agent_id: AgentId,
    room_id: Uuid,
    data: JsonValue,
}

pub(crate) struct UnicastHandler;

#[async_trait]
impl RequestHandler for UnicastHandler {
    type Payload = UnicastRequest;
    const ERROR_TITLE: &'static str = "Failed to send unicast message";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        {
            let conn = context.db().get()?;
            let room = find_room(payload.room_id, &conn)?;
            check_room_presence(&room, reqp.as_agent_id(), &conn)?;
            check_room_presence(&room, &payload.agent_id, &conn)?;
        }

        let response_topic =
            Subscription::multicast_requests_from(&payload.agent_id, Some(API_VERSION))
                .subscription_topic(context.agent_id(), API_VERSION)
                .map_err(|err| format!("error building responses subscription topic: {}", err))
                .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

        let correlation_data = to_base64(reqp)
            .map_err(|err| format!("error encoding incoming request properties: {}", err))
            .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

        let props = reqp.to_request(
            reqp.method(),
            &response_topic,
            &correlation_data,
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        let req = OutgoingRequest::unicast(
            payload.data.to_owned(),
            props,
            &payload.agent_id,
            API_VERSION,
        );

        Ok(Box::new(stream::once(
            Box::new(req) as Box<dyn IntoPublishableDump + Send>
        )))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct BroadcastRequest {
    room_id: Uuid,
    data: JsonValue,
}

pub(crate) struct BroadcastHandler;

#[async_trait]
impl RequestHandler for BroadcastHandler {
    type Payload = BroadcastRequest;
    const ERROR_TITLE: &'static str = "Failed to send broadcast message";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let conn = context.db().get()?;
            let room = find_room(payload.room_id, &conn)?;
            check_room_presence(&room, &reqp.as_agent_id(), &conn)?;
            room
        };

        // Respond and broadcast to the room topic.
        let response =
            shared::build_response(ResponseStatus::OK, json!({}), reqp, start_timestamp, None);

        let notification = shared::build_notification(
            "message.broadcast",
            &format!("rooms/{}/events", room.id()),
            payload.data.to_owned(),
            reqp,
            start_timestamp,
        );

        Ok(Box::new(stream::from_iter(vec![response, notification])))
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct CallbackHandler;

#[async_trait]
impl ResponseHandler for CallbackHandler {
    type Payload = JsonValue;

    async fn handle<C: Context>(
        _context: &C,
        payload: Self::Payload,
        respp: &IncomingResponseProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let reqp = from_base64::<IncomingRequestProperties>(respp.correlation_data())
            .map_err(|err| err.to_string())
            .status(ResponseStatus::BAD_REQUEST)?;

        let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);

        let long_term_timing = respp
            .long_term_timing()
            .clone()
            .update_cumulative_timings(&short_term_timing);

        let props = OutgoingResponseProperties::new(
            respp.status(),
            reqp.correlation_data(),
            long_term_timing,
            short_term_timing,
            respp.tracking().clone(),
        );

        let resp = OutgoingResponse::unicast(payload.to_owned(), props, &reqp, API_VERSION);
        let boxed_resp = Box::new(resp) as Box<dyn IntoPublishableDump + Send>;
        Ok(Box::new(stream::once(boxed_resp)))
    }
}

///////////////////////////////////////////////////////////////////////////////

fn find_room(id: Uuid, conn: &PgConnection) -> StdResult<Room, SvcError> {
    db::room::FindQuery::new()
        .time(db::room::now())
        .id(id)
        .execute(&conn)?
        .ok_or_else(|| format!("the room = '{}' is not found", id))
        .status(ResponseStatus::NOT_FOUND)
}

fn check_room_presence(
    room: &Room,
    agent_id: &AgentId,
    conn: &PgConnection,
) -> StdResult<(), SvcError> {
    let results = db::agent::ListQuery::new()
        .room_id(room.id())
        .agent_id(agent_id)
        .status(db::agent::Status::Ready)
        .execute(conn)?;

    if results.len() == 0 {
        let err = format!(
            "agent = '{}' is not online in the room = '{}'",
            agent_id,
            room.id()
        );

        Err(err).status(ResponseStatus::NOT_FOUND)
    } else {
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod unicast {
        use uuid::Uuid;

        use crate::app::API_VERSION;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn unicast_message() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let sender = TestAgent::new("web", "sender", USR_AUDIENCE);
                let receiver = TestAgent::new("web", "receiver", USR_AUDIENCE);

                // Insert room with online both sender and receiver.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        let room = shared_helpers::insert_room(&conn);

                        factory::Agent::new()
                            .room_id(room.id())
                            .agent_id(sender.agent_id())
                            .insert(&conn);

                        factory::Agent::new()
                            .room_id(room.id())
                            .agent_id(receiver.agent_id())
                            .insert(&conn);

                        room
                    })
                    .expect("Failed to insert room");

                // Make message.unicast request.
                let context = TestContext::new(db, TestAuthz::new());

                let payload = UnicastRequest {
                    agent_id: receiver.agent_id().to_owned(),
                    room_id: room.id(),
                    data: json!({ "key": "value" }),
                };

                let messages = handle_request::<UnicastHandler>(&context, &sender, payload)
                    .await
                    .expect("Unicast message sending failed");

                // Assert outgoing request.
                let (payload, _reqp, topic) = find_request::<JsonValue>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    receiver.agent_id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(payload, json!({"key": "value"}));
            });
        }

        #[test]
        fn unicast_message_to_missing_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let context = TestContext::new(db, TestAuthz::new());
                let sender = TestAgent::new("web", "sender", USR_AUDIENCE);
                let receiver = TestAgent::new("web", "receiver", USR_AUDIENCE);

                let payload = UnicastRequest {
                    agent_id: receiver.agent_id().to_owned(),
                    room_id: Uuid::new_v4(),
                    data: json!({ "key": "value" }),
                };

                let err = handle_request::<UnicastHandler>(&context, &sender, payload)
                    .await
                    .expect_err("Unexpected success on unicast message sending");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }

        #[test]
        fn unicast_message_when_sender_is_not_in_the_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let sender = TestAgent::new("web", "sender", USR_AUDIENCE);
                let receiver = TestAgent::new("web", "receiver", USR_AUDIENCE);

                // Insert room with online receiver only.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        let room = shared_helpers::insert_room(&conn);

                        factory::Agent::new()
                            .room_id(room.id())
                            .agent_id(receiver.agent_id())
                            .insert(&conn);

                        room
                    })
                    .expect("Failed to insert room");

                // Make message.unicast request.
                let context = TestContext::new(db, TestAuthz::new());

                let payload = UnicastRequest {
                    agent_id: receiver.agent_id().to_owned(),
                    room_id: room.id(),
                    data: json!({ "key": "value" }),
                };

                let err = handle_request::<UnicastHandler>(&context, &sender, payload)
                    .await
                    .expect_err("Unexpected success on unicast message sending");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }

        #[test]
        fn unicast_message_when_receiver_is_not_in_the_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let sender = TestAgent::new("web", "sender", USR_AUDIENCE);
                let receiver = TestAgent::new("web", "receiver", USR_AUDIENCE);

                // Insert room with online sender only.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        let room = shared_helpers::insert_room(&conn);

                        factory::Agent::new()
                            .room_id(room.id())
                            .agent_id(sender.agent_id())
                            .insert(&conn);

                        room
                    })
                    .expect("Failed to insert room");

                // Make message.unicast request.
                let context = TestContext::new(db, TestAuthz::new());

                let payload = UnicastRequest {
                    agent_id: receiver.agent_id().to_owned(),
                    room_id: room.id(),
                    data: json!({ "key": "value" }),
                };

                let err = handle_request::<UnicastHandler>(&context, &sender, payload)
                    .await
                    .expect_err("Unexpected success on unicast message sending");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }

    mod broadcast {
        use uuid::Uuid;

        use crate::app::API_VERSION;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn broadcast_message() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let sender = TestAgent::new("web", "sender", USR_AUDIENCE);

                // Insert room with online agent.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        let room = shared_helpers::insert_room(&conn);
                        let agent_factory = factory::Agent::new().room_id(room.id());
                        agent_factory.agent_id(sender.agent_id()).insert(&conn);
                        room
                    })
                    .expect("Failed to insert room");

                // Make message.broadcast request.
                let context = TestContext::new(db, TestAuthz::new());

                let payload = BroadcastRequest {
                    room_id: room.id(),
                    data: json!({ "key": "value" }),
                };

                let messages = handle_request::<BroadcastHandler>(&context, &sender, payload)
                    .await
                    .expect("Broadcast message sending failed");

                // Assert response.
                let (_, respp) = find_response::<JsonValue>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);

                // Assert broadcast event.
                let (payload, _evp, topic) = find_event::<JsonValue>(messages.as_slice());

                let expected_topic = format!(
                    "apps/conference.{}/api/{}/rooms/{}/events",
                    SVC_AUDIENCE,
                    API_VERSION,
                    room.id(),
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(payload, json!({"key": "value"}));
            });
        }

        #[test]
        fn broadcast_message_to_missing_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let context = TestContext::new(db, TestAuthz::new());
                let sender = TestAgent::new("web", "sender", USR_AUDIENCE);

                let payload = BroadcastRequest {
                    room_id: Uuid::new_v4(),
                    data: json!({ "key": "value" }),
                };

                let err = handle_request::<BroadcastHandler>(&context, &sender, payload)
                    .await
                    .expect_err("Unexpected success on unicast message sending");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }

        #[test]
        fn broadcast_message_when_not_in_the_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let sender = TestAgent::new("web", "sender", USR_AUDIENCE);

                // Insert room with online agent.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| shared_helpers::insert_room(&conn))
                    .expect("Failed to insert room");

                // Make message.broadcast request.
                let context = TestContext::new(db, TestAuthz::new());

                let payload = BroadcastRequest {
                    room_id: room.id(),
                    data: json!({ "key": "value" }),
                };

                let err = handle_request::<BroadcastHandler>(&context, &sender, payload)
                    .await
                    .expect_err("Unexpected success on unicast message sending");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }
}
