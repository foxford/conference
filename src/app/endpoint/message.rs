use crate::{
    app::{
        context::Context,
        endpoint::prelude::*,
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
        API_VERSION,
    },
    db,
};
use anyhow::Context as AnyhowContext;
use async_trait::async_trait;
use futures::stream;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IncomingResponseProperties, IntoPublishableMessage,
        OutgoingRequest, OutgoingResponse, OutgoingResponseProperties, ResponseStatus,
        ShortTermTimingProperties, SubscriptionTopic,
    },
    Addressable, AgentId, Subscription,
};

use tracing_attributes::instrument;

use super::MqttResult;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct CorrelationDataPayload {
    reqp: IncomingRequestProperties,
}

impl CorrelationDataPayload {
    pub fn new(reqp: IncomingRequestProperties) -> Self {
        Self { reqp }
    }
}

#[derive(Debug, Deserialize)]
pub struct UnicastRequest {
    agent_id: AgentId,
    room_id: db::room::Id,
    data: JsonValue,
}

pub struct UnicastHandler;

#[async_trait]
impl RequestHandler for UnicastHandler {
    type Payload = UnicastRequest;
    const ERROR_TITLE: &'static str = "Failed to send unicast message";

    #[instrument(skip(context, payload, reqp), fields(room_id = %payload.room_id))]
    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let mqtt_params = reqp.as_mqtt_params()?;
        crate::util::spawn_blocking({
            let room_id = payload.room_id;
            let reqp_agent_id = reqp.as_agent_id().clone();
            let payload_agent_id = payload.agent_id.clone();

            let conn = context.get_conn().await?;
            move || {
                let room =
                    helpers::find_room_by_id(room_id, helpers::RoomTimeRequirement::Open, &conn)?;

                helpers::check_room_presence(&room, &reqp_agent_id, &conn)?;
                helpers::check_room_presence(&room, &payload_agent_id, &conn)?;
                Ok::<_, AppError>(room)
            }
        })
        .await?;

        let response_topic =
            Subscription::multicast_requests_from(&payload.agent_id, Some(API_VERSION))
                .subscription_topic(context.agent_id(), API_VERSION)
                .context("Error building responses subscription topic")
                .error(AppErrorKind::MessageBuildingFailed)?;

        let corr_data_payload = CorrelationDataPayload::new(mqtt_params.to_owned());

        let corr_data = CorrelationData::MessageUnicast(corr_data_payload)
            .dump()
            .error(AppErrorKind::MessageBuildingFailed)?;

        let props = mqtt_params.to_request(
            mqtt_params.method(),
            &response_topic,
            &corr_data,
            ShortTermTimingProperties::until_now(context.start_timestamp()),
        );
        let mut response = Response::new(
            ResponseStatus::OK,
            json!({}),
            context.start_timestamp(),
            None,
        );
        response.add_message(Box::new(OutgoingRequest::unicast(
            payload.data.to_owned(),
            props,
            &payload.agent_id,
            API_VERSION,
        )));

        context
            .metrics()
            .request_duration
            .message_unicast_request
            .observe_timestamp(context.start_timestamp());

        Ok(response)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct BroadcastRequest {
    room_id: db::room::Id,
    data: JsonValue,
    #[allow(dead_code)]
    label: Option<String>,
}

pub struct BroadcastHandler;

#[async_trait]
impl RequestHandler for BroadcastHandler {
    type Payload = BroadcastRequest;
    const ERROR_TITLE: &'static str = "Failed to send broadcast message";

    #[instrument(skip(context, payload, reqp), fields(room_id = %payload.room_id))]
    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let room = crate::util::spawn_blocking({
            let agent_id = reqp.as_agent_id().clone();
            let room_id = payload.room_id;

            let conn = context.get_conn().await?;
            move || {
                let room =
                    helpers::find_room_by_id(room_id, helpers::RoomTimeRequirement::Open, &conn)?;

                helpers::check_room_presence(&room, &agent_id, &conn)?;
                Ok::<_, AppError>(room)
            }
        })
        .await?;

        // Respond and broadcast to the room topic.
        let mut response = Response::new(
            ResponseStatus::OK,
            json!({}),
            context.start_timestamp(),
            None,
        );

        response.add_notification(
            "message.broadcast",
            &format!("rooms/{}/events", room.id()),
            payload.data,
            context.start_timestamp(),
        );
        context
            .metrics()
            .request_duration
            .message_broadcast
            .observe_timestamp(context.start_timestamp());

        Ok(response)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct UnicastResponseHandler;

#[async_trait]
impl ResponseHandler for UnicastResponseHandler {
    type Payload = JsonValue;
    type CorrelationData = CorrelationDataPayload;

    #[instrument(skip(context, payload, respp, corr_data))]
    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        respp: &IncomingResponseProperties,
        corr_data: &Self::CorrelationData,
    ) -> MqttResult {
        let short_term_timing = ShortTermTimingProperties::until_now(context.start_timestamp());

        let long_term_timing = respp
            .long_term_timing()
            .clone()
            .update_cumulative_timings(&short_term_timing);

        let props = OutgoingResponseProperties::new(
            respp.status(),
            corr_data.reqp.correlation_data(),
            long_term_timing,
            short_term_timing,
            respp.tracking().clone(),
            respp.local_tracking_label().clone(),
        );

        let resp = OutgoingResponse::unicast(payload, props, &corr_data.reqp, API_VERSION);
        let boxed_resp = Box::new(resp) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
        context
            .metrics()
            .request_duration
            .message_unicast_response
            .observe_timestamp(context.start_timestamp());
        Ok(Box::new(stream::once(std::future::ready(boxed_resp))))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod unicast {

        use crate::{
            app::API_VERSION,
            test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn unicast_message() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let sender = TestAgent::new("web", "sender", USR_AUDIENCE);
            let receiver = TestAgent::new("web", "receiver", USR_AUDIENCE);

            // Insert room with online both sender and receiver.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = shared_helpers::insert_room(&conn);
                    shared_helpers::insert_agent(&conn, sender.agent_id(), room.id());
                    shared_helpers::insert_agent(&conn, receiver.agent_id(), room.id());
                    room
                })
                .expect("Failed to insert room");

            // Make message.unicast request.
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = UnicastRequest {
                agent_id: receiver.agent_id().to_owned(),
                room_id: room.id(),
                data: json!({ "key": "value" }),
            };

            let messages = handle_request::<UnicastHandler>(&mut context, &sender, payload)
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
        }

        #[tokio::test]
        async fn unicast_message_to_missing_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;
            let sender = TestAgent::new("web", "sender", USR_AUDIENCE);
            let receiver = TestAgent::new("web", "receiver", USR_AUDIENCE);

            let payload = UnicastRequest {
                agent_id: receiver.agent_id().to_owned(),
                room_id: db::room::Id::random(),
                data: json!({ "key": "value" }),
            };

            let err = handle_request::<UnicastHandler>(&mut context, &sender, payload)
                .await
                .expect_err("Unexpected success on unicast message sending");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }

        #[tokio::test]
        async fn unicast_message_when_sender_is_not_in_the_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let sender = TestAgent::new("web", "sender", USR_AUDIENCE);
            let receiver = TestAgent::new("web", "receiver", USR_AUDIENCE);

            // Insert room with online receiver only.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = shared_helpers::insert_room(&conn);
                    shared_helpers::insert_agent(&conn, receiver.agent_id(), room.id());
                    room
                })
                .expect("Failed to insert room");

            // Make message.unicast request.
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = UnicastRequest {
                agent_id: receiver.agent_id().to_owned(),
                room_id: room.id(),
                data: json!({ "key": "value" }),
            };

            let err = handle_request::<UnicastHandler>(&mut context, &sender, payload)
                .await
                .expect_err("Unexpected success on unicast message sending");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
        }

        #[tokio::test]
        async fn unicast_message_when_receiver_is_not_in_the_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let sender = TestAgent::new("web", "sender", USR_AUDIENCE);
            let receiver = TestAgent::new("web", "receiver", USR_AUDIENCE);

            // Insert room with online sender only.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = shared_helpers::insert_room(&conn);
                    shared_helpers::insert_agent(&conn, sender.agent_id(), room.id());
                    room
                })
                .expect("Failed to insert room");

            // Make message.unicast request.
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = UnicastRequest {
                agent_id: receiver.agent_id().to_owned(),
                room_id: room.id(),
                data: json!({ "key": "value" }),
            };

            let err = handle_request::<UnicastHandler>(&mut context, &sender, payload)
                .await
                .expect_err("Unexpected success on unicast message sending");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
        }
    }

    mod broadcast {
        use crate::{
            app::API_VERSION,
            test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn broadcast_message() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
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
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = BroadcastRequest {
                room_id: room.id(),
                data: json!({ "key": "value" }),
                label: None,
            };

            let messages = handle_request::<BroadcastHandler>(&mut context, &sender, payload)
                .await
                .expect("Broadcast message sending failed");

            // Assert response.
            let (_, respp, _) = find_response::<JsonValue>(messages.as_slice());
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
        }

        #[tokio::test]
        async fn broadcast_message_to_missing_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;
            let sender = TestAgent::new("web", "sender", USR_AUDIENCE);

            let payload = BroadcastRequest {
                room_id: db::room::Id::random(),
                data: json!({ "key": "value" }),
                label: None,
            };

            let err = handle_request::<BroadcastHandler>(&mut context, &sender, payload)
                .await
                .expect_err("Unexpected success on unicast message sending");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }

        #[tokio::test]
        async fn broadcast_message_when_not_in_the_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let sender = TestAgent::new("web", "sender", USR_AUDIENCE);

            // Insert room with online agent.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| shared_helpers::insert_room(&conn))
                .expect("Failed to insert room");

            // Make message.broadcast request.
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = BroadcastRequest {
                room_id: room.id(),
                data: json!({ "key": "value" }),
                label: None,
            };

            let err = handle_request::<BroadcastHandler>(&mut context, &sender, payload)
                .await
                .expect_err("Unexpected success on unicast message sending");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
        }
    }
}
