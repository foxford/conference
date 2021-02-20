use std::result::Result as StdResult;

use async_std::stream;
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use svc_agent::{
    mqtt::{
        IncomingEventProperties, IncomingRequestProperties, IncomingResponseProperties,
        IntoPublishableMessage, OutgoingEvent, ResponseStatus, ShortTermTimingProperties,
        TrackingProperties,
    },
    Addressable, AgentId, Authenticable,
};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::db;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CorrelationDataPayload {
    reqp: IncomingRequestProperties,
    subject: AgentId,
    object: Vec<String>,
}

impl CorrelationDataPayload {
    pub(crate) fn new(
        reqp: IncomingRequestProperties,
        subject: AgentId,
        object: Vec<String>,
    ) -> Self {
        Self {
            reqp,
            subject,
            object,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub(crate) struct RoomEnterLeaveEvent {
    id: Uuid,
    agent_id: AgentId,
}

impl RoomEnterLeaveEvent {
    pub(crate) fn new(id: Uuid, agent_id: AgentId) -> Self {
        Self { id, agent_id }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct CreateDeleteResponsePayload {}

pub(crate) struct CreateResponseHandler;

#[async_trait]
impl ResponseHandler for CreateResponseHandler {
    type Payload = CreateDeleteResponsePayload;
    type CorrelationData = CorrelationDataPayload;

    async fn handle<C: Context>(
        context: &mut C,
        _payload: Self::Payload,
        respp: &IncomingResponseProperties,
        corr_data: &Self::CorrelationData,
    ) -> Result {
        ensure_broker(context, respp)?;

        context.add_logger_tags(o!(
            "agent_label" => respp.as_agent_id().label().to_owned(),
            "account_label" => respp.as_account_id().label().to_owned(),
            "audience" => respp.as_account_id().audience().to_owned(),
        ));

        // Find room.
        let room_id = try_room_id(&corr_data.object)?;
        helpers::find_room_by_id(context, room_id, helpers::RoomTimeRequirement::NotClosed)?;

        {
            let conn = context.get_conn()?;

            // Update agent state to `ready`.
            db::agent::UpdateQuery::new(&corr_data.subject, room_id)
                .status(db::agent::Status::Ready)
                .execute(&conn)?;
        }

        // Send a response to the original `room.enter` request and a room-wide notification.
        let response = helpers::build_response(
            ResponseStatus::OK,
            json!({}),
            &corr_data.reqp,
            context.start_timestamp(),
            None,
        );

        let notification = helpers::build_notification(
            "room.enter",
            &format!("rooms/{}/events", room_id),
            RoomEnterLeaveEvent::new(room_id, corr_data.subject.to_owned()),
            &corr_data.reqp,
            context.start_timestamp(),
        );

        Ok(Box::new(stream::from_iter(vec![response, notification])))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct DeleteResponseHandler;

#[async_trait]
impl ResponseHandler for DeleteResponseHandler {
    type Payload = CreateDeleteResponsePayload;
    type CorrelationData = CorrelationDataPayload;

    async fn handle<C: Context>(
        context: &mut C,
        _payload: Self::Payload,
        respp: &IncomingResponseProperties,
        corr_data: &Self::CorrelationData,
    ) -> Result {
        ensure_broker(context, respp)?;
        let room_id = try_room_id(&corr_data.object)?;
        let maybe_left = leave_room(context, &corr_data.subject, room_id, respp.tracking())?;

        match maybe_left {
            (false, _) => {
                Err(anyhow!("The agent is not found")).error(AppErrorKind::AgentNotEnteredTheRoom)
            }
            (true, mut messages) => {
                let response = helpers::build_response(
                    ResponseStatus::OK,
                    json!({}),
                    &corr_data.reqp,
                    context.start_timestamp(),
                    None,
                );

                let notification = helpers::build_notification(
                    "room.leave",
                    &format!("rooms/{}/events", room_id),
                    RoomEnterLeaveEvent::new(room_id, corr_data.subject.to_owned()),
                    &corr_data.reqp,
                    context.start_timestamp(),
                );

                messages.push(response);
                messages.push(notification);
                Ok(Box::new(stream::from_iter(messages)))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeleteEventPayload {
    subject: AgentId,
    object: Vec<String>,
}

pub(crate) struct DeleteEventHandler;

#[async_trait]
impl EventHandler for DeleteEventHandler {
    type Payload = DeleteEventPayload;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
    ) -> Result {
        ensure_broker(context, evp)?;
        let room_id = try_room_id(&payload.object)?;

        match leave_room(context, &payload.subject, room_id, evp.tracking())? {
            (false, _) => Ok(Box::new(stream::empty())),
            (true, mut messages) => {
                let outgoing_event_payload =
                    RoomEnterLeaveEvent::new(room_id, payload.subject.to_owned());
                let short_term_timing =
                    ShortTermTimingProperties::until_now(context.start_timestamp());
                let props = evp.to_event("room.leave", short_term_timing);
                let to_uri = format!("rooms/{}/events", room_id);
                let outgoing_event =
                    OutgoingEvent::broadcast(outgoing_event_payload, props, &to_uri);
                let notification =
                    Box::new(outgoing_event) as Box<dyn IntoPublishableMessage + Send>;

                messages.push(notification);
                Ok(Box::new(stream::from_iter(messages)))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

fn ensure_broker<C: Context, A: Addressable>(
    context: &mut C,
    sender: &A,
) -> StdResult<(), AppError> {
    if sender.as_account_id() == &context.config().broker_id {
        Ok(())
    } else {
        Err(anyhow!(
            "Expected subscription.delete event to be sent from the broker account '{}', got '{}'",
            context.config().broker_id,
            sender.as_account_id()
        ))
        .error(AppErrorKind::AccessDenied)
    }
}

fn try_room_id(object: &[String]) -> StdResult<Uuid, AppError> {
    let object: Vec<&str> = object.iter().map(AsRef::as_ref).collect();

    match object.as_slice() {
        ["rooms", room_id, "events"] => {
            Uuid::parse_str(room_id).map_err(|err| anyhow!("UUID parse error: {}", err))
        }
        _ => Err(anyhow!(
            "Bad 'object' format; expected [\"room\", <ROOM_ID>, \"events\"], got: {:?}",
            object
        )),
    }
    .error(AppErrorKind::InvalidSubscriptionObject)
}

fn leave_room<C: Context>(
    context: &mut C,
    agent_id: &AgentId,
    room_id: Uuid,
    tracking: &TrackingProperties,
) -> StdResult<(bool, Vec<Box<dyn IntoPublishableMessage + Send>>), AppError> {
    // Delete agent from the DB.
    context.add_logger_tags(o!("room_id" => room_id.to_string()));
    let conn = context.get_conn()?;

    let row_count = db::agent::DeleteQuery::new()
        .agent_id(agent_id)
        .room_id(room_id)
        .execute(&conn)?;

    if row_count != 1 {
        return Ok((false, vec![]));
    }

    // `agent.leave` requests to Janus instances that host active streams in this room.
    let streams = db::janus_rtc_stream::ListQuery::new()
        .room_id(room_id)
        .active(true)
        .execute(&conn)?;

    let mut is_publisher = false;
    let mut messages: Vec<Box<dyn IntoPublishableMessage + Send>> =
        Vec::with_capacity(streams.len() + 1);

    for stream in streams.iter() {
        // If the agent is a publisher.
        if stream.sent_by() == agent_id {
            // Stop the stream.
            db::janus_rtc_stream::stop(stream.id(), &conn)?;
            is_publisher = true;
        }
    }

    // Disconnect stream readers since the stream has gone.
    if is_publisher {
        db::agent_connection::BulkDisconnectByRoomQuery::new(room_id).execute(&conn)?;
    }

    // Send agent.leave requests to those backends where the agent is connected to.
    let mut backend_ids = streams
        .iter()
        .map(|stream| stream.backend_id())
        .collect::<Vec<&AgentId>>();

    backend_ids.dedup();

    let backends = db::janus_backend::ListQuery::new()
        .ids(&backend_ids[..])
        .execute(&conn)?;

    for backend in backends {
        let result = context.janus_client().agent_leave_request(
            backend.session_id(),
            backend.handle_id(),
            &agent_id,
            backend.id(),
            tracking,
        );

        match result {
            Ok(req) => messages.push(Box::new(req)),
            Err(err) => {
                return Err(err.context("Error creating a backend request"))
                    .error(AppErrorKind::MessageBuildingFailed);
            }
        }
    }

    Ok((true, messages))
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod create_response {
        use svc_agent::mqtt::ResponseStatus;

        use crate::app::API_VERSION;
        use crate::db::agent::{ListQuery as AgentListQuery, Status as AgentStatus};
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn create_subscription() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let room = {
                    // Create room.
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let room = shared_helpers::insert_room(&conn);

                    // Put agent in the room in `in_progress` status.
                    factory::Agent::new()
                        .room_id(room.id())
                        .agent_id(agent.agent_id())
                        .insert(&conn);

                    room
                };

                // Send subscription.create response.
                let mut context = TestContext::new(db, TestAuthz::new());
                let reqp = build_reqp(agent.agent_id(), "room.enter");
                let room_id = room.id().to_string();

                let corr_data = CorrelationDataPayload {
                    reqp: reqp.clone(),
                    subject: agent.agent_id().to_owned(),
                    object: vec!["rooms".to_string(), room_id, "events".to_string()],
                };

                let broker_account_label = context.config().broker_id.label();
                let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

                let messages = handle_response::<CreateResponseHandler>(
                    &mut context,
                    &broker,
                    CreateDeleteResponsePayload {},
                    &corr_data,
                )
                .await
                .expect("Subscription creation failed");

                // Assert original request response.
                let (_payload, respp, topic) =
                    find_response::<CreateDeleteResponsePayload>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    agent.agent_id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, &expected_topic);
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(respp.correlation_data(), reqp.correlation_data());

                // Assert notification.
                let (payload, evp, topic) = find_event::<RoomEnterLeaveEvent>(messages.as_slice());
                assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
                assert_eq!(evp.label(), "room.enter");
                assert_eq!(payload.id, room.id());
                assert_eq!(&payload.agent_id, agent.agent_id());

                // Assert agent turned to `ready` status.
                let conn = context.get_conn().expect("Failed to get DB connection");

                let db_agents = AgentListQuery::new()
                    .agent_id(agent.agent_id())
                    .room_id(room.id())
                    .execute(&conn)
                    .expect("Failed to execute agent list query");

                let db_agent = db_agents.first().expect("Missing agent in the DB");
                assert_eq!(db_agent.status(), AgentStatus::Ready);
            });
        }

        #[test]
        fn create_subscription_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let room_id = Uuid::new_v4().to_string();

                let corr_data = CorrelationDataPayload {
                    reqp: build_reqp(agent.agent_id(), "room.enter"),
                    subject: agent.agent_id().to_owned(),
                    object: vec!["rooms".to_string(), room_id, "events".to_string()],
                };

                let broker_account_label = context.config().broker_id.label();
                let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

                let err = handle_response::<CreateResponseHandler>(
                    &mut context,
                    &broker,
                    CreateDeleteResponsePayload {},
                    &corr_data,
                )
                .await
                .expect_err("Unexpected success on subscription creation");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }

        #[test]
        fn create_subscription_closed_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    shared_helpers::insert_closed_room(&conn)
                };

                let mut context = TestContext::new(db, TestAuthz::new());
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = room.id().to_string();

                let corr_data = CorrelationDataPayload {
                    reqp: build_reqp(agent.agent_id(), "room.enter"),
                    subject: agent.agent_id().to_owned(),
                    object: vec!["rooms".to_string(), room_id, "events".to_string()],
                };

                let broker_account_label = context.config().broker_id.label();
                let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

                let err = handle_response::<CreateResponseHandler>(
                    &mut context,
                    &broker,
                    CreateDeleteResponsePayload {},
                    &corr_data,
                )
                .await
                .expect_err("Unexpected success on subscription creation");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_closed");
            });
        }
    }

    mod delete_response {
        use svc_agent::mqtt::ResponseStatus;

        use crate::app::API_VERSION;
        use crate::db::agent::ListQuery as AgentListQuery;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn delete_subscription() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let room = {
                    // Create room and put the agent online.
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let room = shared_helpers::insert_room(&conn);
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    room
                };

                // Send subscription.delete response.
                let mut context = TestContext::new(db, TestAuthz::new());
                let reqp = build_reqp(agent.agent_id(), "room.leave");
                let room_id = room.id().to_string();

                let corr_data = CorrelationDataPayload {
                    reqp: reqp.clone(),
                    subject: agent.agent_id().to_owned(),
                    object: vec!["rooms".to_string(), room_id, "events".to_string()],
                };

                let broker_account_label = context.config().broker_id.label();
                let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

                let messages = handle_response::<DeleteResponseHandler>(
                    &mut context,
                    &broker,
                    CreateDeleteResponsePayload {},
                    &corr_data,
                )
                .await
                .expect("Subscription deletion failed");

                // Assert original request response.
                let (_payload, respp, topic) =
                    find_response::<CreateDeleteResponsePayload>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    agent.agent_id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, &expected_topic);
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(respp.correlation_data(), reqp.correlation_data());

                // Assert notification.
                let (payload, evp, topic) = find_event::<RoomEnterLeaveEvent>(messages.as_slice());
                assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
                assert_eq!(evp.label(), "room.leave");
                assert_eq!(payload.id, room.id());
                assert_eq!(&payload.agent_id, agent.agent_id());

                // Assert agent deleted from the DB.
                let conn = context.get_conn().expect("Failed to get DB connection");

                let db_agents = AgentListQuery::new()
                    .agent_id(agent.agent_id())
                    .room_id(room.id())
                    .execute(&conn)
                    .expect("Failed to execute agent list query");

                assert_eq!(db_agents.len(), 0);
            });
        }

        #[test]
        fn delete_subscription_missing_agent() {
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

                let mut context = TestContext::new(db, TestAuthz::new());
                let room_id = room.id().to_string();

                let corr_data = CorrelationDataPayload {
                    reqp: build_reqp(agent.agent_id(), "room.leave"),
                    subject: agent.agent_id().to_owned(),
                    object: vec!["rooms".to_string(), room_id, "events".to_string()],
                };

                let broker_account_label = context.config().broker_id.label();
                let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

                let err = handle_response::<DeleteResponseHandler>(
                    &mut context,
                    &broker,
                    CreateDeleteResponsePayload {},
                    &corr_data,
                )
                .await
                .expect_err("Unexpected success on subscription deletion");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "agent_not_entered_the_room");
            });
        }

        #[test]
        fn delete_subscription_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let room_id = Uuid::new_v4().to_string();

                let corr_data = CorrelationDataPayload {
                    reqp: build_reqp(agent.agent_id(), "room.leave"),
                    subject: agent.agent_id().to_owned(),
                    object: vec!["rooms".to_string(), room_id, "events".to_string()],
                };

                let broker_account_label = context.config().broker_id.label();
                let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

                let err = handle_response::<DeleteResponseHandler>(
                    &mut context,
                    &broker,
                    CreateDeleteResponsePayload {},
                    &corr_data,
                )
                .await
                .expect_err("Unexpected success on subscription deletion");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "agent_not_entered_the_room");
            });
        }
    }

    mod delete_event {
        use crate::db::agent::ListQuery as AgentListQuery;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn delete_subscription() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let room = {
                    // Create room and put the agent online.
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let room = shared_helpers::insert_room(&conn);
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    room
                };

                // Send subscription.delete event.
                let mut context = TestContext::new(db, TestAuthz::new());
                let room_id = room.id().to_string();

                let payload = DeleteEventPayload {
                    subject: agent.agent_id().to_owned(),
                    object: vec!["rooms".to_string(), room_id, "events".to_string()],
                };

                let broker_account_label = context.config().broker_id.label();
                let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

                let messages = handle_event::<DeleteEventHandler>(&mut context, &broker, payload)
                    .await
                    .expect("Subscription deletion failed");

                // Assert notification.
                let (payload, evp, topic) = find_event::<RoomEnterLeaveEvent>(messages.as_slice());
                assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
                assert_eq!(evp.label(), "room.leave");
                assert_eq!(payload.id, room.id());
                assert_eq!(&payload.agent_id, agent.agent_id());

                // Assert agent deleted from the DB.
                let conn = context.get_conn().expect("Failed to get DB connection");

                let db_agents = AgentListQuery::new()
                    .agent_id(agent.agent_id())
                    .room_id(room.id())
                    .execute(&conn)
                    .expect("Failed to execute agent list query");

                assert_eq!(db_agents.len(), 0);
            });
        }

        #[test]
        fn delete_subscription_missing_agent() {
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

                let mut context = TestContext::new(db, TestAuthz::new());
                let room_id = room.id().to_string();

                let payload = DeleteEventPayload {
                    subject: agent.agent_id().to_owned(),
                    object: vec!["rooms".to_string(), room_id, "events".to_string()],
                };

                let broker_account_label = context.config().broker_id.label();
                let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

                let messages = handle_event::<DeleteEventHandler>(&mut context, &broker, payload)
                    .await
                    .expect("Subscription deletion failed");

                assert!(messages.is_empty());
            });
        }
    }
}
