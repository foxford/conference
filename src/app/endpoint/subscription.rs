use std::result::Result as StdResult;

use async_std::stream;
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        IncomingEventProperties, IntoPublishableMessage, OutgoingEvent, ShortTermTimingProperties,
    },
    Addressable, AgentId, Authenticable,
};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::db;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct SubscriptionEvent {
    subject: AgentId,
    object: Vec<String>,
}

impl SubscriptionEvent {
    fn try_room_id(&self) -> StdResult<Uuid, AppError> {
        let object: Vec<&str> = self.object.iter().map(AsRef::as_ref).collect();

        match object.as_slice() {
            ["rooms", room_id, "events"] => {
                Uuid::parse_str(room_id).map_err(|err| anyhow!("UUID parse error: {}", err))
            }
            _ => Err(anyhow!(
                "Bad 'object' format; expected [\"room\", <ROOM_ID>, \"events\"]",
            )),
        }
        .error(AppErrorKind::InvalidSubscriptionObject)
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

pub(crate) struct CreateHandler;

#[async_trait]
impl EventHandler for CreateHandler {
    type Payload = SubscriptionEvent;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
    ) -> Result {
        // Check if the event is sent by the broker.
        if evp.as_account_id() != &context.config().broker_id {
            return Err(anyhow!(
                "Expected subscription.create event to be sent from the broker account '{}'",
                context.config().broker_id,
            ))
            .error(AppErrorKind::AccessDenied);
        }

        // Find room.
        let room_id = payload.try_room_id()?;

        context.add_logger_tags(o!(
            "agent_label" => evp.as_agent_id().label().to_owned(),
            "account_label" => evp.as_account_id().label().to_owned(),
            "audience" => evp.as_account_id().audience().to_owned(),
        ));

        helpers::find_room_by_id(context, room_id, helpers::RoomTimeRequirement::NotClosed)?;

        {
            let conn = context.get_conn()?;

            // Update agent state to `ready`.
            db::agent::UpdateQuery::new(&payload.subject, room_id)
                .status(db::agent::Status::Ready)
                .execute(&conn)?;
        }

        // Send broadcast notification that the agent has entered the room.
        let outgoing_event_payload = RoomEnterLeaveEvent::new(room_id.to_owned(), payload.subject);
        let short_term_timing = ShortTermTimingProperties::until_now(context.start_timestamp());
        let props = evp.to_event("room.enter", short_term_timing);
        let to_uri = format!("rooms/{}/events", room_id);
        let outgoing_event = OutgoingEvent::broadcast(outgoing_event_payload, props, &to_uri);
        let boxed_event = Box::new(outgoing_event) as Box<dyn IntoPublishableMessage + Send>;
        Ok(Box::new(stream::once(boxed_event)))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct DeleteHandler;

#[async_trait]
impl EventHandler for DeleteHandler {
    type Payload = SubscriptionEvent;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
    ) -> Result {
        // Check if the event is sent by the broker.
        if evp.as_account_id() != &context.config().broker_id {
            return Err(anyhow!(
                "Expected subscription.delete event to be sent from the broker account '{}'",
                context.config().broker_id
            ))
            .error(AppErrorKind::AccessDenied);
        }

        // Delete agent from the DB.
        let room_id = payload.try_room_id()?;
        context.add_logger_tags(o!("room_id" => room_id.to_string()));
        let conn = context.get_conn()?;

        let row_count = db::agent::DeleteQuery::new()
            .agent_id(&payload.subject)
            .room_id(room_id)
            .execute(&conn)?;

        if row_count == 1 {
            // Send broadcast notification that the agent has left the room.
            let outgoing_event_payload =
                RoomEnterLeaveEvent::new(room_id.to_owned(), payload.subject.to_owned());

            let short_term_timing = ShortTermTimingProperties::until_now(context.start_timestamp());
            let props = evp.to_event("room.leave", short_term_timing);
            let to_uri = format!("rooms/{}/events", room_id);
            let outgoing_event = OutgoingEvent::broadcast(outgoing_event_payload, props, &to_uri);
            let boxed_event = Box::new(outgoing_event) as Box<dyn IntoPublishableMessage + Send>;
            let mut messages = vec![boxed_event];

            // `agent.leave` requests to Janus instances that host active streams in this room.
            let backends = db::janus_backend::ActiveListQuery::new().execute(&conn)?;

            for backend in backends {
                let result = context.janus_client().agent_leave_request(
                    evp.to_owned(),
                    backend.session_id(),
                    backend.handle_id(),
                    &payload.subject,
                    backend.id(),
                    evp.tracking(),
                );

                match result {
                    Ok(req) => messages.push(Box::new(req)),
                    Err(err) => {
                        return Err(err.context("Error creating a backend request"))
                            .error(AppErrorKind::MessageBuildingFailed);
                    }
                }
            }

            Ok(Box::new(stream::from_iter(messages)))
        } else {
            Err(anyhow!("The agent is not found")).error(AppErrorKind::AgentNotEnteredTheRoom)
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use serde_derive::Deserialize;
    use svc_agent::{mqtt::ResponseStatus, AgentId};

    use crate::app::API_VERSION;
    use crate::db::agent::{ListQuery as AgentListQuery, Status as AgentStatus};
    use crate::test_helpers::prelude::*;

    use super::*;

    ///////////////////////////////////////////////////////////////////////////

    #[derive(Deserialize)]
    struct AgentLeaveRequest {
        janus: String,
        session_id: i64,
        handle_id: i64,
        body: AgentLeaveRequestBody,
    }

    #[derive(Deserialize)]
    struct AgentLeaveRequestBody {
        method: String,
        agent_id: AgentId,
    }

    #[test]
    fn create_subscription() {
        async_std::task::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                let room = shared_helpers::insert_room(&conn);

                // Put agent in the room in `in_progress` status.
                factory::Agent::new()
                    .room_id(room.id())
                    .agent_id(agent.agent_id())
                    .status(AgentStatus::InProgress)
                    .insert(&conn);

                room
            };

            // Send subscription.create event.
            let mut context = TestContext::new(db.clone(), TestAuthz::new());
            let room_id = room.id().to_string();

            let payload = SubscriptionEvent {
                subject: agent.agent_id().to_owned(),
                object: vec!["rooms".to_string(), room_id, "events".to_string()],
            };

            let broker_account_label = context.config().broker_id.label();
            let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

            let messages = handle_event::<CreateHandler>(&mut context, &broker, payload)
                .await
                .expect("Subscription creation failed");

            // Assert notification.
            let (payload, evp, topic) = find_event::<RoomEnterLeaveEvent>(messages.as_slice());
            assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
            assert_eq!(evp.label(), "room.enter");
            assert_eq!(payload.id, room.id());
            assert_eq!(&payload.agent_id, agent.agent_id());

            // Assert agent turned to `ready` status.
            let conn = db
                .connection_pool()
                .get()
                .expect("Failed to get DB connection");

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
            let db = TestDb::new();
            let mut context = TestContext::new(db, TestAuthz::new());
            let room_id = Uuid::new_v4().to_string();

            let payload = SubscriptionEvent {
                subject: agent.agent_id().to_owned(),
                object: vec!["rooms".to_string(), room_id, "events".to_string()],
            };

            let broker_account_label = context.config().broker_id.label();
            let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

            let err = handle_event::<CreateHandler>(&mut context, &broker, payload)
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

            let payload = SubscriptionEvent {
                subject: agent.agent_id().to_owned(),
                object: vec!["rooms".to_string(), room_id, "events".to_string()],
            };

            let broker_account_label = context.config().broker_id.label();
            let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

            let err = handle_event::<CreateHandler>(&mut context, &broker, payload)
                .await
                .expect_err("Unexpected success on subscription creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn delete_subscription() {
        async_std::task::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room and put the agent online.
                let room = shared_helpers::insert_room(&conn);
                shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                room
            };

            // Send subscription.delete event.
            let mut context = TestContext::new(db.clone(), TestAuthz::new());
            let room_id = room.id().to_string();

            let payload = SubscriptionEvent {
                subject: agent.agent_id().to_owned(),
                object: vec!["rooms".to_string(), room_id, "events".to_string()],
            };

            let broker_account_label = context.config().broker_id.label();
            let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

            let messages = handle_event::<DeleteHandler>(&mut context, &broker, payload)
                .await
                .expect("Subscription deletion failed");

            // Assert notification.
            let (payload, evp, topic) = find_event::<RoomEnterLeaveEvent>(messages.as_slice());
            assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
            assert_eq!(evp.label(), "room.leave");
            assert_eq!(payload.id, room.id());
            assert_eq!(&payload.agent_id, agent.agent_id());

            // Assert agent deleted from the DB.
            let conn = db
                .connection_pool()
                .get()
                .expect("Failed to get DB connection");

            let db_agents = AgentListQuery::new()
                .agent_id(agent.agent_id())
                .room_id(room.id())
                .execute(&conn)
                .expect("Failed to execute agent list query");

            assert_eq!(db_agents.len(), 0);
        });
    }

    #[test]
    fn delete_subscription_for_stream_writer() {
        async_std::task::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let (backend, room) = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room with rtc, backend and a started active stream.
                let backend = shared_helpers::insert_janus_backend(&conn);
                let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                shared_helpers::insert_connected_agent(&conn, agent.agent_id(), room.id());
                let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                let stream = factory::JanusRtcStream::new(USR_AUDIENCE)
                    .backend(&backend)
                    .rtc(&rtc)
                    .sent_by(agent.agent_id())
                    .insert(&conn);

                crate::db::janus_rtc_stream::start(stream.id(), &conn)
                    .expect("Failed to start janus rtc stream")
                    .expect("Janus rtc stream couldn't start");

                (backend, room)
            };

            // Send subscription.delete event for the writer.
            let mut context = TestContext::new(db.clone(), TestAuthz::new());
            let room_id = room.id().to_string();

            let payload = SubscriptionEvent {
                subject: agent.agent_id().to_owned(),
                object: vec!["rooms".to_string(), room_id, "events".to_string()],
            };

            let broker_account_label = context.config().broker_id.label();
            let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

            let messages = handle_event::<DeleteHandler>(&mut context, &broker, payload)
                .await
                .expect("Subscription deletion failed");

            // Assert broadcast notification.
            let (payload, evp, topic) = find_event::<RoomEnterLeaveEvent>(messages.as_slice());
            assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
            assert_eq!(evp.label(), "room.leave");
            assert_eq!(payload.id, room.id());
            assert_eq!(&payload.agent_id, agent.agent_id());

            // Assert `agent.leave` request to janus.
            let (payload, _reqp, topic) = find_request::<AgentLeaveRequest>(messages.as_slice());

            let expected_topic = format!(
                "agents/{}/api/{}/in/conference.{}",
                backend.id(),
                API_VERSION,
                SVC_AUDIENCE,
            );

            assert_eq!(topic, expected_topic);
            assert_eq!(payload.session_id, backend.session_id());
            assert_eq!(payload.handle_id, backend.handle_id());
            assert_eq!(payload.janus, "message");
            assert_eq!(payload.body.method, "agent.leave");
            assert_eq!(&payload.body.agent_id, agent.agent_id());

            // Assert agent deleted from the DB.
            let conn = db
                .connection_pool()
                .get()
                .expect("Failed to get DB connection");

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

            let mut context = TestContext::new(db.clone(), TestAuthz::new());
            let room_id = room.id().to_string();

            let payload = SubscriptionEvent {
                subject: agent.agent_id().to_owned(),
                object: vec!["rooms".to_string(), room_id, "events".to_string()],
            };

            let broker_account_label = context.config().broker_id.label();
            let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

            let err = handle_event::<DeleteHandler>(&mut context, &broker, payload)
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
            let db = TestDb::new();
            let mut context = TestContext::new(db.clone(), TestAuthz::new());
            let room_id = Uuid::new_v4().to_string();

            let payload = SubscriptionEvent {
                subject: agent.agent_id().to_owned(),
                object: vec!["rooms".to_string(), room_id, "events".to_string()],
            };

            let broker_account_label = context.config().broker_id.label();
            let broker = TestAgent::new("alpha", broker_account_label, SVC_AUDIENCE);

            let err = handle_event::<DeleteHandler>(&mut context, &broker, payload)
                .await
                .expect_err("Unexpected success on subscription deletion");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
        });
    }
}
