use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use diesel::PgConnection;
use futures::stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::result::Result as StdResult;
use svc_agent::{
    mqtt::{
        IncomingEventProperties, IncomingRequestProperties, IncomingResponseProperties,
        IntoPublishableMessage, OutgoingEvent, ResponseStatus, ShortTermTimingProperties,
    },
    Addressable, AgentId,
};

use tracing::Span;

use crate::{
    app::{context::Context, endpoint::prelude::*, metrics::HistogramExt},
    backend::janus::client::agent_leave::{AgentLeaveRequest, AgentLeaveRequestBody},
    db::{self, room::FindQueryable},
};
use tracing_attributes::instrument;

use super::MqttResult;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub struct CorrelationDataPayload {
    reqp: IncomingRequestProperties,
    subject: AgentId,
    object: Vec<String>,
}

impl CorrelationDataPayload {
    pub fn new(reqp: IncomingRequestProperties, subject: AgentId, object: Vec<String>) -> Self {
        Self {
            reqp,
            subject,
            object,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct RoomEnterLeaveEvent {
    id: db::room::Id,
    agent_id: AgentId,
}

impl RoomEnterLeaveEvent {
    pub fn new(id: db::room::Id, agent_id: AgentId) -> Self {
        Self { id, agent_id }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct CreateDeleteResponsePayload {}

pub struct CreateResponseHandler;

#[async_trait]
impl ResponseHandler for CreateResponseHandler {
    type Payload = CreateDeleteResponsePayload;
    type CorrelationData = CorrelationDataPayload;

    #[instrument(skip(context, _payload, respp, corr_data), fields(room_id))]
    async fn handle<C: Context>(
        context: &mut C,
        _payload: Self::Payload,
        respp: &IncomingResponseProperties,
        corr_data: &Self::CorrelationData,
    ) -> MqttResult {
        ensure_broker(context, respp)?;

        // Find room.
        let room_id = try_room_id(&corr_data.object)?;
        let conn = context.get_conn().await?;
        let subject = corr_data.subject.clone();
        let room = crate::util::spawn_blocking(move || {
            let room =
                helpers::find_room_by_id(room_id, helpers::RoomTimeRequirement::NotClosed, &conn)?;
            if room.host() == Some(&subject) {
                db::orphaned_room::remove_room(room_id, &conn)?;
            }
            // Update agent state to `ready`.
            db::agent::UpdateQuery::new(&subject, room_id)
                .status(db::agent::Status::Ready)
                .execute(&conn)?;
            Ok::<_, AppError>(room)
        })
        .await?;
        Span::current().record("room_id", &room.id().to_string().as_str());

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
            corr_data.reqp.tracking(),
            context.start_timestamp(),
        );
        context
            .metrics()
            .request_duration
            .subscription_create
            .observe_timestamp(context.start_timestamp());

        Ok(Box::new(stream::iter(vec![response, notification])))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct DeleteResponseHandler;

#[async_trait]
impl ResponseHandler for DeleteResponseHandler {
    type Payload = CreateDeleteResponsePayload;
    type CorrelationData = CorrelationDataPayload;

    #[instrument(skip(context, _payload, respp, corr_data))]
    async fn handle<C: Context>(
        context: &mut C,
        _payload: Self::Payload,
        respp: &IncomingResponseProperties,
        corr_data: &Self::CorrelationData,
    ) -> MqttResult {
        ensure_broker(context, respp)?;
        let room_id = try_room_id(&corr_data.object)?;
        let maybe_left = leave_room(context, &corr_data.subject, room_id).await?;
        if maybe_left {
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
                corr_data.reqp.tracking(),
                context.start_timestamp(),
            );
            context
                .metrics()
                .request_duration
                .subscription_delete_response
                .observe_timestamp(context.start_timestamp());

            Ok(Box::new(stream::iter(vec![response, notification])))
        } else {
            Err(anyhow!("The agent is not found")).error(AppErrorKind::AgentNotEnteredTheRoom)
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DeleteEventPayload {
    subject: AgentId,
    object: Vec<String>,
}

pub struct DeleteEventHandler;

#[async_trait]
impl EventHandler for DeleteEventHandler {
    type Payload = DeleteEventPayload;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
    ) -> MqttResult {
        ensure_broker(context, evp)?;
        let room_id = try_room_id(&payload.object)?;
        if leave_room(context, &payload.subject, room_id).await? {
            let outgoing_event_payload =
                RoomEnterLeaveEvent::new(room_id, payload.subject.to_owned());
            let short_term_timing = ShortTermTimingProperties::until_now(context.start_timestamp());
            let props = evp.to_event("room.leave", short_term_timing);
            let to_uri = format!("rooms/{}/events", room_id);
            let outgoing_event = OutgoingEvent::broadcast(outgoing_event_payload, props, &to_uri);
            let notification =
                Box::new(outgoing_event) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
            context
                .metrics()
                .request_duration
                .subscription_delete_event
                .observe_timestamp(context.start_timestamp());

            Ok(Box::new(stream::once(std::future::ready(notification))))
        } else {
            Ok(Box::new(stream::empty()))
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

fn try_room_id(object: &[String]) -> StdResult<db::room::Id, AppError> {
    let object: Vec<&str> = object.iter().map(AsRef::as_ref).collect();

    match object.as_slice() {
        ["rooms", room_id, "events"] => room_id
            .parse()
            .map_err(|err| anyhow!("UUID parse error: {}", err)),
        _ => Err(anyhow!(
            "Bad 'object' format; expected [\"room\", <ROOM_ID>, \"events\"], got: {:?}",
            object
        )),
    }
    .error(AppErrorKind::InvalidSubscriptionObject)
}

#[instrument(skip(context))]
async fn leave_room<C: Context>(
    context: &mut C,
    agent_id: &AgentId,
    room_id: db::room::Id,
) -> StdResult<bool, AppError> {
    let conn = context.get_conn().await?;
    let backends = crate::util::spawn_blocking({
        let agent_id = agent_id.clone();

        move || {
            let row_count = db::agent::DeleteQuery::new()
                .agent_id(&agent_id)
                .room_id(room_id)
                .execute(&conn)?;

            if row_count != 1 {
                return Ok::<_, AppError>(None);
            }

            make_orphaned_if_host_left(room_id, &agent_id, &conn)?;

            // `agent.leave` requests to Janus instances that host active streams in this room.
            let streams = db::janus_rtc_stream::ListQuery::new()
                .room_id(room_id)
                .active(true)
                .execute(&conn)?;

            let mut maybe_stopped_rtc_id = None;

            for stream in streams.iter() {
                // If the agent is a publisher.
                if stream.sent_by() == &agent_id {
                    // Stop the stream.
                    db::janus_rtc_stream::stop(stream.id(), &conn)?;
                    maybe_stopped_rtc_id = Some(stream.rtc_id());
                }
            }

            // Disconnect stream readers since the stream has gone.
            if let Some(rtc_id) = maybe_stopped_rtc_id {
                db::agent_connection::BulkDisconnectByRtcQuery::new(rtc_id).execute(&conn)?;
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
            Ok::<_, AppError>(Some(backends))
        }
    })
    .await?;

    match backends {
        Some(backends) => Ok(true),
        None => Ok(false),
    }
}

fn make_orphaned_if_host_left(
    room_id: db::room::Id,
    agent_left: &AgentId,
    connection: &PgConnection,
) -> StdResult<(), diesel::result::Error> {
    let room = db::room::FindQuery::new(room_id).execute(connection)?;
    if room.as_ref().and_then(|x| x.host()) == Some(agent_left) {
        db::orphaned_room::upsert_room(room_id, Utc::now(), connection)?;
    }
    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod create_response {
        use svc_agent::mqtt::ResponseStatus;

        use crate::{
            app::API_VERSION,
            db::agent::{ListQuery as AgentListQuery, Status as AgentStatus},
            test_helpers::{prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn create_subscription() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

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
            let conn = context
                .get_conn()
                .await
                .expect("Failed to get DB connection");

            let db_agents = AgentListQuery::new()
                .agent_id(agent.agent_id())
                .room_id(room.id())
                .execute(&conn)
                .expect("Failed to execute agent list query");

            let db_agent = db_agents.first().expect("Missing agent in the DB");
            assert_eq!(db_agent.status(), AgentStatus::Ready);
        }

        #[tokio::test]
        async fn create_subscription_missing_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new());
            let room_id = db::room::Id::random().to_string();

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
        }

        #[tokio::test]
        async fn create_subscription_closed_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

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
        }
    }

    mod delete_response {
        use svc_agent::mqtt::ResponseStatus;

        use crate::{
            app::API_VERSION,
            db::agent::ListQuery as AgentListQuery,
            test_helpers::{prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn delete_subscription() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

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
            let conn = context
                .get_conn()
                .await
                .expect("Failed to get DB connection");

            let db_agents = AgentListQuery::new()
                .agent_id(agent.agent_id())
                .room_id(room.id())
                .execute(&conn)
                .expect("Failed to execute agent list query");

            assert_eq!(db_agents.len(), 0);
        }

        #[tokio::test]
        async fn delete_subscription_missing_agent() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

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
        }

        #[tokio::test]
        async fn delete_subscription_missing_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new());
            let room_id = db::room::Id::random().to_string();

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
        }
    }

    mod delete_event {
        use crate::{
            db::agent::ListQuery as AgentListQuery,
            test_helpers::{prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn delete_subscription() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

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
            let conn = context
                .get_conn()
                .await
                .expect("Failed to get DB connection");

            let db_agents = AgentListQuery::new()
                .agent_id(agent.agent_id())
                .room_id(room.id())
                .execute(&conn)
                .expect("Failed to execute agent list query");

            assert_eq!(db_agents.len(), 0);
        }

        #[tokio::test]
        async fn delete_subscription_missing_agent() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

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
        }
    }
}
