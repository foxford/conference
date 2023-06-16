use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
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

use crate::{
    app::{context::Context, endpoint::prelude::*, metrics::HistogramExt},
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

///////////////////////////////////////////////////////////////////////////////

pub struct DeleteResponseHandler;

#[async_trait]
impl ResponseHandler for DeleteResponseHandler {
    type Payload = CreateDeleteResponsePayload;
    type CorrelationData = CorrelationDataPayload;

    #[instrument(skip(context, _payload, respp, corr_data))]
    async fn handle<C: Context + Send + Sync>(
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
                &format!("rooms/{room_id}/events"),
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

    async fn handle<C: Context + Send + Sync>(
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
            let to_uri = format!("rooms/{room_id}/events");
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
    let mut conn = context.get_conn().await?;
    let row_count = db::agent::DeleteQuery::new()
        .agent_id(agent_id)
        // in theory we should delete agent row only for this room id
        //
        // but right now broker doesnt send a subscription.delete event when
        // someone connects kicking out previous connection
        // (for example when you enter one p2p room and then another,
        //      you will get session_taken_over in old tab, but `agent` row for the first room remains intact)
        // this leads to non existent subscriptions still present in agent table
        //
        // this fix isnt correct since we have multiple brokers
        // and connecting to one broker doesnt interrupt connection to another
        // so we need to delete only those `agent` rows that have rooms subscriptions on the same broker
        // but we cant differentiate between room types here
        //
        // .room_id(room_id)
        .execute(&mut conn)
        .await?;

    let left = if row_count < 1 {
        false
    } else {
        make_orphaned_if_host_left(room_id, agent_id, &mut conn).await?;

        true
    };

    Ok(left)
}

async fn make_orphaned_if_host_left(
    room_id: db::room::Id,
    agent_left: &AgentId,
    connection: &mut sqlx::PgConnection,
) -> sqlx::Result<()> {
    let room = db::room::FindQuery::new(room_id)
        .execute(connection)
        .await?;

    if room.as_ref().and_then(|x| x.host()) == Some(agent_left) {
        db::orphaned_room::upsert_room(room_id, Utc::now(), connection).await?;
    }
    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod delete_response {
        use svc_agent::mqtt::ResponseStatus;

        use crate::{
            app::API_VERSION,
            db::agent::ListQuery as AgentListQuery,
            test_helpers::{db::TestDb, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn delete_subscription() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;

            // Create room and put the agent online.
            let room = shared_helpers::insert_room(&mut conn).await;

            let mut conn_sqlx = db.get_conn().await;
            shared_helpers::insert_agent(&mut conn_sqlx, agent.agent_id(), room.id()).await;

            // Send subscription.delete response.
            let mut context = TestContext::new(db, TestAuthz::new()).await;
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
            let mut conn = context
                .get_conn()
                .await
                .expect("Failed to get DB connection");

            let db_agents = AgentListQuery::new()
                .agent_id(agent.agent_id())
                .room_id(room.id())
                .execute(&mut conn)
                .await
                .expect("Failed to execute agent list query");

            assert_eq!(db_agents.len(), 0);
        }

        #[tokio::test]
        async fn delete_subscription_missing_agent() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let mut context = TestContext::new(db, TestAuthz::new()).await;
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
            let db = TestDb::with_local_postgres(&postgres).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new()).await;
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
            test_helpers::{db::TestDb, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn delete_subscription() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;

            // First room, we were online in it but then session was taken over and we disconnected (not in the db tho).
            // By the end of this test subscription for this room should be absent.
            let old_room = shared_helpers::insert_room(&mut conn).await;
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), old_room.id()).await;

            // Create room and put the agent online.
            let room = shared_helpers::insert_room(&mut conn).await;
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

            // Send subscription.delete event.
            let mut context = TestContext::new(db, TestAuthz::new()).await;
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
            let mut conn = context
                .get_conn()
                .await
                .expect("Failed to get DB connection");

            let db_agents = AgentListQuery::new()
                .agent_id(agent.agent_id())
                .room_id(room.id())
                .execute(&mut conn)
                .await
                .expect("Failed to execute agent list query");

            assert_eq!(db_agents.len(), 0);

            let db_agents = AgentListQuery::new()
                .agent_id(agent.agent_id())
                .room_id(old_room.id())
                .execute(&mut conn)
                .await
                .expect("Failed to execute agent list query");

            assert_eq!(db_agents.len(), 0);
        }

        #[tokio::test]
        async fn delete_subscription_missing_agent() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let mut context = TestContext::new(db, TestAuthz::new()).await;
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
