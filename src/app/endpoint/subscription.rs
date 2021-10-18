use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use diesel::PgConnection;
use futures::stream;
use serde::{Deserialize, Serialize};
use std::result::Result as StdResult;
use svc_agent::{
    mqtt::{
        IncomingEventProperties, IntoPublishableMessage, OutgoingEvent, ShortTermTimingProperties,
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
    let left = crate::util::spawn_blocking({
        let agent_id = agent_id.clone();
        move || {
            let row_count = db::agent::DeleteQuery::new()
                .agent_id(&agent_id)
                .room_id(room_id)
                .execute(&conn)?;

            if row_count != 1 {
                return Ok::<_, AppError>(false);
            }

            make_orphaned_if_host_left(room_id, &agent_id, &conn)?;
            Ok::<_, AppError>(true)
        }
    })
    .await?;
    Ok(left)
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
