use async_trait::async_trait;
use chrono::Utc;
use diesel::PgConnection;
use futures::stream;
use serde::{Deserialize, Serialize};
use std::result::Result as StdResult;
use svc_agent::{
    mqtt::{IncomingRequestProperties, IncomingResponseProperties},
    AgentId,
};

use crate::{
    app::{context::Context, endpoint::prelude::*},
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

    #[instrument(skip(_context, _payload, _respp, _corr_data))]
    async fn handle<C: Context>(
        _context: &mut C,
        _payload: Self::Payload,
        _respp: &IncomingResponseProperties,
        _corr_data: &Self::CorrelationData,
    ) -> MqttResult {
        Ok(Box::new(stream::empty()))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[instrument(skip(context))]
pub async fn leave_room<C: Context>(
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

            if row_count < 1 {
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
}
