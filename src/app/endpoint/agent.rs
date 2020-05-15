use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_derive::Deserialize;
use svc_agent::mqtt::{IncomingRequestProperties, ResponseStatus};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::db;

///////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 25;

#[derive(Debug, Deserialize)]
pub(crate) struct ListRequest {
    room_id: Uuid,
    offset: Option<i64>,
    limit: Option<i64>,
}

pub(crate) struct ListHandler;

#[async_trait]
impl RequestHandler for ListHandler {
    type Payload = ListRequest;
    const ERROR_TITLE: &'static str = "Failed to list agents";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        // Check whether the room exists and open.
        let room = {
            let conn = context.db().get()?;

            db::room::FindQuery::new()
                .id(payload.room_id)
                .time(db::room::now())
                .execute(&conn)?
                .ok_or_else(|| format!("the room = '{}' is not found or closed", payload.room_id))
                .status(ResponseStatus::NOT_FOUND)?
        };

        // Authorize agents listing in the room.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "agents"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "list")
            .await?;

        // Get agents list in the room.
        let agents = {
            let conn = context.db().get()?;

            db::agent::ListQuery::new()
                .room_id(payload.room_id)
                .status(db::agent::Status::Ready)
                .offset(payload.offset.unwrap_or_else(|| 0))
                .limit(std::cmp::min(
                    payload.limit.unwrap_or_else(|| MAX_LIMIT),
                    MAX_LIMIT,
                ))
                .execute(&conn)?
        };

        // Respond with agents list.
        Ok(Box::new(stream::once(shared::build_response(
            ResponseStatus::OK,
            agents,
            reqp,
            start_timestamp,
            Some(authz_time),
        ))))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod list {
        use serde_derive::Deserialize;
        use svc_agent::AgentId;
        use uuid::Uuid;

        use crate::test_helpers::prelude::*;

        use super::super::*;

        ///////////////////////////////////////////////////////////////////////////

        #[derive(Deserialize)]
        struct Agent {
            agent_id: AgentId,
            room_id: Uuid,
        }

        #[test]
        fn list_agents() {
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

                // Allow agent to list agents in the room.
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "agents"],
                    "list",
                );

                // Make agent.list request.
                let context = TestContext::new(db, authz);

                let payload = ListRequest {
                    room_id: room.id(),
                    offset: None,
                    limit: None,
                };

                let messages = handle_request::<ListHandler>(&context, &agent, payload)
                    .await
                    .expect("Agents listing failed");

                // Assert response.
                let (agents, respp) = find_response::<Vec<Agent>>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(agents.len(), 1);
                assert_eq!(&agents[0].agent_id, agent.agent_id());
                assert_eq!(agents[0].room_id, room.id());
            });
        }

        #[test]
        fn list_agents_not_authorized() {
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

                let context = TestContext::new(db, TestAuthz::new());

                let payload = ListRequest {
                    room_id: room.id(),
                    offset: None,
                    limit: None,
                };

                let err = handle_request::<ListHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on agents listing");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn list_agents_closed_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create closed room.
                    shared_helpers::insert_closed_room(&conn)
                };

                // Allow agent to list agents in the room.
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "agents"],
                    "list",
                );

                // Make agent.list request.
                let context = TestContext::new(db, authz);

                let payload = ListRequest {
                    room_id: room.id(),
                    offset: None,
                    limit: None,
                };

                let err = handle_request::<ListHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on agents listing");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }

        #[test]
        fn list_agents_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());

                let payload = ListRequest {
                    room_id: Uuid::new_v4(),
                    offset: None,
                    limit: None,
                };

                let err = handle_request::<ListHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on agents listing");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }
}
