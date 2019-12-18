use chrono::{DateTime, Utc};
use serde_derive::Deserialize;
use svc_agent::mqtt::{IncomingRequest, ResponseStatus, ShortTermTimingProperties};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::{endpoint, API_VERSION};
use crate::db::{agent, room, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 25;

////////////////////////////////////////////////////////////////////////////////

pub(crate) type ListRequest = IncomingRequest<ListRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ListRequestData {
    room_id: Uuid,
    offset: Option<i64>,
    limit: Option<i64>,
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(authz: svc_authz::ClientMap, db: ConnectionPool) -> Self {
        Self { authz, db }
    }

    pub(crate) async fn list(
        &self,
        inreq: ListRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        let room_id = inreq.payload().room_id;

        // Authorization: room's owner has to allow the action
        let authz_time = {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
                .time(room::now())
                .id(room_id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("the room = '{}' is not found", &room_id))
                        .build()
                })?;

            let room_id = room.id().to_string();

            self.authz
                .authorize(
                    room.audience(),
                    inreq.properties(),
                    vec!["rooms", &room_id, "agents"],
                    "list",
                )
                .await
                .map_err(|err| SvcError::from(err))?
        };

        let objects = {
            let conn = self.db.get()?;

            agent::ListQuery::from((
                Some(room_id),
                inreq.payload().offset,
                Some(std::cmp::min(
                    inreq.payload().limit.unwrap_or_else(|| MAX_LIMIT),
                    MAX_LIMIT,
                )),
                Some(agent::Status::Ready),
            ))
            .execute(&conn)?
        };

        let mut timing = ShortTermTimingProperties::until_now(start_timestamp);
        timing.set_authorization_time(authz_time);

        inreq
            .to_response(objects, ResponseStatus::OK, timing, API_VERSION)
            .into()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Try;

    use failure::format_err;
    use serde_json::json;
    use svc_agent::{mqtt::ResponseStatus, AgentId};

    use crate::test_helpers::{
        agent::TestAgent,
        authz::{no_authz, TestAuthz},
        db::TestDb,
        extract_payload, factory,
    };

    use super::*;

    const AUDIENCE: &str = "dev.svc.example.org";

    #[derive(Debug, PartialEq, Deserialize)]
    struct AgentResponse {
        id: Uuid,
        agent_id: AgentId,
        room_id: Uuid,
        created_at: i64,
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn list_agents() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert online agent.
            let (room, online_agent) = db
                .connection_pool()
                .get()
                .map_err(|err| format_err!("Failed to get DB connection: {}", err))
                .and_then(|conn| {
                    let room = factory::insert_room(&conn, AUDIENCE);

                    let agent = factory::Agent::new()
                        .audience(AUDIENCE)
                        .room_id(room.id())
                        .insert(&conn)?;

                    let _other_agent = factory::Agent::new().audience(AUDIENCE).insert(&conn)?;

                    Ok((room, agent))
                })
                .expect("Failed to insert online agent");

            // Allow room agents listing for the agent.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = room.id().to_string();
            let object = vec!["rooms", &room_id, "agents"];
            authz.allow(agent.account_id(), object, "list");

            // Make agent.list request.
            let state = State::new(authz.into(), db.connection_pool().clone());
            let payload = json!({"room_id": online_agent.room_id()});
            let request: ListRequest = agent.build_request("agent.list", &payload).unwrap();
            let mut result = state.list(request, Utc::now()).await.into_result().unwrap();
            let message = result.remove(0);

            // Assert response.
            let resp: Vec<AgentResponse> = extract_payload(message).unwrap();
            assert_eq!(resp.len(), 1);

            assert_eq!(
                *resp.first().unwrap(),
                AgentResponse {
                    id: online_agent.id(),
                    agent_id: online_agent.agent_id().to_owned(),
                    room_id: online_agent.room_id(),
                    created_at: online_agent.created_at().timestamp(),
                }
            );
        });
    }

    #[test]
    fn list_agents_in_missing_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make agent.list request.
            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "room_id": Uuid::new_v4() });
            let request: ListRequest = agent.build_request("agent.list", &payload).unwrap();
            let result = state.list(request, Utc::now()).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected agent.list to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn list_agent_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            let room = db
                .connection_pool()
                .get()
                .map(|conn| factory::insert_room(&conn, AUDIENCE))
                .unwrap();

            // Make agent.list request.
            let state = State::new(authz.into(), db.connection_pool().clone());
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({"room_id": room.id()});
            let request: ListRequest = agent.build_request("agent.list", &payload).unwrap();
            let result = state.list(request, Utc::now()).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected agent.list to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }
}
