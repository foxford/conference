use serde_derive::Deserialize;
use svc_agent::mqtt::{IncomingRequest, ResponseStatus};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
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

    pub(crate) async fn list(&self, inreq: ListRequest) -> endpoint::Result {
        let room_id = inreq.payload().room_id;

        // Authorization: room's owner has to allow the action
        {
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
            self.authz.authorize(
                room.audience(),
                inreq.properties(),
                vec!["rooms", &room_id, "agents"],
                "list",
            )?;
        }

        let objects = {
            let conn = self.db.get()?;

            agent::ListQuery::from((
                Some(room_id),
                inreq.payload().offset,
                Some(std::cmp::min(
                    inreq.payload().limit.unwrap_or_else(|| MAX_LIMIT),
                    MAX_LIMIT,
                )),
            ))
            .execute(&conn)?
        };

        inreq.to_response(objects, ResponseStatus::OK).into()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Try;

    use failure::format_err;
    use serde_json::json;
    use svc_agent::AgentId;

    use crate::test_helpers::{agent::TestAgent, db::TestDb, extract_payload, factory, no_authz};

    use super::*;

    const AUDIENCE: &str = "dev.svc.example.org";

    fn build_state(db: &TestDb) -> State {
        State::new(no_authz(AUDIENCE), db.connection_pool().clone())
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct AgentResponse {
        id: Uuid,
        agent_id: AgentId,
        room_id: Uuid,
        created_at: i64,
    }

    #[test]
    fn list_agents() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Insert online agent.
            let online_agent = db
                .connection_pool()
                .get()
                .map_err(|err| format_err!("Failed to get DB connection: {}", err))
                .and_then(|conn| {
                    let agent_factory = factory::Agent::new().audience(AUDIENCE);
                    let agent = agent_factory.insert(&conn)?;
                    let _other_agent = agent_factory.insert(&conn)?;
                    Ok(agent)
                })
                .expect("Failed to insert online agent");

            // Make agent.list request.
            let state = build_state(&db);
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({"room_id": online_agent.room_id()});
            let request: ListRequest = agent.build_request("agent.list", &payload).unwrap();
            let mut result = state.list(request).await.into_result().unwrap();
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
}
