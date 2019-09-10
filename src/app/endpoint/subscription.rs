use std::str::FromStr;

use serde_derive::Deserialize;
use svc_agent::mqtt::{IncomingEvent, IncomingEventProperties, Publishable, ResponseStatus};
use svc_agent::AgentId;
use svc_authn::Authenticable;
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::db::{agent, room, ConnectionPool};

///////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateDeleteEvent = IncomingEvent<CreateDeleteEventData>;

#[derive(Deserialize)]
pub(crate) struct CreateDeleteEventData {
    object: Vec<String>,
    subject: String,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub(crate) struct State {
    broker_account_id: svc_agent::AccountId,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(broker_account_id: svc_agent::AccountId, db: ConnectionPool) -> Self {
        Self {
            broker_account_id,
            db,
        }
    }
}

impl State {
    pub(crate) async fn create(
        &self,
        evt: CreateDeleteEvent,
    ) -> Result<Vec<Box<dyn Publishable>>, SvcError> {
        self.check_sender_account_id(&evt.properties())?;

        let agent_id = parse_agent_id(&evt)?;
        let room_id = parse_room_id(&evt)?;

        let conn = self.db.get()?;

        room::FindQuery::new()
            .time(room::upto_now())
            .id(room_id)
            .execute(&conn)?
            .ok_or_else(|| {
                SvcError::builder()
                    .status(ResponseStatus::NOT_FOUND)
                    .detail(&format!("the room = '{}' is not found", room_id))
                    .build()
            })?;

        agent::InsertQuery::new(&agent_id, room_id).execute(&conn)?;
        Ok(vec![])
    }

    pub(crate) async fn delete(
        &self,
        evt: CreateDeleteEvent,
    ) -> Result<Vec<Box<dyn Publishable>>, SvcError> {
        self.check_sender_account_id(&evt.properties())?;

        let agent_id = parse_agent_id(&evt)?;
        let room_id = parse_room_id(&evt)?;

        let conn = self.db.get()?;
        let row_count = agent::DeleteQuery::new(&agent_id, room_id).execute(&conn)?;

        if row_count == 1 {
            Ok(vec![])
        } else {
            let err = format!(
                "the agent is not found for agent_id = '{}', room = '{}'",
                agent_id, room_id
            );

            Err(SvcError::builder()
                .status(ResponseStatus::NOT_FOUND)
                .detail(&err)
                .build())
        }
    }

    fn check_sender_account_id(&self, evp: &IncomingEventProperties) -> Result<(), SvcError> {
        // Authorization: sender's account id = broker id
        if evp.as_account_id() == &self.broker_account_id {
            Ok(())
        } else {
            let err = SvcError::builder()
                .status(ResponseStatus::FORBIDDEN)
                .detail("Forbidden")
                .build();

            return Err(err);
        }
    }
}

fn parse_agent_id(evt: &CreateDeleteEvent) -> Result<AgentId, SvcError> {
    AgentId::from_str(&evt.payload().subject).map_err(|err| {
        SvcError::builder()
            .status(ResponseStatus::BAD_REQUEST)
            .detail(&format!("Failed to parse subject agent id: {}", err))
            .build()
    })
}

fn parse_room_id(evt: &CreateDeleteEvent) -> Result<Uuid, SvcError> {
    let object: Vec<&str> = evt.payload().object.iter().map(AsRef::as_ref).collect();

    let result = match object.as_slice() {
        ["room", room_id, "events"] => {
            Uuid::parse_str(room_id).map_err(|err| format!("UUID parse error: {}", err))
        }
        _ => Err(String::from(
            "Bad 'object' format; expected [\"room\", <ROOM_ID>, \"events\"]",
        )),
    };

    result.map_err(|err| {
        SvcError::builder()
            .status(ResponseStatus::BAD_REQUEST)
            .detail(&err)
            .build()
    })
}

#[cfg(test)]
mod test {
    use diesel::prelude::*;
    use failure::format_err;
    use serde_json::json;

    use crate::db::agent::Object as Agent;
    use crate::schema::agent::dsl::*;
    use crate::test_helpers::{agent::TestAgent, db::TestDb, factory, factory::insert_room};

    use super::*;

    const AUDIENCE: &str = "dev.svc.example.org";

    fn build_state(db: &TestDb) -> State {
        let account_id = svc_agent::AccountId::new("mqtt-gateway", AUDIENCE);
        State::new(account_id, db.connection_pool().clone())
    }

    #[test]
    fn create_subscription() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Insert room.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| insert_room(&conn, AUDIENCE))
                .unwrap();

            // Send subscription.create event.
            let user_agent = TestAgent::new("web", "user_agent", AUDIENCE);

            let payload = json!({
                "object": vec!["room", &room.id().to_string(), "events"],
                "subject": user_agent.agent_id().to_string(),
            });

            let broker_agent = TestAgent::new("alpha", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);
            state.create(event).await.unwrap();

            // Assert agent presence in the DB.
            let conn = db.connection_pool().get().unwrap();

            let db_agent: Agent = agent
                .filter(agent_id.eq(user_agent.agent_id()))
                .get_result(&conn)
                .unwrap();

            assert_eq!(db_agent.room_id(), room.id());
        });
    }

    #[test]
    fn create_subscription_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Send subscription.create event.
            let user_agent = TestAgent::new("web", "user_agent", AUDIENCE);

            let payload = json!({
                "object": vec!["room", &Uuid::new_v4().to_string(), "events"],
                "subject": user_agent.agent_id().to_string(),
            });

            let broker_agent = TestAgent::new("web", "wrong_user", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 403 error.
            match state.create(event).await {
                Ok(_) => panic!("Expected subscription.create to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    #[test]
    fn create_subscription_missing_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Send subscription.create event.
            let user_agent = TestAgent::new("web", "user_agent", AUDIENCE);

            let payload = json!({
                "object": vec!["room", &Uuid::new_v4().to_string(), "events"],
                "subject": user_agent.agent_id().to_string(),
            });

            let broker_agent = TestAgent::new("web", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 404 error.
            match state.create(event).await {
                Ok(_) => panic!("Expected subscription.create to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn create_subscription_bad_object() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Send subscription.create event.
            let user_agent = TestAgent::new("web", "user_agent", AUDIENCE);

            let payload = json!({
                "object": vec!["wrong"],
                "subject": user_agent.agent_id().to_string(),
            });

            let broker_agent = TestAgent::new("web", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 400 error.
            match state.create(event).await {
                Ok(_) => panic!("Expected subscription.create to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::BAD_REQUEST),
            }
        });
    }

    #[test]
    fn create_subscription_bad_subject() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Send subscription.create event.
            let payload = json!({
                "object": vec!["room", &Uuid::new_v4().to_string(), "events"],
                "subject": "wrong",
            });

            let broker_agent = TestAgent::new("web", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 400 error.
            match state.create(event).await {
                Ok(_) => panic!("Expected subscription.create to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::BAD_REQUEST),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn delete_subscription() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Insert agent.
            let db_agent = db
                .connection_pool()
                .get()
                .map_err(|err| format_err!("Failed to get DB connection: {}", err))
                .and_then(|conn| factory::Agent::new().audience(AUDIENCE).insert(&conn))
                .expect("Failed to insert agent");

            // Send subscription.delete event.
            let payload = json!({
                "object": vec!["room", &db_agent.room_id().to_string(), "events"],
                "subject": db_agent.agent_id().to_string(),
            });

            let broker_agent = TestAgent::new("alpha", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.delete", &payload)
                .unwrap();

            let state = build_state(&db);
            state.delete(event).await.unwrap();

            // Assert agent absence in the DB.
            let conn = db.connection_pool().get().unwrap();
            let query = agent.filter(agent_id.eq(db_agent.agent_id()));
            assert_eq!(query.execute(&conn).unwrap(), 0);
        });
    }

    #[test]
    fn delete_subscription_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Send subscription.create event.
            let user_agent = TestAgent::new("web", "user_agent", AUDIENCE);

            let payload = json!({
                "object": vec!["room", &Uuid::new_v4().to_string(), "events"],
                "subject": user_agent.agent_id().to_string(),
            });

            let broker_agent = TestAgent::new("web", "wrong_user", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 403 error.
            match state.delete(event).await {
                Ok(_) => panic!("Expected subscription.delete to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    #[test]
    fn delete_subscription_missing_agent() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Send subscription.create event.
            let user_agent = TestAgent::new("web", "user_agent", AUDIENCE);

            let payload = json!({
                "object": vec!["room", &Uuid::new_v4().to_string(), "events"],
                "subject": user_agent.agent_id().to_string(),
            });

            let broker_agent = TestAgent::new("web", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 404 error.
            match state.delete(event).await {
                Ok(_) => panic!("Expected subscription.delete to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn delete_subscription_bad_object() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Send subscription.create event.
            let user_agent = TestAgent::new("web", "user_agent", AUDIENCE);

            let payload = json!({
                "object": vec!["wrong"],
                "subject": user_agent.agent_id().to_string(),
            });

            let broker_agent = TestAgent::new("web", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 400 error.
            match state.delete(event).await {
                Ok(_) => panic!("Expected subscription.delete to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::BAD_REQUEST),
            }
        });
    }

    #[test]
    fn delete_subscription_bad_subject() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Send subscription.create event.
            let payload = json!({
                "object": vec!["room", &Uuid::new_v4().to_string(), "events"],
                "subject": "wrong",
            });

            let broker_agent = TestAgent::new("web", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 400 error.
            match state.delete(event).await {
                Ok(_) => panic!("Expected subscription.delete to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::BAD_REQUEST),
            }
        });
    }
}
