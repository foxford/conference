use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::mqtt::{
    Connection, IncomingEvent, IncomingEventProperties, OutgoingEvent, ResponseStatus,
    ShortTermTimingProperties,
};
use svc_agent::AgentId;
use svc_authn::Authenticable;
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
use crate::db::{agent, room, ConnectionPool};

///////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateDeleteEvent = IncomingEvent<CreateDeleteEventData>;

#[derive(Deserialize)]
pub(crate) struct CreateDeleteEventData {
    subject: Connection,
    object: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct RoomEnterLeaveEventData {
    id: Uuid,
    agent_id: AgentId,
}

impl RoomEnterLeaveEventData {
    fn new(id: Uuid, agent_id: AgentId) -> Self {
        Self { id, agent_id }
    }
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
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        self.is_broker(&evt.properties())?;

        let agent_id = evt.payload().subject.agent_id();
        let room_id = parse_room_id(&evt)?;

        let conn = self.db.get()?;

        room::FindQuery::new()
            .time(room::now())
            .id(room_id)
            .execute(&conn)?
            .ok_or_else(|| {
                SvcError::builder()
                    .status(ResponseStatus::NOT_FOUND)
                    .detail(&format!("the room = '{}' is not found", room_id))
                    .build()
            })?;

        agent::UpdateQuery::new(agent_id, room_id)
            .status(agent::Status::Ready)
            .execute(&conn)?;

        let payload = RoomEnterLeaveEventData::new(room_id.to_owned(), agent_id.to_owned());
        let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
        let props = evt.properties().to_event("room.enter", short_term_timing);
        let to_uri = format!("rooms/{}/events", room_id);
        OutgoingEvent::broadcast(payload, props, &to_uri).into()
    }

    pub(crate) async fn delete(
        &self,
        evt: CreateDeleteEvent,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        self.is_broker(&evt.properties())?;

        let agent_id = evt.payload().subject.agent_id();
        let room_id = parse_room_id(&evt)?;

        let conn = self.db.get()?;
        let row_count = agent::DeleteQuery::new(agent_id, room_id).execute(&conn)?;

        if row_count == 1 {
            let payload = RoomEnterLeaveEventData::new(room_id.to_owned(), agent_id.to_owned());
            let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
            let props = evt.properties().to_event("room.leave", short_term_timing);
            let to_uri = format!("rooms/{}/events", room_id);
            OutgoingEvent::broadcast(payload, props, &to_uri).into()
        } else {
            let err = format!(
                "the agent is not found for agent_id = '{}', room = '{}'",
                agent_id, room_id
            );

            SvcError::builder()
                .status(ResponseStatus::NOT_FOUND)
                .detail(&err)
                .build()
                .into()
        }
    }

    fn is_broker(&self, evp: &IncomingEventProperties) -> Result<(), SvcError> {
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

fn parse_room_id(evt: &CreateDeleteEvent) -> Result<Uuid, SvcError> {
    let object: Vec<&str> = evt.payload().object.iter().map(AsRef::as_ref).collect();

    let result = match object.as_slice() {
        ["rooms", room_id, "events"] => {
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
    use std::ops::Try;

    use diesel::prelude::*;
    use failure::format_err;
    use serde_json::json;
    use svc_agent::Destination;

    use crate::db::agent::Object as Agent;
    use crate::schema::agent::dsl::*;
    use crate::test_helpers::{
        agent::TestAgent, db::TestDb, extract_payload, factory, factory::insert_room,
    };

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
            let user_agent = TestAgent::new("web", "user_agent", AUDIENCE);

            // Insert room and agent in `in_progress` status.
            let room = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room = insert_room(&conn, AUDIENCE);

                    factory::Agent::new()
                        .audience(AUDIENCE)
                        .agent_id(user_agent.agent_id())
                        .room_id(room.id())
                        .status(crate::db::agent::Status::InProgress)
                        .insert(&conn)
                        .unwrap();

                    room
                })
                .unwrap();

            // Send subscription.create event.
            let payload = json!({
                "object": vec!["rooms", &room.id().to_string(), "events"],
                "subject": format!("v1/agents/{}", user_agent.agent_id()),
            });

            let broker_agent = TestAgent::new("alpha", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);
            let mut result = state.create(event, Utc::now()).await.into_result().unwrap();

            // Assert notification to the room topic.
            let message = result.remove(0);
            assert_eq!(message.message_type(), "event");

            match message.destination() {
                Destination::Broadcast(destination) => {
                    assert_eq!(destination, &format!("rooms/{}/events", room.id()))
                }
                _ => panic!("Expected broadcast destination"),
            }

            let payload: RoomEnterLeaveEventData = extract_payload(message).unwrap();
            assert_eq!(payload.id, room.id());
            assert_eq!(payload.agent_id, *user_agent.agent_id());

            // Assert agent presence in the DB.
            let conn = db.connection_pool().get().unwrap();

            let db_agent: Agent = agent
                .filter(agent_id.eq(user_agent.agent_id()))
                .get_result(&conn)
                .unwrap();

            assert_eq!(db_agent.room_id(), room.id());
            assert_eq!(*db_agent.status(), crate::db::agent::Status::Ready);
        });
    }

    #[test]
    fn create_subscription_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Send subscription.create event.
            let user_agent = TestAgent::new("web", "user_agent", AUDIENCE);

            let payload = json!({
                "object": vec!["rooms", &Uuid::new_v4().to_string(), "events"],
                "subject": format!("v1/agents/{}", user_agent.agent_id()),
            });

            let broker_agent = TestAgent::new("web", "wrong_user", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 403 error.
            match state.create(event, Utc::now()).await.into_result() {
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
                "object": vec!["rooms", &Uuid::new_v4().to_string(), "events"],
                "subject": format!("v1/agents/{}", user_agent.agent_id()),
            });

            let broker_agent = TestAgent::new("web", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 404 error.
            match state.create(event, Utc::now()).await.into_result() {
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
                "subject": format!("v1/agents/{}", user_agent.agent_id()),
            });

            let broker_agent = TestAgent::new("web", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 400 error.
            match state.create(event, Utc::now()).await.into_result() {
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
                "object": vec!["rooms", &db_agent.room_id().to_string(), "events"],
                "subject": format!("v1/agents/{}", db_agent.agent_id()),
            });

            let broker_agent = TestAgent::new("alpha", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.delete", &payload)
                .unwrap();

            let state = build_state(&db);
            let mut result = state.delete(event, Utc::now()).await.into_result().unwrap();

            // Assert notification to the room topic.
            let message = result.remove(0);
            assert_eq!(message.message_type(), "event");

            match message.destination() {
                Destination::Broadcast(destination) => {
                    assert_eq!(destination, &format!("rooms/{}/events", db_agent.room_id()))
                }
                _ => panic!("Expected broadcast destination"),
            }

            let payload: RoomEnterLeaveEventData = extract_payload(message).unwrap();
            assert_eq!(payload.id, db_agent.room_id());
            assert_eq!(payload.agent_id, *db_agent.agent_id());

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
                "object": vec!["rooms", &Uuid::new_v4().to_string(), "events"],
                "subject": format!("v1/agents/{}", user_agent.agent_id()),
            });

            let broker_agent = TestAgent::new("web", "wrong_user", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 403 error.
            match state.delete(event, Utc::now()).await.into_result() {
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
                "object": vec!["rooms", &Uuid::new_v4().to_string(), "events"],
                "subject": format!("v1/agents/{}", user_agent.agent_id()),
            });

            let broker_agent = TestAgent::new("web", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 404 error.
            match state.delete(event, Utc::now()).await.into_result() {
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
                "subject": format!("v1/agents/{}", user_agent.agent_id()),
            });

            let broker_agent = TestAgent::new("web", "mqtt-gateway", AUDIENCE);

            let event: CreateDeleteEvent = broker_agent
                .build_event("subscription.create", &payload)
                .unwrap();

            let state = build_state(&db);

            // Assert 400 error.
            match state.delete(event, Utc::now()).await.into_result() {
                Ok(_) => panic!("Expected subscription.delete to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::BAD_REQUEST),
            }
        });
    }
}
