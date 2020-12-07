use std::fmt;
use std::ops::Bound;

use async_std::stream;
use async_trait::async_trait;
use chrono::Duration;
use chrono::Utc;
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{IncomingRequestProperties, IntoPublishableMessage, OutgoingResponse, ResponseStatus},
    Addressable,
};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::handle_id::HandleId;
use crate::db;
use crate::diesel::Connection;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct ConnectResponseData {
    handle_id: HandleId,
}

impl ConnectResponseData {
    pub(crate) fn new(handle_id: HandleId) -> Self {
        Self { handle_id }
    }
}

pub(crate) type ConnectResponse = OutgoingResponse<ConnectResponseData>;

////////////////////////////////////////////////////////////////////////////////

const MAX_WEBINAR_DURATION: i64 = 6;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequest {
    room_id: Uuid,
}

pub(crate) struct CreateHandler;

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;
    const ERROR_TITLE: &'static str = "Failed to create rtc";

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room =
            helpers::find_room_by_id(context, payload.room_id, helpers::RoomTimeRequirement::Open)?;

        // Authorize room creation.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "create")
            .await?;

        // Create an rtc.
        let rtc = {
            let conn = context.get_conn()?;

            conn.transaction::<_, diesel::result::Error, _>(|| {
                match room.time() {
                    (start, Bound::Unbounded) => {
                        let new_time = (
                            *start,
                            Bound::Excluded(Utc::now() + Duration::hours(MAX_WEBINAR_DURATION)),
                        );

                        db::room::UpdateQuery::new(room.id())
                            .time(Some(new_time))
                            .execute(&conn)?;
                    }
                    _ => {}
                }
                let rtc = db::rtc::InsertQuery::new(room.id()).execute(&conn)?;
                Ok(rtc)
            })?
        };

        context.add_logger_tags(o!("rtc_id" => rtc.id().to_string()));

        // Respond and broadcast to the room topic.
        let response = helpers::build_response(
            ResponseStatus::CREATED,
            rtc.clone(),
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        );

        let notification = helpers::build_notification(
            "room.create",
            &format!("rooms/{}/events", room.id()),
            rtc,
            reqp,
            context.start_timestamp(),
        );

        Ok(Box::new(stream::from_iter(vec![response, notification])))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct ReadRequest {
    id: Uuid,
}

pub(crate) struct ReadHandler;

#[async_trait]
impl RequestHandler for ReadHandler {
    type Payload = ReadRequest;
    const ERROR_TITLE: &'static str = "Failed to read rtc";

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room =
            helpers::find_room_by_rtc_id(context, payload.id, helpers::RoomTimeRequirement::Open)?;

        // Authorize rtc reading.
        let rtc_id = payload.id.to_string();
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs", &rtc_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "read")
            .await?;

        // Return rtc.
        let rtc = {
            let conn = context.get_conn()?;

            db::rtc::FindQuery::new()
                .id(payload.id)
                .execute(&conn)?
                .ok_or_else(|| anyhow!("RTC not found"))
                .error(AppErrorKind::RtcNotFound)?
        };

        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            rtc,
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        ))))
    }
}

////////////////////////////////////////////////////////////////////////////////

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
    const ERROR_TITLE: &'static str = "Failed to list rtcs";

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room =
            helpers::find_room_by_id(context, payload.room_id, helpers::RoomTimeRequirement::Open)?;

        // Authorize rtc listing.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "list")
            .await?;

        // Return rtc list.
        let mut query = db::rtc::ListQuery::new().room_id(payload.room_id);

        if let Some(offset) = payload.offset {
            query = query.offset(offset);
        }

        let limit = std::cmp::min(payload.limit.unwrap_or_else(|| MAX_LIMIT), MAX_LIMIT);
        query = query.limit(limit);

        let rtcs = {
            let conn = context.get_conn()?;
            query.execute(&conn)?
        };

        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            rtcs,
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        ))))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ConnectIntent {
    Read,
    Write,
}

impl fmt::Display for ConnectIntent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Read => write!(f, "read"),
            Self::Write => write!(f, "write"),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConnectRequest {
    id: Uuid,
    #[serde(default = "ConnectRequest::default_intent")]
    intent: ConnectIntent,
}

impl ConnectRequest {
    fn default_intent() -> ConnectIntent {
        ConnectIntent::Read
    }
}

pub(crate) struct ConnectHandler;

#[async_trait]
impl RequestHandler for ConnectHandler {
    type Payload = ConnectRequest;
    const ERROR_TITLE: &'static str = "Failed to connect to rtc";

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        context.add_logger_tags(o!(
            "rtc_id" => payload.id.to_string(),
            "intent" => payload.intent.to_string(),
        ));

        let room =
            helpers::find_room_by_rtc_id(context, payload.id, helpers::RoomTimeRequirement::Open)?;

        // Authorize connecting to the rtc.
        if room.backend() != db::room::RoomBackend::Janus {
            return Err(anyhow!(
                "'rtc.connect' is not implemented for '{}' backend",
                room.backend(),
            ))
            .error(AppErrorKind::NotImplemented);
        }

        let rtc_id = payload.id.to_string();
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs", &rtc_id];

        let action = match payload.intent {
            ConnectIntent::Read => "read",
            ConnectIntent::Write => "update",
        };

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, action)
            .await?;

        // Choose backend to connect.
        let backend = {
            let conn = context.get_conn()?;

            // There are 3 cases:
            // 1. Connecting as writer for the first time. There's no `backend_id` in that case.
            //    Select the most loaded backend that is capable to host the room's reservation.
            //    If there's no capable backend then select the least loaded and send a warning
            //    to Sentry. If there are no backends at all then return `no available backends`
            //    error and also send it to Sentry.
            // 2. Connecting as reader with existing `backend_id`. Choose it because Janus doesn't
            //    support clustering and it must be the same server that the writer is connected to.
            // 3. Reconnecting as writer with existing `backend_id`. Select it to avoid partitioning
            //    of the record across multiple servers.
            let backend = match room.backend_id() {
                Some(backend_id) => db::janus_backend::FindQuery::new()
                    .id(backend_id)
                    .execute(&conn)?
                    .ok_or_else(|| anyhow!("No backend found for stream"))
                    .error(AppErrorKind::BackendNotFound)?,
                None => match db::janus_backend::most_loaded(room.id(), &conn)? {
                    Some(backend) => backend,
                    None => db::janus_backend::least_loaded(room.id(), &conn)?
                        .map(|backend| {
                            use sentry::protocol::{value::Value, Event, Level};
                            let backend_id = backend.id().to_string();

                            warn!(crate::LOG, "No capable backends to host the reserve; falling back to the least loaded backend: room_id = {}, rtc_id = {}, backend_id = {}", room_id, rtc_id, backend_id);

                            let mut extra = std::collections::BTreeMap::new();
                            extra.insert(String::from("room_id"), Value::from(room_id));
                            extra.insert(String::from("rtc_id"), Value::from(rtc_id));
                            extra.insert(String::from("backend_id"), Value::from(backend_id));

                            if let Some(reserve) = room.reserve() {
                                extra.insert(String::from("reserve"), Value::from(reserve));
                            }


                            sentry::capture_event(Event {
                                message: Some(String::from("No capable backends to host the reserve; falling back to the least loaded backend")),
                                level: Level::Warning,
                                extra,
                                ..Default::default()
                            });

                            backend
                        })
                        .ok_or_else(|| anyhow!("No available backends"))
                        .error(AppErrorKind::NoAvailableBackends)?,
                },
            };

            // Create recording if a writer connects for the first time.
            if payload.intent == ConnectIntent::Write && room.backend_id().is_none() {
                conn.transaction::<_, diesel::result::Error, _>(|| {
                    db::room::UpdateQuery::new(room.id())
                        .backend_id(Some(backend.id()))
                        .execute(&conn)?;

                    db::recording::InsertQuery::new(payload.id).execute(&conn)?;
                    Ok(())
                })?;
            }

            // Check that the backend's capacity is not exceeded for readers.
            if payload.intent == ConnectIntent::Read
                && db::janus_backend::free_capacity(payload.id, &conn)? == 0
            {
                return Err(anyhow!(
                    "Active agents number on the backend exceeded its capacity"
                ))
                .error(AppErrorKind::CapacityExceeded);
            }

            backend
        };

        context.add_logger_tags(o!("backend_id" => backend.id().to_string()));

        // Send janus handle creation request.
        let janus_request_result = context.janus_client().create_rtc_handle_request(
            reqp.clone(),
            Uuid::new_v4(),
            payload.id,
            backend.session_id(),
            backend.id(),
            context.start_timestamp(),
            authz_time,
        );

        match janus_request_result {
            Ok(req) => {
                let conn = context.get_conn()?;

                db::agent::UpdateQuery::new(reqp.as_agent_id(), room.id())
                    .status(db::agent::Status::Connected)
                    .execute(&conn)?;

                let boxed_request = Box::new(req) as Box<dyn IntoPublishableMessage + Send>;
                Ok(Box::new(stream::once(boxed_request)))
            }
            Err(err) => Err(err.context("Error creating a backend request"))
                .error(AppErrorKind::MessageBuildingFailed),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod create {
        use chrono::{SubsecRound, Utc};

        use crate::db::room::FindQueryable;
        use crate::db::rtc::Object as Rtc;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn create() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert a room.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| shared_helpers::insert_room(&conn))
                    .unwrap();

                // Allow user to create rtcs in the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = room.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs"];
                authz.allow(agent.account_id(), object, "create");

                // Make rtc.create request.
                let mut context = TestContext::new(db, authz);
                let payload = CreateRequest { room_id: room.id() };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc creation failed");

                // Assert response.
                let (rtc, respp) = find_response::<Rtc>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::CREATED);
                assert_eq!(rtc.room_id(), room.id());

                // Assert notification.
                let (rtc, evp, topic) = find_event::<Rtc>(messages.as_slice());
                assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
                assert_eq!(evp.label(), "room.create");
                assert_eq!(rtc.room_id(), room.id());
            });
        }

        #[test]
        fn create_in_unbounded_room() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert a room.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(Utc::now().trunc_subsecs(0)),
                                Bound::Unbounded,
                            ))
                            .backend(crate::db::room::RoomBackend::Janus)
                            .insert(&conn)
                    })
                    .unwrap();

                // Allow user to create rtcs in the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = room.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs"];
                authz.allow(agent.account_id(), object, "create");

                // Make rtc.create request.
                let mut context = TestContext::new(db, authz);
                let payload = CreateRequest { room_id: room.id() };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc creation failed");

                // Assert response.
                let (rtc, respp) = find_response::<Rtc>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::CREATED);
                assert_eq!(rtc.room_id(), room.id());

                // Assert room closure is not unbounded
                let conn = context.db().get().expect("Failed to get conn");

                let room = db::room::FindQuery::new(room.id())
                    .execute(&conn)
                    .expect("Db query failed")
                    .expect("Room must exist");
                assert_ne!(room.time().1, Bound::Unbounded);
            });
        }

        #[test]
        fn create_rtc_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = CreateRequest {
                    room_id: Uuid::new_v4(),
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }

        #[test]
        fn create_rtc_duplicate() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert a room.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| shared_helpers::insert_room(&conn))
                    .unwrap();

                // Allow user to create rtcs in the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = room.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs"];
                authz.allow(agent.account_id(), object, "create");

                // Make rtc.create request.
                let mut context = TestContext::new(db, authz);
                let payload = CreateRequest { room_id: room.id() };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc creation failed");

                // Assert response.
                let (rtc, respp) = find_response::<Rtc>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::CREATED);
                assert_eq!(rtc.room_id(), room.id());

                // Make rtc.create request second time.
                let payload = CreateRequest { room_id: room.id() };
                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                // This should fail with already exists
                assert_eq!(err.status(), ResponseStatus::UNPROCESSABLE_ENTITY);
                assert_eq!(err.kind(), "database_query_failed");
            });
        }

        #[test]
        fn create_rtc_unauthorized() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                // Insert a room.
                let room = db
                    .connection_pool()
                    .get()
                    .map(|conn| shared_helpers::insert_room(&conn))
                    .unwrap();

                // Make rtc.create request.
                let mut context = TestContext::new(db, TestAuthz::new());
                let payload = CreateRequest { room_id: room.id() };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }
    }

    mod read {
        use crate::db::rtc::Object as Rtc;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn read_rtc() {
            async_std::task::block_on(async {
                let db = TestDb::new();

                let rtc = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create rtc.
                    shared_helpers::insert_rtc(&conn)
                };

                // Allow agent to read the rtc.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(agent.account_id(), object, "read");

                // Make rtc.read request.
                let mut context = TestContext::new(db, authz);
                let payload = ReadRequest { id: rtc.id() };

                let messages = handle_request::<ReadHandler>(&mut context, &agent, payload)
                    .await
                    .expect("RTC reading failed");

                // Assert response.
                let (resp_rtc, respp) = find_response::<Rtc>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_rtc.room_id(), rtc.room_id());
            });
        }

        #[test]
        fn read_rtc_not_authorized() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let rtc = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    shared_helpers::insert_rtc(&conn)
                };

                let mut context = TestContext::new(db, TestAuthz::new());
                let payload = ReadRequest { id: rtc.id() };

                let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc reading");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }

        #[test]
        fn read_rtc_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = ReadRequest { id: Uuid::new_v4() };

                let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc reading");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }

    mod list {
        use crate::db::rtc::Object as Rtc;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn list_rtcs() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let rtc = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create rtc.
                    shared_helpers::insert_rtc(&conn)
                };

                // Allow agent to list rtcs in the room.
                let mut authz = TestAuthz::new();
                let room_id = rtc.room_id().to_string();
                let object = vec!["rooms", &room_id, "rtcs"];
                authz.allow(agent.account_id(), object, "list");

                // Make rtc.list request.
                let mut context = TestContext::new(db, authz);

                let payload = ListRequest {
                    room_id: rtc.room_id(),
                    offset: None,
                    limit: None,
                };

                let messages = handle_request::<ListHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc listing failed");

                // Assert response.
                let (rtcs, respp) = find_response::<Vec<Rtc>>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(rtcs.len(), 1);
                assert_eq!(rtcs[0].id(), rtc.id());
                assert_eq!(rtcs[0].room_id(), rtc.room_id());
            });
        }

        #[test]
        fn list_rtcs_not_authorized() {
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

                let mut context = TestContext::new(db, TestAuthz::new());

                let payload = ListRequest {
                    room_id: room.id(),
                    offset: None,
                    limit: None,
                };

                let err = handle_request::<ListHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc listing");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }

        #[test]
        fn list_rtcs_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

                let payload = ListRequest {
                    room_id: Uuid::new_v4(),
                    offset: None,
                    limit: None,
                };

                let err = handle_request::<ListHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc listing");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }

    mod connect {
        use std::ops::Bound;

        use chrono::{Duration, Utc};
        use rand::Rng;
        use serde_json::Value as JsonValue;
        use svc_agent::{AccountId, AgentId};

        use crate::backend::janus;
        use crate::db::agent::Status as AgentStatus;
        use crate::db::room::RoomBackend;
        use crate::test_helpers::prelude::*;
        use crate::util::from_base64;

        use super::super::*;

        #[derive(Debug, PartialEq, Deserialize)]
        struct RtcConnectTransaction {
            rtc_id: String,
            session_id: i64,
            reqp: RtcConnectTransactionReqp,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct RtcConnectTransactionReqp {
            method: String,
            agent_id: AgentId,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct JanusAttachRequest {
            janus: String,
            plugin: String,
            session_id: i64,
            transaction: String,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct JanusAttachRequestTransaction {
            rtc_id: String,
            session_id: i64,
            reqp: JanusAttachRequestTransactionReqp,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct JanusAttachRequestTransactionReqp {
            method: String,
            agent_id: AgentId,
        }

        #[test]
        fn connect_to_rtc_only() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert an rtc and janus backend.
                let (rtc, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert janus backends.
                        let backend1 = shared_helpers::insert_janus_backend(&conn);
                        let backend2 = shared_helpers::insert_janus_backend(&conn);

                        // The first backend has an active agent.
                        let room1 =
                            shared_helpers::insert_room_with_backend_id(&conn, backend1.id());

                        let _rtc1 = shared_helpers::insert_rtc_with_room(&conn, &room1);

                        let s1a1 = TestAgent::new("web", "s1a1", USR_AUDIENCE);
                        shared_helpers::insert_agent(&conn, s1a1.agent_id(), room1.id());

                        // The second backend has 2 agents.
                        let room2 =
                            shared_helpers::insert_room_with_backend_id(&conn, backend2.id());

                        let _rtc2 = shared_helpers::insert_rtc_with_room(&conn, &room2);

                        let s2a1 = TestAgent::new("web", "s2a1", USR_AUDIENCE);
                        shared_helpers::insert_agent(&conn, s2a1.agent_id(), room2.id());

                        let s2a2 = TestAgent::new("web", "s2a2", USR_AUDIENCE);
                        shared_helpers::insert_agent(&conn, s2a2.agent_id(), room2.id());

                        // The new rtc for which we will balance the stream.
                        let rtc3 = shared_helpers::insert_rtc(&conn);

                        (rtc3, backend2)
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(agent.account_id(), object, "read");

                // Make rtc.connect request.
                let mut context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                let messages = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                    .await
                    .expect("RTC connect failed");

                // Assert outgoing request to Janus.
                let (req, _reqp, topic) = find_request::<JanusAttachRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    backend.id(),
                    janus::JANUS_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, &expected_topic);
                assert_eq!(req.janus, "attach");
                assert_eq!(req.plugin, "janus.plugin.conference");
                assert_eq!(req.session_id, backend.session_id());

                // `transaction` field is base64 encoded JSON. Decode and assert.
                let txn_wrap: JsonValue = from_base64(&req.transaction).unwrap();
                let txn_value = txn_wrap.get("CreateRtcHandle").unwrap().to_owned();
                let txn: RtcConnectTransaction = serde_json::from_value(txn_value).unwrap();

                assert_eq!(
                    txn,
                    RtcConnectTransaction {
                        rtc_id: rtc.id().to_string(),
                        session_id: backend.session_id(),
                        reqp: RtcConnectTransactionReqp {
                            method: "ignore".to_string(),
                            agent_id: AgentId::new("web", AccountId::new("user123", USR_AUDIENCE)),
                        }
                    }
                )
            });
        }

        #[test]
        fn connect_to_ongoing_rtc() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert an rtc and janus backend.
                let (rtc, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        let _backend1 = shared_helpers::insert_janus_backend(&conn);
                        let backend2 = shared_helpers::insert_janus_backend(&conn);

                        let room =
                            shared_helpers::insert_room_with_backend_id(&conn, &backend2.id());

                        let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);
                        (rtc, backend2)
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(agent.account_id(), object, "read");

                // Make rtc.connect request.
                let mut context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                let messages = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                    .await
                    .expect("RTC connect failed");

                // Ensure we're balanced to the backend with the stream.
                let (req, _reqp, topic) = find_request::<JanusAttachRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    backend.id(),
                    janus::JANUS_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(req.session_id, backend.session_id());
            });
        }

        #[test]
        fn connect_to_rtc_with_reservation() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                let (rtc, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // The first backend is big enough but has some load.
                        let backend1_id = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        let backend1 =
                            factory::JanusBackend::new(backend1_id, rng.gen(), rng.gen())
                                .capacity(20)
                                .insert(&conn);

                        let room1 =
                            shared_helpers::insert_room_with_backend_id(&conn, backend1.id());

                        let _rtc1 = shared_helpers::insert_rtc_with_room(&conn, &room1);

                        let agent = TestAgent::new("web", "user456", SVC_AUDIENCE);
                        shared_helpers::insert_agent(&conn, agent.agent_id(), room1.id());

                        let agent = TestAgent::new("web", "user456", USR_AUDIENCE);
                        shared_helpers::insert_agent(&conn, agent.agent_id(), room1.id());

                        // The second backend is too small but has no load.
                        let backend2_id = {
                            let agent = TestAgent::new("beta", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        factory::JanusBackend::new(backend2_id, rng.gen(), rng.gen())
                            .capacity(5)
                            .insert(&conn);

                        // It should balance to the first one despite of the load.
                        let now = Utc::now();

                        let room2 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .backend(RoomBackend::Janus)
                            .reserve(15)
                            .insert(&conn);

                        let rtc2 = shared_helpers::insert_rtc_with_room(&conn, &room2);
                        (rtc2, backend1)
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(agent.account_id(), object, "read");

                // Make rtc.connect request.
                let mut context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                let messages = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                    .await
                    .expect("RTC connect failed");

                // Ensure we're balanced to the right backend.
                let (req, _reqp, topic) = find_request::<JanusAttachRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    backend.id(),
                    janus::JANUS_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(req.session_id, backend.session_id());
            });
        }

        #[test]
        fn connect_to_rtc_take_reserved_slot() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                let reader1 = TestAgent::new("web", "reader1", USR_AUDIENCE);
                let reader2 = TestAgent::new("web", "reader2", USR_AUDIENCE);
                let writer1 = TestAgent::new("web", "writer1", USR_AUDIENCE);
                let writer2 = TestAgent::new("web", "writer2", USR_AUDIENCE);

                let (rtc1, rtc2) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert backend with capacity = 4.
                        let backend_id = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        let backend = factory::JanusBackend::new(backend_id, rng.gen(), rng.gen())
                            .capacity(4)
                            .insert(&conn);

                        // Insert rooms: 1 with reserve = 2 and the other without reserve.
                        let now = Utc::now();

                        let room1 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .backend(RoomBackend::Janus)
                            .backend_id(&backend.id())
                            .reserve(2)
                            .insert(&conn);

                        let room2 =
                            shared_helpers::insert_room_with_backend_id(&conn, &backend.id());

                        // Insert rtcs.
                        let rtc1 = factory::Rtc::new(room1.id()).insert(&conn);
                        let rtc2 = factory::Rtc::new(room2.id()).insert(&conn);

                        // Insert active agents.
                        shared_helpers::insert_agent(&conn, writer1.agent_id(), room1.id());
                        shared_helpers::insert_agent(&conn, writer2.agent_id(), room2.id());

                        factory::Agent::new()
                            .agent_id(reader1.agent_id())
                            .room_id(room2.id())
                            .status(AgentStatus::Ready)
                            .insert(&conn);

                        shared_helpers::insert_agent(&conn, reader2.agent_id(), room2.id());

                        (rtc1, rtc2)
                    })
                    .unwrap();

                // Allow user to read rtcs.
                for rtc in &[&rtc1, &rtc2] {
                    let room_id = rtc.room_id().to_string();
                    let rtc_id = rtc.id().to_string();
                    let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                    authz.allow(reader1.account_id(), object, "read");
                }

                // Connect to the rtc in the room without reserve.
                let mut context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc2.id(),
                    intent: ConnectIntent::Read,
                };

                // Expect failure.
                let err = handle_request::<ConnectHandler>(&mut context, &reader1, payload)
                    .await
                    .expect_err("Connected to RTC while expected capacity exceeded error");

                assert_eq!(err.status(), ResponseStatus::SERVICE_UNAVAILABLE);
                assert_eq!(err.kind(), "capacity_exceeded");

                // Connect to the rtc in the room with free reserved slots.
                let payload = ConnectRequest {
                    id: rtc1.id(),
                    intent: ConnectIntent::Read,
                };

                // Expect success.
                handle_request::<ConnectHandler>(&mut context, &reader1, payload)
                    .await
                    .expect("RTC connect failed");
            });
        }

        #[test]
        fn connect_to_rtc_as_last_reader() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                let writer = TestAgent::new("web", "writer", USR_AUDIENCE);
                let reader = TestAgent::new("web", "reader", USR_AUDIENCE);

                let rtc = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert backend.
                        let backend_id = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        let backend = factory::JanusBackend::new(backend_id, rng.gen(), rng.gen())
                            .capacity(2)
                            .insert(&conn);

                        // Insert room and rtc.
                        let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                        let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                        // Insert active agents.
                        shared_helpers::insert_agent(&conn, writer.agent_id(), room.id());

                        factory::Agent::new()
                            .agent_id(reader.agent_id())
                            .room_id(room.id())
                            .status(AgentStatus::Ready)
                            .insert(&conn);

                        rtc
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(reader.account_id(), object, "read");

                // Make rtc.connect request.
                let mut context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                handle_request::<ConnectHandler>(&mut context, &reader, payload)
                    .await
                    .expect("RTC connect failed");
            });
        }

        #[test]
        fn connect_to_rtc_full_server_as_reader() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                let writer = TestAgent::new("web", "writer", USR_AUDIENCE);
                let reader1 = TestAgent::new("web", "reader1", USR_AUDIENCE);
                let reader2 = TestAgent::new("web", "reader2", USR_AUDIENCE);

                let rtc = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert backend.
                        let backend_id = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        let backend = factory::JanusBackend::new(backend_id, rng.gen(), rng.gen())
                            .capacity(2)
                            .insert(&conn);

                        // Insert room and rtc.
                        let room =
                            shared_helpers::insert_room_with_backend_id(&conn, &backend.id());

                        let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                        // Insert active agents.
                        shared_helpers::insert_agent(&conn, writer.agent_id(), room.id());
                        shared_helpers::insert_agent(&conn, reader1.agent_id(), room.id());

                        factory::Agent::new()
                            .agent_id(reader2.agent_id())
                            .room_id(room.id())
                            .status(AgentStatus::Ready)
                            .insert(&conn);

                        rtc
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(reader2.account_id(), object, "read");

                // Make rtc.connect request.
                let mut context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                let err = handle_request::<ConnectHandler>(&mut context, &reader2, payload)
                    .await
                    .expect_err("Unexpected success on rtc connecting");

                assert_eq!(err.status(), ResponseStatus::SERVICE_UNAVAILABLE);
                assert_eq!(err.kind(), "capacity_exceeded");
            });
        }

        #[test]
        fn connect_to_rtc_full_server_as_writer() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                let writer = TestAgent::new("web", "writer", USR_AUDIENCE);
                let reader = TestAgent::new("web", "reader", USR_AUDIENCE);

                let rtc = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert backend.
                        let backend_id = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            agent.agent_id().to_owned()
                        };

                        let backend = factory::JanusBackend::new(backend_id, rng.gen(), rng.gen())
                            .capacity(1)
                            .insert(&conn);

                        // Insert rtc.
                        let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                        let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                        // Insert active agents.
                        shared_helpers::insert_agent(&conn, reader.agent_id(), room.id());

                        factory::Agent::new()
                            .agent_id(writer.agent_id())
                            .room_id(room.id())
                            .status(AgentStatus::Ready)
                            .insert(&conn);

                        rtc
                    })
                    .unwrap();

                // Allow user to update the rtc.
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(writer.account_id(), object, "update");

                // Make rtc.connect request.
                let mut context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Write,
                };

                handle_request::<ConnectHandler>(&mut context, &writer, payload)
                    .await
                    .expect("RTC connect failed");
            });
        }

        #[test]
        fn connect_to_rtc_too_big_reserve() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                let new_writer = TestAgent::new("web", "new-writer", USR_AUDIENCE);

                let (rtc, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        let now = Utc::now();

                        // We have two backends with cap=800 and balance_cap=700 each
                        // We have two rooms with reserves 500 and 600, each at its own backend
                        // Room with reserve 500 has 1 writer and 2 readers, ie its load is 3
                        // Room with reserve 600 has 1 writer and 1 readers, ie its load is 2
                        // We want to balance a room with reserve 400
                        // Since it doesnt fit anywhere it should go to backend with smallest current load,
                        // ie to backend 2 (though it has only 100 free reserve, and backend1 has 200 free reserve)

                        // Insert alpha and beta backends.
                        let backend1 = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            let id = agent.agent_id().to_owned();
                            factory::JanusBackend::new(id, rng.gen(), rng.gen())
                                .balancer_capacity(700)
                                .capacity(800)
                                .insert(&conn)
                        };

                        let backend2 = {
                            let agent = TestAgent::new("beta", "janus", SVC_AUDIENCE);
                            let id = agent.agent_id().to_owned();
                            factory::JanusBackend::new(id, rng.gen(), rng.gen())
                                .balancer_capacity(700)
                                .capacity(800)
                                .insert(&conn)
                        };

                        // Setup three rooms with 500, 600 and 400 reserves.
                        let room1 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .backend(RoomBackend::Janus)
                            .backend_id(backend1.id())
                            .reserve(500)
                            .insert(&conn);

                        let room2 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .reserve(600)
                            .backend(RoomBackend::Janus)
                            .backend_id(backend2.id())
                            .insert(&conn);

                        let room3 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .reserve(400)
                            .backend(RoomBackend::Janus)
                            .insert(&conn);

                        // Insert rtcs for each room.
                        let _rtc1 = shared_helpers::insert_rtc_with_room(&conn, &room1);
                        let _rtc2 = shared_helpers::insert_rtc_with_room(&conn, &room2);
                        let rtc3 = shared_helpers::insert_rtc_with_room(&conn, &room3);

                        // Insert writer for room 1 @ backend 1
                        factory::Agent::new()
                            .agent_id(TestAgent::new("web", "writer1", USR_AUDIENCE).agent_id())
                            .room_id(room1.id())
                            .status(AgentStatus::Connected)
                            .insert(&conn);

                        // Insert two readers for room 1 @ backend 1
                        factory::Agent::new()
                            .agent_id(TestAgent::new("web", "reader1-1", USR_AUDIENCE).agent_id())
                            .room_id(room1.id())
                            .status(AgentStatus::Connected)
                            .insert(&conn);

                        factory::Agent::new()
                            .agent_id(TestAgent::new("web", "reader1-2", USR_AUDIENCE).agent_id())
                            .room_id(room1.id())
                            .status(AgentStatus::Connected)
                            .insert(&conn);

                        // Insert writer for room 2 @ backend 2
                        factory::Agent::new()
                            .agent_id(TestAgent::new("web", "writer2", USR_AUDIENCE).agent_id())
                            .room_id(room2.id())
                            .status(AgentStatus::Connected)
                            .insert(&conn);

                        // Insert reader for room 2 @ backend 2
                        factory::Agent::new()
                            .agent_id(TestAgent::new("web", "reader2", USR_AUDIENCE).agent_id())
                            .room_id(room2.id())
                            .status(AgentStatus::Connected)
                            .insert(&conn);

                        (rtc3, backend2)
                    })
                    .unwrap();

                // Allow user to update the rtc.
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(new_writer.account_id(), object, "update");

                // Make an rtc.connect request.
                // Despite none of the backends are capable to host the reserve it should
                // select the least loaded one.
                let mut context = TestContext::new(db, authz);

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Write,
                };

                let messages = handle_request::<ConnectHandler>(&mut context, &new_writer, payload)
                    .await
                    .expect("RTC connect failed");

                // Assert outgoing request goes to the expected backend.
                let (_req, _reqp, topic) = find_request::<JanusAttachRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    backend.id(),
                    janus::JANUS_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, &expected_topic);
            });
        }

        #[test]
        fn connect_to_rtc_reserve_overflow() {
            async_std::task::block_on(async {
                let mut rng = rand::thread_rng();
                let db = TestDb::new();
                let mut authz = TestAuthz::new();
                let new_reader = TestAgent::new("web", "new-reader", USR_AUDIENCE);

                let (rtcs, backend) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        let now = Utc::now();

                        // Lets say we have a single backend with cap=800
                        // Somehow reserves of all rooms that were allocated to it overflow its capacity
                        // We should allow users to connect to rooms with reserves if reserve and cap allows them
                        // But not allow to connect to room with no reserve

                        // Insert alpha backend.
                        let backend = {
                            let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                            let id = agent.agent_id().to_owned();
                            factory::JanusBackend::new(id, rng.gen(), rng.gen())
                                .balancer_capacity(700)
                                .capacity(800)
                                .insert(&conn)
                        };

                        // Setup three rooms with 500, 600 and none.
                        let room1 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .backend(RoomBackend::Janus)
                            .backend_id(backend.id())
                            .reserve(500)
                            .insert(&conn);

                        let room2 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .reserve(600)
                            .backend(RoomBackend::Janus)
                            .backend_id(backend.id())
                            .insert(&conn);

                        let room3 = factory::Room::new()
                            .audience(USR_AUDIENCE)
                            .time((
                                Bound::Included(now),
                                Bound::Excluded(now + Duration::hours(1)),
                            ))
                            .backend(RoomBackend::Janus)
                            .backend_id(backend.id())
                            .insert(&conn);

                        // Insert rtcs for each room.
                        let rtc1 = shared_helpers::insert_rtc_with_room(&conn, &room1);
                        let rtc2 = shared_helpers::insert_rtc_with_room(&conn, &room2);
                        let rtc3 = shared_helpers::insert_rtc_with_room(&conn, &room3);

                        // Insert writer for room 1
                        factory::Agent::new()
                            .agent_id(TestAgent::new("web", "writer1", USR_AUDIENCE).agent_id())
                            .room_id(room1.id())
                            .status(AgentStatus::Connected)
                            .insert(&conn);

                        // Insert 450 readers for room 1
                        for i in 0..450 {
                            factory::Agent::new()
                                .agent_id(
                                    TestAgent::new("web", &format!("reader1-{}", i), USR_AUDIENCE)
                                        .agent_id(),
                                )
                                .room_id(room1.id())
                                .status(AgentStatus::Connected)
                                .insert(&conn);
                        }

                        // Insert writer for room 3
                        factory::Agent::new()
                            .agent_id(TestAgent::new("web", "writer3", USR_AUDIENCE).agent_id())
                            .room_id(room2.id())
                            .status(AgentStatus::Connected)
                            .insert(&conn);

                        // Insert reader for room 3
                        factory::Agent::new()
                            .agent_id(TestAgent::new("web", "reader3", USR_AUDIENCE).agent_id())
                            .room_id(room3.id())
                            .status(AgentStatus::Connected)
                            .insert(&conn);

                        ([rtc1, rtc2, rtc3], backend)
                    })
                    .unwrap();

                // Allow user to read the rtcs.
                for rtc in rtcs.iter() {
                    let room_id = rtc.room_id().to_string();
                    let rtc_id = rtc.id().to_string();
                    let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                    authz.allow(new_reader.account_id(), object, "read");
                }

                let mut context = TestContext::new(db, authz);

                // First two rooms have reserves AND there is free capacity so we can connect to them
                for rtc in rtcs.iter().take(2) {
                    let payload = ConnectRequest {
                        id: rtc.id(),
                        intent: ConnectIntent::Read,
                    };

                    // Make an rtc.connect request.
                    let messages =
                        handle_request::<ConnectHandler>(&mut context, &new_reader, payload)
                            .await
                            .expect("RTC connect failed");

                    // Assert outgoing request goes to the expected backend.
                    let (_req, _reqp, topic) =
                        find_request::<JanusAttachRequest>(messages.as_slice());

                    let expected_topic = format!(
                        "agents/{}/api/{}/in/{}",
                        backend.id(),
                        janus::JANUS_API_VERSION,
                        context.config().id,
                    );

                    assert_eq!(topic, &expected_topic);
                }

                let payload = ConnectRequest {
                    id: rtcs[2].id(),
                    intent: ConnectIntent::Read,
                };

                // Last room has NO reserve AND there is free capacity BUT it was exhausted by first two rooms
                // So we cant connect to this room
                let err = handle_request::<ConnectHandler>(&mut context, &new_reader, payload)
                    .await
                    .expect_err("Connected to RTC while expected capacity exceeded error");

                assert_eq!(err.status(), ResponseStatus::SERVICE_UNAVAILABLE);
                assert_eq!(err.kind(), "capacity_exceeded");
            });
        }

        #[test]
        fn connect_to_rtc_not_authorized() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let rtc = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    shared_helpers::insert_rtc(&conn)
                };

                let mut context = TestContext::new(db, TestAuthz::new());

                let payload = ConnectRequest {
                    id: rtc.id(),
                    intent: ConnectIntent::Read,
                };

                let err = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc connecting");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }

        #[test]
        fn connect_to_rtc_missing() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

                let payload = ConnectRequest {
                    id: Uuid::new_v4(),
                    intent: ConnectIntent::Read,
                };

                let err = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc connecting");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }
}
