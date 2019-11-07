use chrono::{DateTime, Utc};
use serde_derive::Deserialize;
use svc_agent::mqtt::{
    IncomingRequest, OutgoingEvent, OutgoingEventProperties, ResponseStatus,
    ShortTermTimingProperties,
};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
use crate::db::{janus_rtc_stream, janus_rtc_stream::Time, room, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 25;

////////////////////////////////////////////////////////////////////////////////

pub(crate) type ListRequest = IncomingRequest<ListRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct ListRequestData {
    room_id: Uuid,
    rtc_id: Option<Uuid>,
    #[serde(default)]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<Time>,
    offset: Option<i64>,
    limit: Option<i64>,
}

pub(crate) type ObjectUpdateEvent = OutgoingEvent<janus_rtc_stream::Object>;

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

            if room.backend() != &room::RoomBackend::Janus {
                return Err(SvcError::builder()
                    .status(ResponseStatus::NOT_IMPLEMENTED)
                    .detail(&format!(
                        "'rtc_stream.list' is not implemented for the backend = '{}'.",
                        room.backend()
                    ))
                    .build())?;
            }

            let room_id = room.id().to_string();

            self.authz
                .authorize(
                    room.audience(),
                    inreq.properties(),
                    vec!["rooms", &room_id, "rtcs"],
                    "list",
                )
                .await
                .map_err(|err| SvcError::from(err))?
        };

        let objects = {
            let conn = self.db.get()?;
            janus_rtc_stream::ListQuery::from((
                Some(room_id),
                inreq.payload().rtc_id,
                inreq.payload().time,
                inreq.payload().offset,
                Some(std::cmp::min(
                    inreq.payload().limit.unwrap_or_else(|| MAX_LIMIT),
                    MAX_LIMIT,
                )),
            ))
            .execute(&conn)?
        };

        let mut timing = ShortTermTimingProperties::until_now(start_timestamp);
        timing.set_authorization_time(authz_time);

        inreq
            .to_response(objects, ResponseStatus::OK, timing)
            .into()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn update_event(
    room_id: Uuid,
    object: janus_rtc_stream::Object,
    start_timestamp: DateTime<Utc>,
) -> Result<ObjectUpdateEvent, SvcError> {
    let uri = format!("rooms/{}/events", room_id);

    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let props = OutgoingEventProperties::new("rtc_stream.update", timing);
    Ok(OutgoingEvent::broadcast(object, props, &uri))
}

#[cfg(test)]
mod test {
    use std::ops::{Bound, Try};

    use diesel::prelude::*;
    use serde_json::json;
    use svc_agent::AgentId;

    use crate::test_helpers::{
        agent::TestAgent,
        authz::{no_authz, TestAuthz},
        db::TestDb,
        extract_payload,
        factory::{insert_janus_rtc_stream, insert_rtc},
    };

    use super::*;

    const AUDIENCE: &str = "dev.svc.example.org";

    #[derive(Debug, PartialEq, Deserialize)]
    struct RtcStreamResponse {
        id: Uuid,
        handle_id: i64,
        rtc_id: Uuid,
        backend_id: AgentId,
        label: String,
        sent_by: AgentId,
        time: Option<Vec<Option<i64>>>,
        created_at: i64,
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn list_rtc_streams() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            let (rtc_stream, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    // Insert a janus rtc stream.
                    let rtc_stream = insert_janus_rtc_stream(&conn, AUDIENCE);
                    let _other_rtc_stream = insert_janus_rtc_stream(&conn, AUDIENCE);

                    // Find rtc.
                    let rtc: crate::db::rtc::Object = crate::schema::rtc::table
                        .find(rtc_stream.rtc_id())
                        .get_result(&conn)
                        .unwrap();

                    (rtc_stream, rtc)
                })
                .unwrap();

            // Allow user to list rtcs in the room.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = rtc.room_id().to_string();
            let object = vec!["rooms", &room_id, "rtcs"];
            authz.allow(agent.account_id(), object, "list");

            // Make rtc_stream.list request.
            let state = State::new(authz.into(), db.connection_pool().clone());
            let payload = json!({"room_id": rtc.room_id(), "rtc_id": rtc.id()});
            let request: ListRequest = agent.build_request("rtc_stream.list", &payload).unwrap();
            let mut result = state.list(request, Utc::now()).await.into_result().unwrap();
            let message = result.remove(0);

            // Assert response.
            let resp: Vec<RtcStreamResponse> = extract_payload(message).unwrap();
            assert_eq!(resp.len(), 1);

            let start = match rtc_stream.time().unwrap().0 {
                Bound::Included(val) => val,
                _ => panic!("Bad rtc stream time"),
            };

            assert_eq!(
                *resp.first().unwrap(),
                RtcStreamResponse {
                    id: rtc_stream.id().to_owned(),
                    handle_id: rtc_stream.handle_id(),
                    rtc_id: rtc_stream.rtc_id(),
                    backend_id: rtc_stream.backend_id().to_owned(),
                    label: rtc_stream.label().to_string(),
                    sent_by: rtc_stream.sent_by().to_owned(),
                    time: Some(vec![Some(start.timestamp()), None]),
                    created_at: rtc_stream.created_at().timestamp(),
                }
            );
        });
    }

    #[test]
    fn list_rtc_streams_missing_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Make rtc_stream.list request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let payload = json!({"room_id": Uuid::new_v4(), "rtc_id": Uuid::new_v4()});
            let request: ListRequest = agent.build_request("rtc_stream.list", &payload).unwrap();
            let result = state.list(request, Utc::now()).await.into_result();

            // Assert 404 error response.
            match result {
                Ok(_) => panic!("Expected rtc_stream.list to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND),
            }
        });
    }

    #[test]
    fn list_rtc_streams_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert an rtc.
            let rtc = db
                .connection_pool()
                .get()
                .map(|conn| insert_rtc(&conn, AUDIENCE))
                .unwrap();

            // Make rtc_stream.list request.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({ "room_id": rtc.room_id(), "rtc_id": rtc.id() });
            let state = State::new(authz.into(), db.connection_pool().clone());
            let request: ListRequest = agent.build_request("rtc_stream.list", &payload).unwrap();
            let result = state.list(request, Utc::now()).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected rtc_stream.list to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }
}
