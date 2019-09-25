use serde_derive::Deserialize;
use svc_agent::mqtt::{IncomingRequest, OutgoingEvent, OutgoingEventProperties, ResponseStatus};
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
            self.authz.authorize(
                room.audience(),
                inreq.properties(),
                vec!["rooms", &room_id, "rtcs"],
                "list",
            )?;
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

        inreq.to_response(objects, ResponseStatus::OK).into()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn update_event(room_id: Uuid, object: janus_rtc_stream::Object) -> ObjectUpdateEvent {
    let uri = format!("rooms/{}/events", room_id);
    OutgoingEvent::broadcast(
        object,
        OutgoingEventProperties::new("rtc_stream.update"),
        &uri,
    )
}

#[cfg(test)]
mod test {
    use std::ops::{Bound, Try};

    use diesel::prelude::*;
    use serde_json::json;
    use svc_agent::AgentId;

    use crate::test_helpers::{
        agent::TestAgent, db::TestDb, extract_payload, factory::insert_janus_rtc_stream, no_authz,
    };

    use super::*;

    const AUDIENCE: &str = "dev.svc.example.org";

    fn build_state(db: &TestDb) -> State {
        State::new(no_authz(AUDIENCE), db.connection_pool().clone())
    }

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

    #[test]
    fn list_rtc_streams() {
        futures::executor::block_on(async {
            let db = TestDb::new();

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

            // Make rtc_stream.list request.
            let state = build_state(&db);
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let payload = json!({"room_id": rtc.room_id(), "rtc_id": rtc.id()});
            let request: ListRequest = agent.build_request("rtc_stream.list", &payload).unwrap();
            let mut result = state.list(request).await.into_result().unwrap();
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
}
