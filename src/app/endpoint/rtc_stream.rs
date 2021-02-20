use std::result::Result as StdResult;

use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_derive::Deserialize;
use svc_agent::mqtt::{
    IncomingRequestProperties, OutgoingEvent, OutgoingEventProperties, OutgoingMessage,
    ResponseStatus, ShortTermTimingProperties, TrackingProperties,
};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::db;

////////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 25;

#[derive(Debug, Deserialize)]
pub(crate) struct ListRequest {
    room_id: Uuid,
    rtc_id: Option<Uuid>,
    #[serde(default)]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<db::room::Time>,
    offset: Option<i64>,
    limit: Option<i64>,
}

pub(crate) struct ListHandler;

#[async_trait]
impl RequestHandler for ListHandler {
    type Payload = ListRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        if let Some(rtc_id) = payload.rtc_id {
            context.add_logger_tags(o!("rtc_id" => rtc_id.to_string()));
        }

        let room =
            helpers::find_room_by_id(context, payload.room_id, helpers::RoomTimeRequirement::Open)?;

        if room.backend() != db::room::RoomBackend::Janus {
            let err = anyhow!(
                "'rtc_stream.list' is not supported for '{}' backend",
                room.backend()
            );

            return Err(err).error(AppErrorKind::UnsupportedBackend)?;
        }

        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "rtcs"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "list")
            .await?;

        let mut query = db::janus_rtc_stream::ListQuery::new().room_id(payload.room_id);

        if let Some(rtc_id) = payload.rtc_id {
            query = query.rtc_id(rtc_id);
        }

        if let Some(time) = payload.time {
            query = query.time(time);
        }

        if let Some(offset) = payload.offset {
            query = query.offset(offset);
        }

        query = query.limit(std::cmp::min(payload.limit.unwrap_or(MAX_LIMIT), MAX_LIMIT));

        let rtc_streams = {
            let conn = context.get_conn()?;
            query.execute(&conn)?
        };

        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            rtc_streams,
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        ))))
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) type ObjectUpdateEvent = OutgoingMessage<db::janus_rtc_stream::Object>;

pub(crate) fn update_event(
    room_id: Uuid,
    object: db::janus_rtc_stream::Object,
    start_timestamp: DateTime<Utc>,
    tracking: &TrackingProperties,
) -> StdResult<ObjectUpdateEvent, AppError> {
    let uri = format!("rooms/{}/events", room_id);
    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let mut props = OutgoingEventProperties::new("rtc_stream.update", timing);
    props.set_tracking(tracking.to_owned());
    Ok(OutgoingEvent::broadcast(object, props, &uri))
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod list {
        use chrono::serde::ts_seconds;
        use chrono::{DateTime, SubsecRound, Utc};
        use serde_derive::Deserialize;
        use std::ops::Bound;
        use svc_agent::AgentId;
        use uuid::Uuid;

        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[derive(Deserialize)]
        struct JanusRtcStream {
            id: Uuid,
            rtc_id: Uuid,
            label: String,
            sent_by: AgentId,
            #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
            time: Option<(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>)>,
            #[serde(with = "ts_seconds")]
            created_at: DateTime<Utc>,
        }

        #[test]
        fn list_rtc_streams() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                let (rtc_stream, rtc) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert backend, rooms and rtcs.
                        let backend = shared_helpers::insert_janus_backend(&conn);

                        let room1 =
                            shared_helpers::insert_room_with_backend_id(&conn, backend.id());

                        let room2 =
                            shared_helpers::insert_room_with_backend_id(&conn, backend.id());

                        let rtc1 = shared_helpers::insert_rtc_with_room(&conn, &room1);
                        let rtc2 = shared_helpers::insert_rtc_with_room(&conn, &room2);

                        // Insert janus rtc streams.
                        let rtc_stream = factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend)
                            .rtc(&rtc1)
                            .insert(&conn);

                        let rtc_stream = crate::db::janus_rtc_stream::start(rtc_stream.id(), &conn)
                            .expect("Failed to start rtc stream")
                            .expect("Missing rtc stream");

                        let other_rtc_stream = factory::JanusRtcStream::new(USR_AUDIENCE)
                            .backend(&backend)
                            .rtc(&rtc2)
                            .insert(&conn);

                        crate::db::janus_rtc_stream::start(other_rtc_stream.id(), &conn)
                            .expect("Failed to start rtc stream");

                        (rtc_stream, rtc1)
                    })
                    .expect("Failed to create rtc streams");

                // Allow user to list rtcs in the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = rtc.room_id().to_string();
                let object = vec!["rooms", &room_id, "rtcs"];
                authz.allow(agent.account_id(), object, "list");

                // Make rtc_stream.list request.
                let mut context = TestContext::new(db, authz);

                let payload = ListRequest {
                    room_id: rtc.room_id(),
                    rtc_id: Some(rtc.id()),
                    time: None,
                    offset: None,
                    limit: None,
                };

                let messages = handle_request::<ListHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc streams listing failed");

                // Assert response.
                let (streams, respp, _) = find_response::<Vec<JanusRtcStream>>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(streams.len(), 1);

                let expected_time = match rtc_stream.time().expect("Missing time") {
                    (Bound::Included(val), upper) => (Bound::Included(val.trunc_subsecs(0)), upper),
                    _ => panic!("Bad rtc stream time"),
                };

                assert_eq!(streams[0].id, rtc_stream.id());
                assert_eq!(streams[0].rtc_id, rtc_stream.rtc_id());
                assert_eq!(streams[0].label, rtc_stream.label());
                assert_eq!(&streams[0].sent_by, rtc_stream.sent_by());
                assert_eq!(streams[0].time, Some(expected_time));

                assert_eq!(
                    streams[0].created_at,
                    rtc_stream.created_at().trunc_subsecs(0)
                );
            });
        }

        #[test]
        fn list_rtc_streams_not_authorized() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let backend = shared_helpers::insert_janus_backend(&conn);
                    shared_helpers::insert_room_with_backend_id(&conn, backend.id())
                };

                let mut context = TestContext::new(db, TestAuthz::new());

                let payload = ListRequest {
                    room_id: room.id(),
                    rtc_id: None,
                    time: None,
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
        fn list_rtc_streams_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new(), TestAuthz::new());

                let payload = ListRequest {
                    room_id: Uuid::new_v4(),
                    rtc_id: None,
                    time: None,
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
}
