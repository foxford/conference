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
    const ERROR_TITLE: &'static str = "Failed to list rtc streams";

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
                "'rtc_stream.list' is not implemented for '{}' backend",
                room.backend()
            );

            return Err(err).error(AppErrorKind::NotImplemented)?;
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
        use std::ops::Bound;

        use chrono::SubsecRound;
        use diesel::prelude::*;

        use crate::db::janus_rtc_stream::Object as JanusRtcStream;
        use crate::db::rtc::Object as Rtc;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn list_rtc_streams() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                let (rtc_stream, rtc) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        // Insert janus rtc streams.
                        let rtc_stream = factory::JanusRtcStream::new(USR_AUDIENCE).insert(&conn);

                        let rtc_stream = crate::db::janus_rtc_stream::start(rtc_stream.id(), &conn)
                            .expect("Failed to start rtc stream")
                            .expect("Missing rtc stream");

                        let other_rtc_stream =
                            factory::JanusRtcStream::new(USR_AUDIENCE).insert(&conn);

                        crate::db::janus_rtc_stream::start(other_rtc_stream.id(), &conn)
                            .expect("Failed to start rtc stream");

                        // Find rtc.
                        let rtc: Rtc = crate::schema::rtc::table
                            .find(rtc_stream.rtc_id())
                            .get_result(&conn)
                            .expect("Rtc not found");

                        (rtc_stream, rtc)
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
                let (streams, respp) = find_response::<Vec<JanusRtcStream>>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(streams.len(), 1);

                let expected_time = match rtc_stream.time().expect("Missing time") {
                    (Bound::Included(val), upper) => (Bound::Included(val.trunc_subsecs(0)), upper),
                    _ => panic!("Bad rtc stream time"),
                };

                assert_eq!(streams[0].id(), rtc_stream.id());
                assert_eq!(streams[0].handle_id(), rtc_stream.handle_id());
                assert_eq!(streams[0].backend_id(), rtc_stream.backend_id());
                assert_eq!(streams[0].label(), rtc_stream.label());
                assert_eq!(streams[0].sent_by(), rtc_stream.sent_by());
                assert_eq!(streams[0].time(), Some(expected_time));
                assert_eq!(
                    streams[0].created_at(),
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

                    shared_helpers::insert_room(&conn)
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

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
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

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }
}
