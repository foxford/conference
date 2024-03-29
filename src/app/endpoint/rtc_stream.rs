use anyhow::anyhow;
use async_trait::async_trait;
use axum::extract::{Extension, Path, Query};
use chrono::{DateTime, Utc};

use serde::Deserialize;
use std::sync::Arc;
use svc_agent::mqtt::{
    OutgoingEvent, OutgoingEventProperties, OutgoingMessage, ResponseStatus,
    ShortTermTimingProperties,
};
use svc_utils::extractors::AgentIdExtractor;

use crate::{
    app::{
        context::{AppContext, Context},
        endpoint::prelude::*,
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
    },
    authz::AuthzObject,
    db,
};
use tracing_attributes::instrument;

////////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 25;

#[derive(Debug, Deserialize)]
pub struct ListRequest {
    room_id: db::room::Id,
    rtc_id: Option<db::rtc::Id>,
    #[serde(default)]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<db::room::Time>,
    offset: Option<i64>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    rtc_id: Option<db::rtc::Id>,
    #[serde(default)]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<db::room::Time>,
    offset: Option<i64>,
    limit: Option<i64>,
}

pub async fn list(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    query: Option<Query<ListParams>>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = match query {
        Some(x) => ListRequest {
            room_id,
            rtc_id: x.rtc_id,
            time: x.time,
            offset: x.offset,
            limit: x.limit,
        },
        None => ListRequest {
            room_id,
            rtc_id: None,
            time: None,
            offset: None,
            limit: None,
        },
    };
    ListHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct ListHandler;

#[async_trait]
impl RequestHandler for ListHandler {
    type Payload = ListRequest;
    const ERROR_TITLE: &'static str = "Failed to list rtc streams";

    #[instrument(skip(context, payload, reqp), fields(rtc_id = ?payload.rtc_id, room_id = %payload.room_id))]
    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let room = {
            let mut conn = context.get_conn().await?;
            helpers::find_room_by_id(
                payload.room_id,
                helpers::RoomTimeRequirement::Open,
                &mut conn,
            )
            .await?
        };

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        if room.rtc_sharing_policy() == db::rtc::SharingPolicy::None {
            let err = anyhow!(
                "'rtc_stream.list' is not implemented for rtc_sharing_policy = '{}'",
                room.rtc_sharing_policy()
            );

            return Err(err).error(AppErrorKind::NotImplemented)?;
        }

        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "read".into())
            .await?;
        context.metrics().observe_auth(authz_time);

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

        let mut conn = context.get_conn().await?;
        let rtc_streams = query.execute(&mut conn).await?;

        context
            .metrics()
            .request_duration
            .rtc_stream_list
            .observe_timestamp(context.start_timestamp());

        Ok(Response::new(
            ResponseStatus::OK,
            rtc_streams,
            context.start_timestamp(),
            Some(authz_time),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////

pub type ObjectUpdateEvent = OutgoingMessage<db::janus_rtc_stream::Object>;

pub fn update_event(
    room_id: db::room::Id,
    object: db::janus_rtc_stream::Object,
    start_timestamp: DateTime<Utc>,
) -> ObjectUpdateEvent {
    let uri = format!("rooms/{room_id}/events");
    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let props = OutgoingEventProperties::new("rtc_stream.update", timing);
    OutgoingEvent::broadcast(object, props, &uri)
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod list {
        use std::ops::Bound;

        use chrono::SubsecRound;

        use crate::{
            db::janus_rtc_stream::Object as JanusRtcStream,
            test_helpers::{db::TestDb, prelude::*},
        };

        use super::super::*;

        #[sqlx::test]
        async fn list_rtc_streams(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);
            let mut authz = TestAuthz::new();

            let mut conn = db.get_conn().await;

            let (rtc_stream, rtc, classroom_id) = {
                // Insert janus rtc streams.
                let rtc_stream = factory::JanusRtcStream::new(USR_AUDIENCE)
                    .insert(&mut conn)
                    .await;

                let rtc_stream = db::janus_rtc_stream::start(rtc_stream.id(), &mut conn)
                    .await
                    .expect("Failed to start rtc stream")
                    .expect("Missing rtc stream");

                let other_rtc_stream = factory::JanusRtcStream::new(USR_AUDIENCE)
                    .insert(&mut conn)
                    .await;

                db::janus_rtc_stream::start(other_rtc_stream.id(), &mut conn)
                    .await
                    .expect("Failed to start rtc stream");

                // Find rtc.
                let rtc = db::rtc::FindQuery::new(rtc_stream.rtc_id())
                    .execute(&mut conn)
                    .await
                    .expect("rtc find query failed")
                    .expect("rtc not found");

                let room = helpers::find_room_by_id(
                    rtc.room_id(),
                    helpers::RoomTimeRequirement::Open,
                    &mut conn,
                )
                .await
                .expect("Room not found");

                (rtc_stream, rtc, room.classroom_id().to_string())
            };

            // Allow user to list rtcs in the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let object = vec!["classrooms", &classroom_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc_stream.list request.
            let mut context = TestContext::new(db, authz).await;

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
        }

        #[sqlx::test]
        async fn list_rtc_streams_not_authorized(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let mut context = TestContext::new(db, TestAuthz::new()).await;

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
        }

        #[sqlx::test]
        async fn list_rtc_streams_missing_room(pool: sqlx::PgPool) {
            let db = TestDb::new(pool);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let payload = ListRequest {
                room_id: db::room::Id::random(),
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
        }
    }
}
