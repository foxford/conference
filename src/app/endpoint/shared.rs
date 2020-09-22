use chrono::{DateTime, Duration, Utc};
use diesel::pg::PgConnection;
use serde::Serialize;
use svc_agent::mqtt::{
    IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
    OutgoingResponse, ResponseStatus, ShortTermTimingProperties,
};
use svc_error::Error as SvcError;

use crate::app::message_handler::SvcErrorSugar;
use crate::app::API_VERSION;
use crate::db::room::{FindQuery, Object as Room};

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_response(
    status: ResponseStatus,
    payload: impl Serialize + Send + 'static,
    reqp: &IncomingRequestProperties,
    start_timestamp: DateTime<Utc>,
    maybe_authz_time: Option<Duration>,
) -> Box<dyn IntoPublishableMessage + Send> {
    let mut timing = ShortTermTimingProperties::until_now(start_timestamp);

    if let Some(authz_time) = maybe_authz_time {
        timing.set_authorization_time(authz_time);
    }

    let props = reqp.to_response(status, timing);
    Box::new(OutgoingResponse::unicast(payload, props, reqp, API_VERSION))
}

pub(crate) fn build_notification(
    label: &'static str,
    path: &str,
    payload: impl Serialize + Send + 'static,
    reqp: &IncomingRequestProperties,
    start_timestamp: DateTime<Utc>,
) -> Box<dyn IntoPublishableMessage + Send> {
    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let mut props = OutgoingEventProperties::new(label, timing);
    props.set_tracking(reqp.tracking().to_owned());
    Box::new(OutgoingEvent::broadcast(payload, props, path))
}

pub(crate) fn find_room<F>(
    query: &FindQuery,
    conn: &PgConnection,
    error_title: &str,
    details: F,
) -> Result<Room, SvcError>
where
    F: FnOnce() -> String,
{
    query
        .execute(&conn)?
        .ok_or_else(details)
        .status(ResponseStatus::NOT_FOUND)
        .map_err(|mut e| {
            e.set_kind("room.not_found", error_title);
            e
        })
}

pub(crate) fn find_open_room<F>(
    query: &FindQuery,
    conn: &PgConnection,
    error_title: &str,
    details: F,
) -> Result<Room, SvcError>
where
    F: FnOnce() -> String,
{
    let room = find_room(query, conn, error_title, details)?;

    if room.is_closed() {
        let err = SvcError::builder()
            .status(ResponseStatus::NOT_FOUND)
            .detail(&format!("the room = '{}' is closed", room.id()))
            .kind("room.closed", error_title)
            .build();

        return Err(err);
    }

    Ok(room)
}
