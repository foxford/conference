use std::ops::Bound;

use anyhow::anyhow;
use chrono::{DateTime, Duration, Utc};
use diesel::pg::PgConnection;
use serde::Serialize;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
        OutgoingResponse, ResponseStatus, ShortTermTimingProperties,
    },
    AgentId,
};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind};
use crate::app::API_VERSION;
use crate::db;
use crate::db::room::Object as Room;

///////////////////////////////////////////////////////////////////////////////

pub fn build_response(
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

pub fn build_notification(
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

////////////////////////////////////////////////////////////////////////////////

pub enum RoomTimeRequirement {
    Any,
    NotClosed,
    NotClosedOrUnboundedOpen,
    Open,
}

pub fn find_room_by_id<C: Context>(
    context: &mut C,
    id: Uuid,
    opening_requirement: RoomTimeRequirement,
    conn: &PgConnection,
) -> Result<db::room::Object, AppError> {
    context.add_logger_tags(o!("room_id" => id.to_string()));
    let query = db::room::FindQuery::new(id);
    find_room(context, query, opening_requirement, conn)
}

pub fn find_room_by_rtc_id<C: Context>(
    context: &mut C,
    rtc_id: Uuid,
    opening_requirement: RoomTimeRequirement,
    conn: &PgConnection,
) -> Result<db::room::Object, AppError> {
    context.add_logger_tags(o!("rtc_id" => rtc_id.to_string()));
    let query = db::room::FindByRtcIdQuery::new(rtc_id);
    find_room(context, query, opening_requirement, conn)
}

fn find_room<C, Q>(
    context: &mut C,
    query: Q,
    opening_requirement: RoomTimeRequirement,
    conn: &PgConnection,
) -> Result<Room, AppError>
where
    C: Context,
    Q: db::room::FindQueryable,
{
    let room = query
        .execute(&conn)?
        .ok_or_else(|| anyhow!("Room not found"))
        .error(AppErrorKind::RoomNotFound)?;

    add_room_logger_tags(context, &room);

    match opening_requirement {
        // Room time doesn't matter.
        RoomTimeRequirement::Any => Ok(room),
        // Current time must be before room closing, including not yet opened rooms.
        // Rooms without closing time are fine.
        // Rooms without opening time are forbidden.
        RoomTimeRequirement::NotClosed => {
            let now = Utc::now();

            match room.time() {
                (Bound::Unbounded, _) => {
                    Err(anyhow!("Room has no opening time")).error(AppErrorKind::RoomClosed)
                }
                (_, Bound::Included(dt)) | (_, Bound::Excluded(dt)) if *dt < now => {
                    Err(anyhow!("Room closed")).error(AppErrorKind::RoomClosed)
                }
                _ => Ok(room),
            }
        }
        // Current time must be before room closing, including not yet opened rooms.
        // Rooms without closing time are fine.
        // Rooms without opening time are fine.
        RoomTimeRequirement::NotClosedOrUnboundedOpen => {
            let now = Utc::now();

            match room.time() {
                (_, Bound::Included(dt)) | (_, Bound::Excluded(dt)) if *dt < now => {
                    Err(anyhow!("Room closed")).error(AppErrorKind::RoomClosed)
                }
                _ => Ok(room),
            }
        }
        // Current time must be exactly in the room's time range.
        RoomTimeRequirement::Open => {
            let now = Utc::now();
            let (opened_at, closed_at) = room.time();

            match opened_at {
                Bound::Unbounded => {
                    Err(anyhow!("Room has no opening time")).error(AppErrorKind::RoomClosed)
                }
                Bound::Included(dt) | Bound::Excluded(dt) if *dt >= now => {
                    Err(anyhow!("Room not opened")).error(AppErrorKind::RoomClosed)
                }
                _ => Ok(()),
            }?;

            match closed_at {
                Bound::Included(dt) | Bound::Excluded(dt) if *dt < now => {
                    Err(anyhow!("Room closed")).error(AppErrorKind::RoomClosed)
                }
                _ => Ok(()),
            }?;

            Ok(room)
        }
    }
}

pub fn check_room_presence(
    room: &db::room::Object,
    agent_id: &AgentId,
    conn: &PgConnection,
) -> Result<(), AppError> {
    let results = db::agent::ListQuery::new()
        .room_id(room.id())
        .agent_id(agent_id)
        .execute(conn)?;

    if results.is_empty() {
        Err(anyhow!("Agent is not online in the room")).error(AppErrorKind::AgentNotEnteredTheRoom)
    } else {
        Ok(())
    }
}

pub fn add_room_logger_tags<C: Context>(context: &mut C, room: &db::room::Object) {
    context.add_logger_tags(o!("room_id" => room.id().to_string()));

    if let Some(scope) = room.tags().get("scope") {
        context.add_logger_tags(o!("scope" => scope.to_string()));
    }
}
