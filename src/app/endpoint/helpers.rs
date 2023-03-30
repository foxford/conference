use std::ops::Bound;

use crate::{
    app::{
        error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind},
        API_VERSION,
    },
    db,
    db::room::Object as Room,
};
use anyhow::{anyhow, Context};
use chrono::{DateTime, Duration, Utc};
use diesel::pg::PgConnection;
use serde::Serialize;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
        OutgoingResponse, ResponseStatus, ShortTermTimingProperties, TrackingProperties,
    },
    AgentId,
};

///////////////////////////////////////////////////////////////////////////////

pub fn build_response(
    status: ResponseStatus,
    payload: impl Serialize + Send + Sync + 'static,
    reqp: &IncomingRequestProperties,
    start_timestamp: DateTime<Utc>,
    maybe_authz_time: Option<Duration>,
) -> Box<dyn IntoPublishableMessage + Send + Sync + 'static> {
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
    payload: impl Serialize + Send + Sync + 'static,
    trp: &TrackingProperties,
    start_timestamp: DateTime<Utc>,
) -> Box<dyn IntoPublishableMessage + Send + Sync + 'static> {
    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let mut props = OutgoingEventProperties::new(label, timing);
    props.set_tracking(trp.to_owned());
    Box::new(OutgoingEvent::broadcast(payload, props, path))
}

////////////////////////////////////////////////////////////////////////////////

pub enum RoomTimeRequirement {
    Any,
    NotClosed,
    NotClosedOrUnboundedOpen,
    Open,
}

pub fn find_room_by_id(
    id: db::room::Id,
    opening_requirement: RoomTimeRequirement,
    conn: &PgConnection,
) -> Result<db::room::Object, AppError> {
    let query = db::room::FindQuery::new(id);
    find_room(query, opening_requirement, conn)
}

pub fn find_room_by_rtc_id(
    rtc_id: db::rtc::Id,
    opening_requirement: RoomTimeRequirement,
    conn: &PgConnection,
) -> Result<db::room::Object, AppError> {
    let query = db::room::FindByRtcIdQuery::new(rtc_id);
    find_room(query, opening_requirement, conn)
}

fn find_room<Q>(
    query: Q,
    opening_requirement: RoomTimeRequirement,
    conn: &PgConnection,
) -> Result<Room, AppError>
where
    Q: db::room::FindQueryable,
{
    let room = query
        .execute(conn)?
        .context("Room not found")
        .error(AppErrorKind::RoomNotFound)?;

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
