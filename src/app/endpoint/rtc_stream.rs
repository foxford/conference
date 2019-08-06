use serde_derive::Deserialize;
use svc_agent::mqtt::compat::IntoEnvelope;
use svc_agent::mqtt::{
    IncomingRequest, OutgoingEvent, OutgoingEventProperties, Publish, ResponseStatus,
};
use svc_error::Error as SvcError;
use uuid::Uuid;

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
}

impl State {
    pub(crate) async fn list(&self, inreq: ListRequest) -> Result<impl Publish, SvcError> {
        let room_id = inreq.payload().room_id;

        // Authorization: room's owner has to allow the action
        {
            let conn = self.db.get()?;
            let room = room::FindQuery::new()
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
                    .build());
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

        let resp = inreq.to_response(objects, ResponseStatus::OK);
        resp.into_envelope().map_err(Into::into)
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
