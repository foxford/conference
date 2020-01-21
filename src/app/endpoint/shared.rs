use chrono::{DateTime, Duration, Utc};
use diesel::pg::PgConnection;
use serde::Serialize;
use svc_agent::mqtt::{
    IncomingRequest, IntoPublishableDump, OutgoingEvent, OutgoingEventProperties, ResponseStatus,
    ShortTermTimingProperties,
};
use svc_agent::AgentId;
use svc_error::Error as SvcError;

use crate::app::{endpoint, API_VERSION};
use crate::db::{agent, room};

pub(crate) fn respond<R, O: 'static + Clone + Serialize>(
    inreq: &IncomingRequest<R>,
    object: O,
    notification: Option<(&'static str, &str)>,
    start_timestamp: DateTime<Utc>,
    authz_time: Duration,
) -> endpoint::Result {
    let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    short_term_timing.set_authorization_time(authz_time);

    let resp = inreq.to_response(
        object.clone(),
        ResponseStatus::OK,
        short_term_timing.clone(),
        API_VERSION,
    );

    let mut messages: Vec<Box<dyn IntoPublishableDump>> = vec![Box::new(resp)];

    if let Some((label, topic)) = notification {
        let mut props = OutgoingEventProperties::new(label, short_term_timing);
        props.set_tracking(inreq.properties().tracking().to_owned());
        messages.push(Box::new(OutgoingEvent::broadcast(object, props, topic)));
    }

    messages.into()
}

pub(crate) fn check_room_presence(
    room: &room::Object,
    agent_id: &AgentId,
    conn: &PgConnection,
) -> Result<(), SvcError> {
    let results = agent::ListQuery::new()
        .room_id(room.id())
        .agent_id(agent_id)
        .status(agent::Status::Ready)
        .execute(conn)?;

    if results.len() == 0 {
        let err = SvcError::builder()
            .status(ResponseStatus::NOT_FOUND)
            .detail(&format!(
                "agent = '{}' is not online in the room = '{}'",
                agent_id,
                room.id()
            ))
            .build();

        Err(err)
    } else {
        Ok(())
    }
}
