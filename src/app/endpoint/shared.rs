use diesel::pg::PgConnection;
use svc_agent::{mqtt::ResponseStatus, AgentId};
use svc_error::Error as SvcError;

use crate::db::{agent, room};

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
