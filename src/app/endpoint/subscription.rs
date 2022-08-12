use async_trait::async_trait;
use chrono::Utc;
use diesel::PgConnection;
use futures::stream;
use serde::{Deserialize, Serialize};
use std::result::Result as StdResult;
use svc_agent::{
    mqtt::{IncomingRequestProperties, IncomingResponseProperties},
    AgentId,
};

use crate::{
    app::{context::Context, endpoint::prelude::*},
    db::{self, room::FindQueryable},
};
use tracing_attributes::instrument;

use super::MqttResult;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub struct CorrelationDataPayload {
    reqp: IncomingRequestProperties,
    subject: AgentId,
    object: Vec<String>,
}

impl CorrelationDataPayload {
    pub fn new(reqp: IncomingRequestProperties, subject: AgentId, object: Vec<String>) -> Self {
        Self {
            reqp,
            subject,
            object,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct RoomEnterLeaveEvent {
    id: db::room::Id,
    agent_id: AgentId,
}

impl RoomEnterLeaveEvent {
    pub fn new(id: db::room::Id, agent_id: AgentId) -> Self {
        Self { id, agent_id }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct CreateDeleteResponsePayload {}

///////////////////////////////////////////////////////////////////////////////

pub struct DeleteResponseHandler;

#[async_trait]
impl ResponseHandler for DeleteResponseHandler {
    type Payload = CreateDeleteResponsePayload;
    type CorrelationData = CorrelationDataPayload;

    #[instrument(skip(_context, _payload, _respp, _corr_data))]
    async fn handle<C: Context>(
        _context: &mut C,
        _payload: Self::Payload,
        _respp: &IncomingResponseProperties,
        _corr_data: &Self::CorrelationData,
    ) -> MqttResult {
        Ok(Box::new(stream::empty()))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[instrument(skip(context))]
pub async fn leave_room<C: Context>(
    context: &mut C,
    agent_id: &AgentId,
    room_id: db::room::Id,
) -> StdResult<bool, AppError> {
    let conn = context.get_conn().await?;
    let left = crate::util::spawn_blocking({
        let agent_id = agent_id.clone();
        move || {
            let row_count = db::agent::DeleteQuery::new()
                .agent_id(&agent_id)
                .room_id(room_id)
                .execute(&conn)?;

            if row_count < 1 {
                return Ok::<_, AppError>(false);
            }

            make_orphaned_if_host_left(room_id, &agent_id, &conn)?;
            Ok::<_, AppError>(true)
        }
    })
    .await?;
    Ok(left)
}

fn make_orphaned_if_host_left(
    room_id: db::room::Id,
    agent_left: &AgentId,
    connection: &PgConnection,
) -> StdResult<(), diesel::result::Error> {
    let room = db::room::FindQuery::new(room_id).execute(connection)?;
    if room.as_ref().and_then(|x| x.host()) == Some(agent_left) {
        db::orphaned_room::upsert_room(room_id, Utc::now(), connection)?;
    }
    Ok(())
}
