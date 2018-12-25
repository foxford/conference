use crate::app::janus;
use crate::backend::janus::CreateSessionRequest;
use crate::transport::mqtt::{LocalMessage, Message};
use crate::transport::{
    AgentId, Destination, LocalMessageProperties, LocalResponseMessageProperties,
    LocalResponseMessageStatus,
};
use failure::Error;
use serde_derive::Deserialize;
use uuid::Uuid;

use crate::app::model::rtc;

pub(crate) type CreateRequest = Message<CreateRequestData>;
pub(crate) type CreateResponse = LocalMessage<rtc::Record>;

pub(crate) fn create_rtc_response(data: rtc::Record, to: AgentId) -> CreateResponse {
    let status = LocalResponseMessageStatus::Success;
    let props = LocalMessageProperties::Response(LocalResponseMessageProperties::new(status));
    let message = LocalMessage::new(data, props, Destination::Unicast(to));
    message
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    room_id: Uuid,
}

pub(crate) struct State {
    // TODO: replace with backend agent registery, make private
    pub(crate) backend_agent_id: AgentId,
}

impl State {
    pub(crate) fn create(
        &self,
        input: &CreateRequest,
    ) -> Result<LocalMessage<CreateSessionRequest>, Error> {
        // Creating a Real-Time Connection
        let record = rtc::InsertQuery::new(&input.payload().room_id, &input.subject()).execute()?;

        // TODO: reuse a Janus Session if it already exists (create only Janus Handler)
        // Building a Create Janus Session request
        let owner_id = input.agent_id();
        let to = self.backend_agent_id.clone();
        let req = janus::create_session_request(record, owner_id, to)?;

        Ok(req)
    }
}
