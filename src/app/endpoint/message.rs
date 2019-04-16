use serde_derive::{Deserialize, Serialize};
use svc_agent::mqtt::{
    compat::IntoEnvelope, IncomingRequest, OutgoingRequest, OutgoingRequestProperties,
    OutgoingResponseStatus, Publish,
};
use svc_agent::AgentId;
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::util::to_base64;

////////////////////////////////////////////////////////////////////////////////

const IGNORE: &str = "ignore";

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    agent_id: AgentId,
    room_id: Uuid,
    data: String,
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {}

impl State {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl State {
    pub(crate) async fn create(&self, inreq: CreateRequest) -> Result<impl Publish, SvcError> {
        let to = &inreq.payload().agent_id;
        let payload = &inreq.payload().data;

        let correlation_data = to_base64(inreq.properties()).map_err(|_| {
            SvcError::builder()
                .status(OutgoingResponseStatus::UNPROCESSABLE_ENTITY)
                .detail("error encoding incoming request properties")
                .build()
        })?;
        let props =
            OutgoingRequestProperties::new(inreq.properties().method(), IGNORE, &correlation_data);
        let req = OutgoingRequest::unicast(payload, props, to);
        req.into_envelope().map_err(Into::into)
    }
}
