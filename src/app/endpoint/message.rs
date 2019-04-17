use failure::Error;
use serde_derive::Deserialize;
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{
    compat::IntoEnvelope, IncomingRequest, IncomingRequestProperties, IncomingResponse,
    OutgoingRequest, OutgoingRequestProperties, OutgoingResponse, OutgoingResponseProperties,
    OutgoingResponseStatus, Publish, SubscriptionTopic,
};
use svc_agent::{AccountId, AgentId, Subscription};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::util::{from_base64, to_base64};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    agent_id: AgentId,
    room_id: Uuid,
    data: JsonValue,
}

pub(crate) type CreateIncomingResponse = IncomingResponse<JsonValue>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    me: AccountId,
}

impl State {
    pub(crate) fn new(me: AccountId) -> Self {
        Self { me }
    }
}

impl State {
    pub(crate) async fn create(&self, inreq: CreateRequest) -> Result<impl Publish, SvcError> {
        let to = &inreq.payload().agent_id;
        let payload = &inreq.payload().data;

        let response_topic = Subscription::unicast_requests(Some(&self.me))
            .subscription_topic(to)
            .map_err(|_| {
                SvcError::builder()
                    .status(OutgoingResponseStatus::UNPROCESSABLE_ENTITY)
                    .detail("error building responses subscription topic")
                    .build()
            })?;

        let correlation_data = to_base64(inreq.properties()).map_err(|_| {
            SvcError::builder()
                .status(OutgoingResponseStatus::UNPROCESSABLE_ENTITY)
                .detail("error encoding incoming request properties")
                .build()
        })?;
        let props = OutgoingRequestProperties::new(
            inreq.properties().method(),
            &response_topic,
            &correlation_data,
        );
        let req = OutgoingRequest::unicast(payload, props, to);
        req.into_envelope().map_err(Into::into)
    }

    pub(crate) async fn callback(
        &self,
        inresp: CreateIncomingResponse,
    ) -> Result<impl Publish, Error> {
        let reqp =
            from_base64::<IncomingRequestProperties>(inresp.properties().correlation_data())?;
        let payload = inresp.payload();

        let props = OutgoingResponseProperties::new(
            inresp.properties().status(),
            reqp.correlation_data(),
            None,
        );
        let resp = OutgoingResponse::unicast(payload, props, &reqp);
        resp.into_envelope().map_err(Into::into)
    }
}
