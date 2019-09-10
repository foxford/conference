use failure::Error;
use serde_derive::Deserialize;
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{
    IncomingRequest, IncomingRequestProperties, IncomingResponse, OutgoingRequest,
    OutgoingRequestProperties, OutgoingResponse, OutgoingResponseProperties, Publishable,
    ResponseStatus, SubscriptionTopic,
};
use svc_agent::{AgentId, Subscription};
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
    me: AgentId,
}

impl State {
    pub(crate) fn new(me: AgentId) -> Self {
        Self { me }
    }
}

impl State {
    pub(crate) async fn create(
        &self,
        inreq: CreateRequest,
    ) -> Result<Vec<Box<dyn Publishable>>, SvcError> {
        let to = &inreq.payload().agent_id;
        let payload = &inreq.payload().data;

        let response_topic = Subscription::multicast_requests_from(to)
            .subscription_topic(&self.me)
            .map_err(|_| {
                SvcError::builder()
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                    .detail("error building responses subscription topic")
                    .build()
            })?;

        let correlation_data = to_base64(inreq.properties()).map_err(|_| {
            SvcError::builder()
                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                .detail("error encoding incoming request properties")
                .build()
        })?;

        let props = OutgoingRequestProperties::new(
            inreq.properties().method(),
            &response_topic,
            &correlation_data,
        );

        let message = OutgoingRequest::unicast(payload.to_owned(), props, to);
        Ok(vec![Box::new(message) as Box<dyn Publishable>])
    }

    pub(crate) async fn callback(
        &self,
        inresp: CreateIncomingResponse,
    ) -> Result<Vec<Box<dyn Publishable>>, Error> {
        let reqp =
            from_base64::<IncomingRequestProperties>(inresp.properties().correlation_data())?;
        let payload = inresp.payload();

        let props =
            OutgoingResponseProperties::new(inresp.properties().status(), reqp.correlation_data());

        let message = OutgoingResponse::unicast(payload.to_owned(), props, &reqp);
        Ok(vec![Box::new(message) as Box<dyn Publishable>])
    }
}

#[cfg(test)]
mod test {
    use serde_json::{json, Value as JsonValue};
    use svc_agent::Destination;

    use super::*;
    use crate::test_helpers::{agent::TestAgent, extract_payload};

    const AGENT_LABEL: &str = "web";
    const AUDIENCE: &str = "example.org";
    const ROOM_ID: &str = "3b8226e6-a7c0-11e9-8019-60f81db6d53e";

    #[test]
    fn create_message() {
        futures::executor::block_on(async {
            let sender = TestAgent::new(AGENT_LABEL, "sender", AUDIENCE);
            let receiver = TestAgent::new(AGENT_LABEL, "receiver", AUDIENCE);

            let payload = json!({
                "agent_id": receiver.agent_id().to_string(),
                "room_id": ROOM_ID,
                "data": {"key": "value"},
            });

            let incoming: CreateRequest = sender.build_request("message.create", &payload).unwrap();
            let state = State::new(sender.agent_id().clone());
            let mut result = state.create(incoming).await.unwrap();
            let message = result.remove(0);

            match message.destination() {
                &Destination::Unicast(ref agent_id) => assert_eq!(agent_id, receiver.agent_id()),
                _ => panic!("Expected unicast destination"),
            }

            let payload: JsonValue = extract_payload(message).unwrap();
            assert_eq!(payload, json!({"key": "value"}));
        });
    }
}
