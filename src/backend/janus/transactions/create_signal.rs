use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IncomingResponseProperties, OutgoingMessage, OutgoingRequest,
        OutgoingRequestProperties, ShortTermTimingProperties,
    },
    Addressable,
};

use crate::app::handle_id::HandleId;
use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::{CreateSignalRequestBody, MessageRequest};
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_conference_signal.create";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct TransactionData {
    reqp: IncomingRequestProperties,
    handle_id: HandleId,
}

impl TransactionData {
    pub(crate) fn new(reqp: IncomingRequestProperties, handle_id: HandleId) -> Self {
        Self { reqp, handle_id }
    }

    pub(crate) fn reqp(&self) -> &IncomingRequestProperties {
        &self.reqp
    }

    pub(crate) fn handle_id(&self) -> &HandleId {
        &self.handle_id
    }
}

impl Client {
    pub(crate) fn create_signal_request(
        &self,
        reqp: &IncomingRequestProperties,
        respp: &IncomingResponseProperties,
        handle_id: HandleId,
        jsep: JsonValue,
        start_timestamp: DateTime<Utc>,
    ) -> Result<OutgoingMessage<MessageRequest>> {
        let janus_session_id = handle_id.janus_session_id();
        let janus_handle_id = handle_id.janus_handle_id();
        let to = handle_id.backend_id().to_owned();

        let tn_data = TransactionData::new(reqp.to_owned(), handle_id);
        let transaction = Transaction::CreateSignal(tn_data);
        let body = CreateSignalRequestBody::new(reqp.as_agent_id().to_owned());

        let payload = MessageRequest::new(
            &to_base64(&transaction)?,
            janus_session_id,
            janus_handle_id,
            serde_json::to_value(&body)?,
            Some(jsep),
        );

        let mut props = OutgoingRequestProperties::new(
            METHOD,
            &self.response_topic(&to)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        props.set_tracking(respp.tracking().to_owned());
        self.register_transaction(&to, start_timestamp, &props, &payload, self.timeout(METHOD));

        Ok(OutgoingRequest::unicast(
            payload,
            props,
            &to,
            JANUS_API_VERSION,
        ))
    }
}
