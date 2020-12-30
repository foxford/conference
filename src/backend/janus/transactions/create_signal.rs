use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IncomingResponseProperties, OutgoingMessage, OutgoingRequest,
        OutgoingRequestProperties, ShortTermTimingProperties,
    },
    Addressable, AgentId,
};

use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::{CreateSignalRequestBody, MessageRequest};
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_conference_signal.create";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct TransactionData {
    reqp: IncomingRequestProperties,
}

impl TransactionData {
    pub(crate) fn new(reqp: IncomingRequestProperties) -> Self {
        Self { reqp }
    }

    pub(crate) fn reqp(&self) -> &IncomingRequestProperties {
        &self.reqp
    }
}

impl Client {
    pub(crate) fn create_signal_request(
        &self,
        reqp: &IncomingRequestProperties,
        respp: &IncomingResponseProperties,
        backend_id: &AgentId,
        janus_session_id: i64,
        janus_handle_id: i64,
        jsep: JsonValue,
        start_timestamp: DateTime<Utc>,
    ) -> Result<OutgoingMessage<MessageRequest>> {
        let tn_data = TransactionData::new(reqp.to_owned());
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
            &self.response_topic(backend_id)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        props.set_tracking(respp.tracking().to_owned());
        let timeout = self.timeout(METHOD);
        self.register_transaction(backend_id, start_timestamp, &props, &payload, timeout);

        Ok(OutgoingRequest::unicast(
            payload,
            props,
            backend_id,
            JANUS_API_VERSION,
        ))
    }
}
