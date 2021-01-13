use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, OutgoingMessage, OutgoingRequest, OutgoingRequestProperties,
        ShortTermTimingProperties,
    },
    AgentId,
};

use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::{MessageRequest, UpdateSignalRequestBody};
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_conference_signal.update";

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
    pub(crate) fn update_signal_request(
        &self,
        reqp: IncomingRequestProperties,
        backend_id: &AgentId,
        janus_session_id: i64,
        janus_handle_id: i64,
        jsep: JsonValue,
        start_timestamp: DateTime<Utc>,
    ) -> Result<OutgoingMessage<MessageRequest>> {
        let transaction = Transaction::UpdateSignal(TransactionData::new(reqp));
        let body = UpdateSignalRequestBody::new();

        let payload = MessageRequest::new(
            &to_base64(&transaction)?,
            janus_session_id,
            janus_handle_id,
            serde_json::to_value(&body)?,
            Some(jsep),
        );

        let props = OutgoingRequestProperties::new(
            METHOD,
            &self.response_topic(&backend_id)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::until_now(start_timestamp),
        );

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
