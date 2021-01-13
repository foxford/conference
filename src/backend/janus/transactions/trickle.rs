use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, OutgoingMessage, OutgoingRequest, ShortTermTimingProperties,
    },
    AgentId,
};

use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::TrickleRequest;
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_trickle.create";

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
    pub(crate) fn trickle_request(
        &self,
        reqp: IncomingRequestProperties,
        backend_id: &AgentId,
        janus_session_id: i64,
        janus_handle_id: i64,
        jsep: JsonValue,
        start_timestamp: DateTime<Utc>,
    ) -> Result<OutgoingMessage<TrickleRequest>> {
        let props = reqp.to_request(
            METHOD,
            &self.response_topic(backend_id)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        let transaction = Transaction::Trickle(TransactionData::new(reqp));

        let payload = TrickleRequest::new(
            &to_base64(&transaction)?,
            janus_session_id,
            janus_handle_id,
            jsep,
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
