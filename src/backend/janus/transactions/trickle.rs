use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{
    IncomingRequestProperties, OutgoingMessage, OutgoingRequest, ShortTermTimingProperties,
};

use crate::app::handle_id::HandleId;
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
        handle_id: &HandleId,
        jsep: JsonValue,
        start_timestamp: DateTime<Utc>,
    ) -> Result<OutgoingMessage<TrickleRequest>> {
        let to = handle_id.backend_id();

        let props = reqp.to_request(
            METHOD,
            &self.response_topic(to)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        let transaction = Transaction::Trickle(TransactionData::new(reqp));

        let payload = TrickleRequest::new(
            &to_base64(&transaction)?,
            handle_id.janus_session_id(),
            handle_id.janus_handle_id(),
            jsep,
        );

        self.register_transaction(to, start_timestamp, &props, &payload, self.timeout(METHOD));

        Ok(OutgoingRequest::unicast(
            payload,
            props,
            to,
            JANUS_API_VERSION,
        ))
    }
}
