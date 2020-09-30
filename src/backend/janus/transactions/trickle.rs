use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn trickle_request(
        &self,
        reqp: IncomingRequestProperties,
        session_id: i64,
        handle_id: i64,
        jsep: JsonValue,
        to: &AgentId,
        start_timestamp: DateTime<Utc>,
        authz_time: Duration,
    ) -> Result<OutgoingMessage<TrickleRequest>> {
        let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
        short_term_timing.set_authorization_time(authz_time);

        let props = reqp.to_request(
            METHOD,
            &self.response_topic(to)?,
            &generate_correlation_data(),
            short_term_timing,
        );

        let transaction = Transaction::Trickle(TransactionData::new(reqp));
        let payload = TrickleRequest::new(&to_base64(&transaction)?, session_id, handle_id, jsep);
        self.register_transaction(to, start_timestamp, &props, &payload, self.timeout(METHOD));

        Ok(OutgoingRequest::unicast(
            payload,
            props,
            to,
            JANUS_API_VERSION,
        ))
    }
}
