use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, OutgoingMessage, OutgoingRequest, ShortTermTimingProperties,
    },
    AgentId,
};
use uuid::Uuid;

use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::{MessageRequest, ReadStreamRequestBody};
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_conference_stream.create";

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
    pub(crate) fn read_stream_request(
        &self,
        reqp: IncomingRequestProperties,
        backend_id: &AgentId,
        janus_session_id: i64,
        janus_handle_id: i64,
        rtc_id: Uuid,
        start_timestamp: DateTime<Utc>,
        authz_time: Duration,
    ) -> Result<OutgoingMessage<MessageRequest>> {
        let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
        short_term_timing.set_authorization_time(authz_time);

        let props = reqp.to_request(
            METHOD,
            &self.response_topic(backend_id)?,
            &generate_correlation_data(),
            short_term_timing,
        );

        let body = ReadStreamRequestBody::new(rtc_id);
        let transaction = Transaction::ReadStream(TransactionData::new(reqp));

        let payload = MessageRequest::new(
            &to_base64(&transaction)?,
            janus_session_id,
            janus_handle_id,
            serde_json::to_value(&body)?,
            None,
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
