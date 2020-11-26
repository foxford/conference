use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::mqtt::{
    IncomingRequestProperties, OutgoingMessage, OutgoingRequest, ShortTermTimingProperties,
};
use uuid::Uuid;

use crate::app::handle_id::HandleId;
use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::{CreateStreamRequestBody, MessageRequest};
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
    pub(crate) fn create_stream_request(
        &self,
        reqp: IncomingRequestProperties,
        handle_id: &HandleId,
        rtc_id: Uuid,
        start_timestamp: DateTime<Utc>,
        authz_time: Duration,
    ) -> Result<OutgoingMessage<MessageRequest>> {
        let to = handle_id.backend_id();
        let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
        short_term_timing.set_authorization_time(authz_time);

        let props = reqp.to_request(
            METHOD,
            &self.response_topic(to)?,
            &generate_correlation_data(),
            short_term_timing,
        );

        let body = CreateStreamRequestBody::new(rtc_id);
        let transaction = Transaction::CreateStream(TransactionData::new(reqp));

        let payload = MessageRequest::new(
            &to_base64(&transaction)?,
            handle_id.janus_session_id(),
            handle_id.janus_handle_id(),
            serde_json::to_value(&body)?,
            None,
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
