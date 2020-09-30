use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, OutgoingMessage, OutgoingRequest, OutgoingRequestProperties,
        ShortTermTimingProperties,
    },
    AgentId,
};
use uuid::Uuid;

use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::{MessageRequest, UploadStreamRequestBody};
use super::super::{Client, JANUS_API_VERSION, STREAM_UPLOAD_METHOD};
use super::Transaction;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct TransactionData {
    rtc_id: Uuid,
}

impl TransactionData {
    pub(crate) fn new(rtc_id: Uuid) -> Self {
        Self { rtc_id }
    }

    pub(crate) fn method(&self) -> &str {
        STREAM_UPLOAD_METHOD
    }

    pub(crate) fn rtc_id(&self) -> Uuid {
        self.rtc_id
    }
}

impl Client {
    pub(crate) fn upload_stream_request(
        &self,
        reqp: &IncomingRequestProperties,
        session_id: i64,
        handle_id: i64,
        body: UploadStreamRequestBody,
        to: &AgentId,
        start_timestamp: DateTime<Utc>,
    ) -> Result<OutgoingMessage<MessageRequest>> {
        let transaction = Transaction::UploadStream(TransactionData::new(body.id()));

        let payload = MessageRequest::new(
            &to_base64(&transaction)?,
            session_id,
            handle_id,
            serde_json::to_value(&body)?,
            None,
        );

        let mut props = OutgoingRequestProperties::new(
            "janus_conference_stream.upload",
            &self.response_topic(to)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        props.set_tracking(reqp.tracking().to_owned());

        self.register_transaction(
            to,
            start_timestamp,
            &props,
            &payload,
            self.timeout(STREAM_UPLOAD_METHOD),
        );

        Ok(OutgoingRequest::unicast(
            payload,
            props,
            to,
            JANUS_API_VERSION,
        ))
    }
}
