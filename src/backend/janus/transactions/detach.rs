use anyhow::Result;
use chrono::Utc;
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        IncomingEventProperties, OutgoingMessage, OutgoingRequest, OutgoingRequestProperties,
        ShortTermTimingProperties, TrackingProperties,
    },
    AgentId,
};

use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::DetachRequest;
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_conference_agent.detach";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct TransactionData {
    evp: IncomingEventProperties,
}

impl TransactionData {
    pub(crate) fn new(evp: IncomingEventProperties) -> Self {
        Self { evp }
    }
}

impl Client {
    pub(crate) fn detach_request(
        &self,
        evp: IncomingEventProperties,
        session_id: i64,
        handle_id: i64,
        to: &AgentId,
        tracking: &TrackingProperties,
    ) -> Result<OutgoingMessage<DetachRequest>> {
        let start_timestamp = Utc::now();

        let mut props = OutgoingRequestProperties::new(
            METHOD,
            &self.response_topic(to)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::new(start_timestamp),
        );

        props.set_tracking(tracking.to_owned());
        let transaction = Transaction::Detach(TransactionData::new(evp));
        let payload = DetachRequest::new(&to_base64(&transaction)?, session_id, handle_id);
        self.register_transaction(to, start_timestamp, &props, &payload, self.timeout(METHOD));
        Ok(OutgoingRequest::unicast(
            payload,
            props,
            to,
            JANUS_API_VERSION,
        ))
    }
}
