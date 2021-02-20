use anyhow::Result;
use chrono::Utc;
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        OutgoingMessage, OutgoingRequest, OutgoingRequestProperties, ShortTermTimingProperties,
        TrackingProperties,
    },
    AgentId,
};

use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::{AgentLeaveRequestBody, MessageRequest};
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_conference_agent.leave";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct TransactionData;

impl TransactionData {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl Client {
    pub(crate) fn agent_leave_request(
        &self,
        session_id: i64,
        handle_id: i64,
        agent_id: &AgentId,
        to: &AgentId,
        tracking: &TrackingProperties,
    ) -> Result<OutgoingMessage<MessageRequest>> {
        let start_timestamp = Utc::now();

        let mut props = OutgoingRequestProperties::new(
            METHOD,
            &self.response_topic(to)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::new(start_timestamp),
        );

        props.set_tracking(tracking.to_owned());

        let transaction = Transaction::AgentLeave(TransactionData::new());
        let body = AgentLeaveRequestBody::new(agent_id.to_owned());

        let payload = MessageRequest::new(
            &to_base64(&transaction)?,
            session_id,
            handle_id,
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
