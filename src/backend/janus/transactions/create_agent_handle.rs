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
use uuid::Uuid;

use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::CreateHandleRequest;
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_handle.create";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct TransactionData {
    reqp: IncomingRequestProperties,
    session_id: i64,
    room_id: Uuid,
    jsep: JsonValue,
}

impl TransactionData {
    pub(crate) fn new(
        reqp: IncomingRequestProperties,
        room_id: Uuid,
        session_id: i64,
        jsep: JsonValue,
    ) -> Self {
        Self {
            reqp,
            session_id,
            room_id,
            jsep,
        }
    }

    pub(crate) fn reqp(&self) -> &IncomingRequestProperties {
        &self.reqp
    }

    pub(crate) fn room_id(&self) -> Uuid {
        self.room_id
    }

    pub(crate) fn session_id(&self) -> i64 {
        self.session_id
    }

    pub(crate) fn jsep(&self) -> &JsonValue {
        &self.jsep
    }
}

////////////////////////////////////////////////////////////////////////////////

impl Client {
    pub(crate) fn create_agent_handle_request(
        &self,
        reqp: IncomingRequestProperties,
        room_id: Uuid,
        session_id: i64,
        jsep: JsonValue,
        to: &AgentId,
        start_timestamp: DateTime<Utc>,
    ) -> Result<OutgoingMessage<CreateHandleRequest>> {
        let props = reqp.to_request(
            METHOD,
            &self.response_topic(to)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        let tn_data = TransactionData::new(reqp, room_id, session_id, jsep);
        let transaction = Transaction::CreateAgentHandle(tn_data);

        let payload = CreateHandleRequest::new(
            &to_base64(&transaction)?,
            session_id,
            "janus.plugin.conference",
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
