use anyhow::Result;
use chrono::Utc;
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        OutgoingMessage, OutgoingRequest, OutgoingRequestProperties, ShortTermTimingProperties,
    },
    AgentId,
};

use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::CreateHandleRequest;
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_handle.create";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct TransactionData {}

impl TransactionData {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::too_many_arguments)]
impl Client {
    pub(crate) fn create_pool_handle_request(
        &self,
        backend_id: &AgentId,
        session_id: i64,
    ) -> Result<OutgoingMessage<CreateHandleRequest>> {
        let start_timestamp = Utc::now();

        let props = OutgoingRequestProperties::new(
            METHOD,
            &self.response_topic(backend_id)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        let transaction = Transaction::CreatePoolHandle(TransactionData::new());

        let payload = CreateHandleRequest::new(
            &to_base64(&transaction)?,
            session_id,
            "janus.plugin.conference",
        );

        self.register_transaction(
            backend_id,
            start_timestamp,
            &props,
            &payload,
            self.timeout(METHOD),
        );

        Ok(OutgoingRequest::unicast(
            payload,
            props,
            backend_id,
            JANUS_API_VERSION,
        ))
    }
}
