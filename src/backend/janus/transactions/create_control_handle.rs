use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        IncomingResponseProperties, OutgoingMessage, OutgoingRequest, OutgoingRequestProperties,
        ShortTermTimingProperties,
    },
    Addressable,
};

use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::CreateHandleRequest;
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_handle.create";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct TransactionData {
    session_id: i64,
    capacity: i32,
    balancer_capacity: i32,
}

impl TransactionData {
    pub(crate) fn new(session_id: i64, capacity: i32, balancer_capacity: i32) -> Self {
        Self {
            session_id,
            capacity,
            balancer_capacity,
        }
    }

    pub(crate) fn session_id(&self) -> i64 {
        self.session_id
    }

    pub(crate) fn capacity(&self) -> i32 {
        self.capacity
    }

    pub(crate) fn balancer_capacity(&self) -> i32 {
        self.balancer_capacity
    }
}

////////////////////////////////////////////////////////////////////////////////

impl Client {
    pub(crate) fn create_control_handle_request(
        &self,
        respp: &IncomingResponseProperties,
        session_id: i64,
        capacity: i32,
        balancer_capacity: i32,
        start_timestamp: DateTime<Utc>,
    ) -> Result<OutgoingMessage<CreateHandleRequest>> {
        let to = respp.as_agent_id();
        let tn_data = TransactionData::new(session_id, capacity, balancer_capacity);
        let transaction = Transaction::CreateControlHandle(tn_data);

        let payload = CreateHandleRequest::new(
            &to_base64(&transaction)?,
            session_id,
            "janus.plugin.conference",
        );

        let mut props = OutgoingRequestProperties::new(
            METHOD,
            &self.response_topic(to)?,
            &generate_correlation_data(),
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        props.set_tracking(respp.tracking().to_owned());
        self.register_transaction(to, start_timestamp, &props, &payload, self.timeout(METHOD));

        Ok(OutgoingRequest::unicast(
            payload,
            props,
            to,
            JANUS_API_VERSION,
        ))
    }
}
