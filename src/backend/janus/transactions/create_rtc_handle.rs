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

use super::super::requests::CreateHandleRequest;
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_handle.create";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct TransactionData {
    reqp: IncomingRequestProperties,
    rtc_stream_id: Uuid,
    rtc_id: Uuid,
    room_id: Uuid,
    session_id: i64,
}

impl TransactionData {
    pub(crate) fn new(
        reqp: IncomingRequestProperties,
        rtc_stream_id: Uuid,
        rtc_id: Uuid,
        room_id: Uuid,
        session_id: i64,
    ) -> Self {
        Self {
            reqp,
            rtc_stream_id,
            rtc_id,
            room_id,
            session_id,
        }
    }

    pub(crate) fn reqp(&self) -> &IncomingRequestProperties {
        &self.reqp
    }

    pub(crate) fn rtc_stream_id(&self) -> Uuid {
        self.rtc_stream_id
    }

    pub(crate) fn rtc_id(&self) -> Uuid {
        self.rtc_id
    }

    pub(crate) fn room_id(&self) -> Uuid {
        self.room_id
    }

    pub(crate) fn session_id(&self) -> i64 {
        self.session_id
    }
}

////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::too_many_arguments)]
impl Client {
    pub(crate) fn create_rtc_handle_request(
        &self,
        reqp: IncomingRequestProperties,
        rtc_stream_id: Uuid,
        rtc_id: Uuid,
        room_id: Uuid,
        session_id: i64,
        to: &AgentId,
        start_timestamp: DateTime<Utc>,
        authz_time: Duration,
    ) -> Result<OutgoingMessage<CreateHandleRequest>> {
        let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
        short_term_timing.set_authorization_time(authz_time);

        let props = reqp.to_request(
            METHOD,
            &self.response_topic(to)?,
            &generate_correlation_data(),
            short_term_timing,
        );

        let transaction = Transaction::CreateRtcHandle(TransactionData::new(
            reqp,
            rtc_stream_id,
            rtc_id,
            room_id,
            session_id,
        ));

        let payload = CreateHandleRequest::new(
            &to_base64(&transaction)?,
            session_id,
            "janus.plugin.conference",
            Some(&rtc_stream_id.to_string()),
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
