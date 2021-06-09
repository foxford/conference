// use anyhow::Result;
// use chrono::{DateTime, Duration, Utc};
// use serde_derive::{Deserialize, Serialize};
// use serde_json::Value as JsonValue;
// use svc_agent::{
//     mqtt::{
//         IncomingRequestProperties, OutgoingMessage, OutgoingRequest, ShortTermTimingProperties,
//     },
//     Addressable, AgentId,
// };
// use uuid::Uuid;

// use crate::util::{generate_correlation_data, to_base64};

// use super::super::requests::{MessageRequest, ReadStreamRequestBody};
// use super::super::{Client, JANUS_API_VERSION};
// use super::Transaction;

// const METHOD: &str = "janus_conference_stream.create";

// ////////////////////////////////////////////////////////////////////////////////

// #[derive(Debug, Deserialize, Serialize)]
// pub(crate) struct TransactionData {
//     reqp: IncomingRequestProperties,
// }

// impl TransactionData {
//     pub(crate) fn new(reqp: IncomingRequestProperties) -> Self {
//         Self { reqp }
//     }

//     pub(crate) fn reqp(&self) -> &IncomingRequestProperties {
//         &self.reqp
//     }
// }

// #[allow(clippy::too_many_arguments)]
// impl Client {
//     pub(crate) fn read_stream_request(
//         &self,
//         reqp: IncomingRequestProperties,
//         session_id: i64,
//         handle_id: i64,
//         rtc_id: Uuid,
//         jsep: JsonValue,
//         to: &AgentId,
//         start_timestamp: DateTime<Utc>,
//         authz_time: Duration,
//     ) -> Result<OutgoingMessage<MessageRequest>> {
//         let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
//         short_term_timing.set_authorization_time(authz_time);

//         let props = reqp.to_request(
//             METHOD,
//             &self.response_topic(to)?,
//             &generate_correlation_data(),
//             short_term_timing,
//         );

//         let agent_id = reqp.as_agent_id().to_owned();
//         let body = ReadStreamRequestBody::new(rtc_id, agent_id);
//         // let transaction = Transaction::ReadStream(TransactionData::new(reqp));

//         let payload = MessageRequest::new(
//             &to_base64(&transaction)?,
//             session_id,
//             handle_id,
//             serde_json::to_value(&body)?,
//             Some(jsep),
//         );

//         self.register_transaction(to, start_timestamp, &props, &payload, self.timeout(METHOD));

//         Ok(OutgoingRequest::unicast(
//             payload,
//             props,
//             to,
//             JANUS_API_VERSION,
//         ))
//     }
// }
