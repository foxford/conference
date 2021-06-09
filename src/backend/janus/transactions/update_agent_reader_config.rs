// use anyhow::Result;
// use chrono::{DateTime, Utc};
// use svc_agent::mqtt::{
//     IncomingRequestProperties, OutgoingMessage, OutgoingRequest, ShortTermTimingProperties,
// };

// use crate::db::janus_backend::Object as JanusBackend;
// use crate::db::rtc::Object as Rtc;
// use crate::db::rtc_reader_config::Object as RtcReaderConfig;
// use crate::util::{generate_correlation_data, to_base64};

// use super::super::requests::{
//     MessageRequest, UpdateReaderConfigRequestBody, UpdateReaderConfigRequestBodyConfigItem,
// };
// use super::super::{Client, JANUS_API_VERSION};
// use super::Transaction;

// const METHOD: &str = "janus_conference_rtc_reader_config.update";

// ////////////////////////////////////////////////////////////////////////////////

// impl Client {
//     pub(crate) fn update_agent_reader_config_request(
//         &self,
//         reqp: IncomingRequestProperties,
//         backend: &JanusBackend,
//         rtc_reader_configs_with_rtcs: &[(RtcReaderConfig, Rtc)],
//         start_timestamp: DateTime<Utc>,
//     ) -> Result<OutgoingMessage<MessageRequest>> {
//         let to = backend.id();
//         let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);

//         let props = reqp.to_request(
//             METHOD,
//             &self.response_topic(to)?,
//             &generate_correlation_data(),
//             short_term_timing,
//         );

//         let items = rtc_reader_configs_with_rtcs
//             .iter()
//             .map(|(rtc_reader_config, rtc)| {
//                 UpdateReaderConfigRequestBodyConfigItem::new(
//                     rtc_reader_config.reader_id().to_owned(),
//                     rtc.id(),
//                     rtc_reader_config.receive_video(),
//                     rtc_reader_config.receive_audio(),
//                 )
//             })
//             .collect::<Vec<UpdateReaderConfigRequestBodyConfigItem>>();

//         let body = UpdateReaderConfigRequestBody::new(items);

//         let payload = MessageRequest::new(
//             &to_base64(&Transaction::UpdateReaderConfig)?,
//             backend.session_id(),
//             backend.handle_id(),
//             serde_json::to_value(&body)?,
//             None,
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
