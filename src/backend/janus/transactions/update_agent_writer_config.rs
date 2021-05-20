use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use svc_agent::mqtt::{
    IncomingRequestProperties, OutgoingMessage, OutgoingRequest, ShortTermTimingProperties,
};

use crate::db::janus_backend::Object as JanusBackend;
use crate::db::rtc::Object as Rtc;
use crate::db::rtc_writer_config::Object as RtcWriterConfig;
use crate::util::{generate_correlation_data, to_base64};

use super::super::requests::{
    MessageRequest, UpdateWriterConfigRequestBody, UpdateWriterConfigRequestBodyConfigItem,
};
use super::super::{Client, JANUS_API_VERSION};
use super::Transaction;

const METHOD: &str = "janus_conference_rtc_writer_config.update";

////////////////////////////////////////////////////////////////////////////////

impl Client {
    pub(crate) fn update_agent_writer_config_request(
        &self,
        reqp: IncomingRequestProperties,
        backend: &JanusBackend,
        rtc_writer_configs_with_rtcs: &[(RtcWriterConfig, Rtc)],
        start_timestamp: DateTime<Utc>,
        maybe_authz_time: Option<Duration>,
    ) -> Result<OutgoingMessage<MessageRequest>> {
        let to = backend.id();
        let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
        
        if let Some(authz_time) = maybe_authz_time {
            short_term_timing.set_authorization_time(authz_time);
        }

        let props = reqp.to_request(
            METHOD,
            &self.response_topic(to)?,
            &generate_correlation_data(),
            short_term_timing,
        );

        let items = rtc_writer_configs_with_rtcs
            .iter()
            .map(|(rtc_writer_config, rtc)| {
                let mut req = UpdateWriterConfigRequestBodyConfigItem::new(
                    rtc.id(),
                    rtc_writer_config.send_video(),
                    rtc_writer_config.send_audio(),
                );

                if let Some(video_remb) = rtc_writer_config.video_remb() {
                    req.set_video_remb(video_remb as u32);
                }

                req
            })
            .collect::<Vec<UpdateWriterConfigRequestBodyConfigItem>>();

        let body = UpdateWriterConfigRequestBody::new(items);

        let payload = MessageRequest::new(
            &to_base64(&Transaction::UpdateWriterConfig)?,
            backend.session_id(),
            backend.handle_id(),
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
