use async_std::stream;
use async_trait::async_trait;
use chrono::{serde::ts_seconds, DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::mqtt::{
    IncomingEventProperties, IntoPublishableMessage, OutgoingEvent, ShortTermTimingProperties,
};

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::config::TelemetryConfig;

#[derive(Debug, Deserialize)]
pub(crate) struct PullPayload {}

#[derive(Serialize, Debug)]
pub(crate) struct MetricValue {
    value: u64,
    #[serde(with = "ts_seconds")]
    timestamp: DateTime<Utc>,
}

#[derive(Serialize, Debug)]
#[serde(tag = "metric")]
pub(crate) enum Metric {
    //IncomingQueue(MetricValue),
    //OutgoingQueue(MetricValue),
    #[serde(rename(serialize = "apps.conference.db_connections_total"))]
    DbConnections(MetricValue),
}

pub(crate) struct PullHandler;

#[async_trait]
impl EventHandler for PullHandler {
    type Payload = PullPayload;

    async fn handle<C: Context>(
        context: &C,
        _payload: Self::Payload,
        evp: &IncomingEventProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        match context.config().telemetry {
            TelemetryConfig {
                id: Some(ref account_id),
            } => {
                let outgoing_event_payload = vec![Metric::DbConnections(MetricValue {
                    value: context.db().state().connections as u64,
                    timestamp: Utc::now(),
                })];

                let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
                let props = evp.to_event("metric.create", short_term_timing);
                let outgoing_event =
                    OutgoingEvent::multicast(outgoing_event_payload, props, account_id);
                let boxed_event =
                    Box::new(outgoing_event) as Box<dyn IntoPublishableMessage + Send>;
                Ok(Box::new(stream::once(boxed_event)))
            }

            _ => Ok(Box::new(stream::empty())),
        }
    }
}
