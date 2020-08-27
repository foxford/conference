use async_std::stream;
use async_trait::async_trait;
use chrono::{serde::ts_seconds, DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use svc_agent::mqtt::{
    IncomingEventProperties, IntoPublishableMessage, OutgoingEvent, ResponseStatus,
    ShortTermTimingProperties,
};

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::config::TelemetryConfig;

#[derive(Debug, Deserialize)]
pub(crate) struct PullPayload {
    #[serde(default = "default_duration")]
    duration: u64,
}

fn default_duration() -> u64 {
    5
}

#[derive(Serialize, Debug)]
pub(crate) struct MetricValue {
    value: u64,
    #[serde(with = "ts_seconds")]
    timestamp: DateTime<Utc>,
}

#[derive(Serialize, Debug)]
#[serde(tag = "metric")]
pub(crate) enum Metric {
    #[serde(rename(serialize = "apps.conference.incoming_requests_total"))]
    IncomingQueueRequests(MetricValue),
    #[serde(rename(serialize = "apps.conference.incoming_responses_total"))]
    IncomingQueueResponses(MetricValue),
    #[serde(rename(serialize = "apps.conference.incoming_events_total"))]
    IncomingQueueEvents(MetricValue),
    #[serde(rename(serialize = "apps.conference.outgoing_requests_total"))]
    OutgoingQueueRequests(MetricValue),
    #[serde(rename(serialize = "apps.conference.outgoing_responses_total"))]
    OutgoingQueueResponses(MetricValue),
    #[serde(rename(serialize = "apps.conference.outgoing_events_total"))]
    OutgoingQueueEvents(MetricValue),
    #[serde(rename(serialize = "apps.conference.db_connections_total"))]
    DbConnections(MetricValue),
    #[serde(rename(serialize = "apps.conference.idle_db_connections_total"))]
    IdleDbConnections(MetricValue),
    #[serde(rename(serialize = "apps.conference.redis_connections_total"))]
    RedisConnections(MetricValue),
    #[serde(rename(serialize = "apps.conference.idle_redis_connections_total"))]
    IdleRedisConnections(MetricValue),
}

pub(crate) struct PullHandler;

#[async_trait]
impl EventHandler for PullHandler {
    type Payload = PullPayload;

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        match context.config().telemetry {
            TelemetryConfig {
                id: Some(ref account_id),
            } => {
                let now = Utc::now();

                let mut metrics = if let Some(qc) = context.queue_counter() {
                    let stats = qc
                        .get_stats(payload.duration)
                        .status(ResponseStatus::BAD_REQUEST)?;

                    vec![
                        Metric::IncomingQueueRequests(MetricValue {
                            value: stats.incoming_requests,
                            timestamp: now,
                        }),
                        Metric::IncomingQueueResponses(MetricValue {
                            value: stats.incoming_responses,
                            timestamp: now,
                        }),
                        Metric::IncomingQueueEvents(MetricValue {
                            value: stats.incoming_events,
                            timestamp: now,
                        }),
                        Metric::OutgoingQueueRequests(MetricValue {
                            value: stats.outgoing_requests,
                            timestamp: now,
                        }),
                        Metric::OutgoingQueueResponses(MetricValue {
                            value: stats.outgoing_responses,
                            timestamp: now,
                        }),
                        Metric::OutgoingQueueEvents(MetricValue {
                            value: stats.outgoing_events,
                            timestamp: now,
                        }),
                    ]
                } else {
                    vec![]
                };

                let db_state = context.db().state();
                metrics.push(Metric::DbConnections(MetricValue {
                    value: db_state.connections as u64,
                    timestamp: now,
                }));

                metrics.push(Metric::IdleDbConnections(MetricValue {
                    value: db_state.idle_connections as u64,
                    timestamp: now,
                }));

                if let Some(pool) = context.redis_pool() {
                    let pool_state = pool.state();
                    metrics.push(Metric::RedisConnections(MetricValue {
                        value: pool_state.connections as u64,
                        timestamp: now,
                    }));

                    metrics.push(Metric::IdleRedisConnections(MetricValue {
                        value: pool_state.idle_connections as u64,
                        timestamp: now,
                    }));
                }

                let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
                let props = evp.to_event("metric.create", short_term_timing);
                let outgoing_event = OutgoingEvent::multicast(metrics, props, account_id);
                let boxed_event =
                    Box::new(outgoing_event) as Box<dyn IntoPublishableMessage + Send>;
                Ok(Box::new(stream::once(boxed_event)))
            }

            _ => Ok(Box::new(stream::empty())),
        }
    }
}
