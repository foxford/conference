use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_derive::Deserialize;
use svc_agent::mqtt::{
    IncomingEventProperties, IntoPublishableMessage, OutgoingEvent, ResponseStatus,
    ShortTermTimingProperties,
};

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::metrics::{Metric, Metric2, MetricValue};
use crate::config::TelemetryConfig;
use crate::db;

#[derive(Debug, Deserialize)]
pub(crate) struct PullPayload {
    #[serde(default = "default_duration")]
    duration: u64,
}

fn default_duration() -> u64 {
    5
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
                        Metric::IncomingQueueRequests(MetricValue::new(
                            stats.incoming_requests,
                            now,
                        )),
                        Metric::IncomingQueueResponses(MetricValue::new(
                            stats.incoming_responses,
                            now,
                        )),
                        Metric::IncomingQueueEvents(MetricValue::new(stats.incoming_events, now)),
                        Metric::OutgoingQueueRequests(MetricValue::new(
                            stats.outgoing_requests,
                            now,
                        )),
                        Metric::OutgoingQueueResponses(MetricValue::new(
                            stats.outgoing_responses,
                            now,
                        )),
                        Metric::OutgoingQueueEvents(MetricValue::new(stats.outgoing_events, now)),
                    ]
                } else {
                    vec![]
                };

                let db_state = context.db().state();
                metrics.push(Metric::DbConnections(MetricValue::new(
                    db_state.connections as u64,
                    now,
                )));

                metrics.push(Metric::IdleDbConnections(MetricValue::new(
                    db_state.idle_connections as u64,
                    now,
                )));

                if let Some(pool) = context.redis_pool() {
                    let pool_state = pool.state();
                    metrics.push(Metric::RedisConnections(MetricValue::new(
                        pool_state.connections as u64,
                        now,
                    )));

                    metrics.push(Metric::IdleRedisConnections(MetricValue::new(
                        pool_state.idle_connections as u64,
                        now,
                    )));
                }

                append_db_pool_stats(&mut metrics, context, now);

                append_dynamic_stats(&mut metrics, context, now)
                    .map_err(|err| err.to_string())
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

                append_janus_stats(&mut metrics, context, now)
                    .map_err(|err| err.to_string())
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

                let metrics2 = metrics
                    .clone()
                    .into_iter()
                    .map(|m| m.into())
                    .collect::<Vec<Metric2>>();

                let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
                let props = evp.to_event("metric.create", short_term_timing.clone());
                let props2 = evp.to_event("metric.create", short_term_timing);

                let outgoing_event = OutgoingEvent::multicast(metrics, props, account_id);
                let outgoing_event2 = OutgoingEvent::multicast(metrics2, props2, account_id);

                let boxed_events = vec![
                    Box::new(outgoing_event) as Box<dyn IntoPublishableMessage + Send>,
                    Box::new(outgoing_event2) as Box<dyn IntoPublishableMessage + Send>,
                ];
                Ok(Box::new(stream::from_iter(boxed_events)))
            }

            _ => Ok(Box::new(stream::empty())),
        }
    }
}

fn append_db_pool_stats(metrics: &mut Vec<Metric>, context: &dyn Context, now: DateTime<Utc>) {
    if let Some(db_pool_stats) = context.db_pool_stats() {
        let stats = db_pool_stats.get_stats();

        let m = [
            Metric::DbPoolCheckinAverage(MetricValue::new(stats.avg_checkin, now)),
            Metric::MaxDbPoolCheckin(MetricValue::new(stats.max_checkin, now)),
            Metric::DbPoolCheckoutAverage(MetricValue::new(stats.avg_checkout, now)),
            Metric::MaxDbPoolCheckout(MetricValue::new(stats.max_checkout, now)),
            Metric::DbPoolTimeoutAverage(MetricValue::new(stats.avg_timeout, now)),
            Metric::MaxDbPoolTimeout(MetricValue::new(stats.max_timeout, now)),
            Metric::DbPoolReleaseAverage(MetricValue::new(stats.avg_release, now)),
            Metric::MaxDbPoolRelease(MetricValue::new(stats.max_release, now)),
        ];

        metrics.extend_from_slice(&m);
    }
}

fn append_dynamic_stats(
    metrics: &mut Vec<Metric>,
    context: &dyn Context,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    if let Some(dynamic_stats) = context.dynamic_stats() {
        for (key, value) in dynamic_stats.flush()? {
            metrics.push(Metric::Dynamic {
                key,
                value: MetricValue::new(value as u64, now),
            });
        }
    }

    Ok(())
}

fn append_janus_stats(
    metrics: &mut Vec<Metric>,
    context: &dyn Context,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    use anyhow::Context;

    let conn = context.db().get()?;

    // The number of online janus backends.
    let online_backends_count =
        db::janus_backend::count(&conn).context("Failed to get janus backends count")?;

    let value = MetricValue::new(online_backends_count as u64, now);
    metrics.push(Metric::OnlineJanusBackendsCount(value));

    // Total capacity of online janus backends.
    let total_capacity = db::janus_backend::total_capacity(&conn)
        .context("Failed to get janus backends total capacity")?;

    let value = MetricValue::new(total_capacity as u64, now);
    metrics.push(Metric::JanusBackendTotalCapacity(value));

    // The number of agents connect to an RTC.
    let connected_agents_count = db::agent::CountQuery::new()
        .status(db::agent::Status::Connected)
        .execute(&conn)
        .context("Failed to get connected agents count")?;

    let value = MetricValue::new(connected_agents_count as u64, now);
    metrics.push(Metric::ConnectedAgentsCount(value));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct DynamicMetric {
        metric: String,
        value: u64,
        timestamp: DateTime<Utc>,
    }

    #[test]
    fn serialize_dynamic_metric() {
        let now = Utc::now();

        let json = serde_json::json!(Metric::Dynamic {
            key: String::from("example"),
            value: MetricValue::new(123, now),
        });

        let parsed: DynamicMetric =
            serde_json::from_str(&json.to_string()).expect("Failed to parse json");

        assert_eq!(&parsed.metric, "apps.conference.example_total");
        assert_eq!(parsed.value, 123);
        assert_eq!(parsed.timestamp, now);
    }
}
