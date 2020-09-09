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
use crate::db;

#[derive(Debug, Deserialize)]
pub(crate) struct PullPayload {
    #[serde(default = "default_duration")]
    duration: u64,
}

fn default_duration() -> u64 {
    5
}

#[derive(Serialize, Debug, Copy, Clone)]
pub(crate) struct MetricValue<T: serde::Serialize> {
    value: T,
    #[serde(with = "ts_seconds")]
    timestamp: DateTime<Utc>,
}

impl<T: serde::Serialize> MetricValue<T> {
    fn new(value: T, timestamp: DateTime<Utc>) -> Self {
        Self { value, timestamp }
    }
}
#[derive(Serialize, Debug, Clone)]
#[serde(tag = "metric")]
pub(crate) enum Metric {
    #[serde(rename(serialize = "apps.conference.incoming_requests_total"))]
    IncomingQueueRequests(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.incoming_responses_total"))]
    IncomingQueueResponses(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.incoming_events_total"))]
    IncomingQueueEvents(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.outgoing_requests_total"))]
    OutgoingQueueRequests(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.outgoing_responses_total"))]
    OutgoingQueueResponses(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.outgoing_events_total"))]
    OutgoingQueueEvents(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.db_connections_total"))]
    DbConnections(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.idle_db_connections_total"))]
    IdleDbConnections(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.redis_connections_total"))]
    RedisConnections(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.idle_redis_connections_total"))]
    IdleRedisConnections(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.db_pool_checkin_average_total"))]
    DbPoolCheckinAverage(MetricValue<f64>),
    #[serde(rename(serialize = "apps.conference.max_db_pool_checkin_total"))]
    MaxDbPoolCheckin(MetricValue<u128>),
    #[serde(rename(serialize = "apps.conference.db_pool_checkout_average_total"))]
    DbPoolCheckoutAverage(MetricValue<f64>),
    #[serde(rename(serialize = "apps.conference.max_db_pool_checkout_total"))]
    MaxDbPoolCheckout(MetricValue<u128>),
    #[serde(rename(serialize = "apps.conference.db_pool_release_average_total"))]
    DbPoolReleaseAverage(MetricValue<f64>),
    #[serde(rename(serialize = "apps.conference.max_db_pool_release_total"))]
    MaxDbPoolRelease(MetricValue<u128>),
    #[serde(rename(serialize = "apps.conference.db_pool_timeout_average_total"))]
    DbPoolTimeoutAverage(MetricValue<f64>),
    #[serde(rename(serialize = "apps.conference.max_db_pool_timeout_total"))]
    MaxDbPoolTimeout(MetricValue<u128>),
    #[serde(rename(serialize = "apps.conference.online_janus_backends_total"))]
    OnlineJanusBackendsCount(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.janus_backends_capacity_total"))]
    JanusBackendTotalCapacity(MetricValue<u64>),
    #[serde(rename(serialize = "apps.conference.connected_agents_total"))]
    ConnectedAgentsCount(MetricValue<u64>),
    #[serde(serialize_with = "serialize_dynamic_metric")]
    Dynamic {
        key: String,
        value: MetricValue<u64>,
    },
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

                append_db_pool_stats(&mut metrics, context, now);

                append_dynamic_stats(&mut metrics, context, now)
                    .map_err(|err| err.to_string())
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

                append_janus_stats(&mut metrics, context, now)
                    .map_err(|err| err.to_string())
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

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

fn serialize_dynamic_metric<K, V, S>(
    key: K,
    value: V,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    K: std::fmt::Display,
    V: serde::Serialize,
    S: serde::ser::Serializer,
{
    use serde::ser::SerializeMap;

    let mut map = serializer.serialize_map(Some(2))?;
    map.serialize_entry("metric", &format!("app.conference.{}_total", key))?;
    map.serialize_entry("value", &value)?;
    map.end()
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
