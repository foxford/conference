use chrono::{serde::ts_seconds, DateTime, Utc};
use serde_derive::Serialize;

#[derive(Serialize, Copy, Clone)]
pub(crate) struct MetricValue<T: serde::Serialize> {
    value: T,
    #[serde(with = "ts_seconds")]
    timestamp: DateTime<Utc>,
}

impl<T: serde::Serialize> MetricValue<T> {
    pub fn new(value: T, timestamp: DateTime<Utc>) -> Self {
        Self { value, timestamp }
    }
}

#[derive(Serialize, Clone)]
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
#[derive(Serialize, Clone)]
#[serde(tag = "metric")]
pub(crate) enum Metric2 {
    #[serde(rename(serialize = "incoming_requests_total"))]
    IncomingQueueRequests(MetricValue<u64>),
    #[serde(rename(serialize = "incoming_responses_total"))]
    IncomingQueueResponses(MetricValue<u64>),
    #[serde(rename(serialize = "incoming_events_total"))]
    IncomingQueueEvents(MetricValue<u64>),
    #[serde(rename(serialize = "outgoing_requests_total"))]
    OutgoingQueueRequests(MetricValue<u64>),
    #[serde(rename(serialize = "outgoing_responses_total"))]
    OutgoingQueueResponses(MetricValue<u64>),
    #[serde(rename(serialize = "outgoing_events_total"))]
    OutgoingQueueEvents(MetricValue<u64>),
    #[serde(rename(serialize = "db_connections_total"))]
    DbConnections(MetricValue<u64>),
    #[serde(rename(serialize = "idle_db_connections_total"))]
    IdleDbConnections(MetricValue<u64>),
    #[serde(rename(serialize = "redis_connections_total"))]
    RedisConnections(MetricValue<u64>),
    #[serde(rename(serialize = "idle_redis_connections_total"))]
    IdleRedisConnections(MetricValue<u64>),
    #[serde(rename(serialize = "db_pool_checkin_average_total"))]
    DbPoolCheckinAverage(MetricValue<f64>),
    #[serde(rename(serialize = "max_db_pool_checkin_total"))]
    MaxDbPoolCheckin(MetricValue<u128>),
    #[serde(rename(serialize = "db_pool_checkout_average_total"))]
    DbPoolCheckoutAverage(MetricValue<f64>),
    #[serde(rename(serialize = "max_db_pool_checkout_total"))]
    MaxDbPoolCheckout(MetricValue<u128>),
    #[serde(rename(serialize = "db_pool_release_average_total"))]
    DbPoolReleaseAverage(MetricValue<f64>),
    #[serde(rename(serialize = "max_db_pool_release_total"))]
    MaxDbPoolRelease(MetricValue<u128>),
    #[serde(rename(serialize = "db_pool_timeout_average_total"))]
    DbPoolTimeoutAverage(MetricValue<f64>),
    #[serde(rename(serialize = "max_db_pool_timeout_total"))]
    MaxDbPoolTimeout(MetricValue<u128>),
    #[serde(rename(serialize = "online_janus_backends_total"))]
    OnlineJanusBackendsCount(MetricValue<u64>),
    #[serde(rename(serialize = "janus_backends_capacity_total"))]
    JanusBackendTotalCapacity(MetricValue<u64>),
    #[serde(rename(serialize = "connected_agents_total"))]
    ConnectedAgentsCount(MetricValue<u64>),
    #[serde(serialize_with = "serialize_dynamic_metric")]
    Dynamic {
        key: String,
        value: MetricValue<u64>,
    },
}

impl From<Metric> for Metric2 {
    fn from(m: Metric) -> Self {
        match m {
            Metric::IncomingQueueRequests(v) => Metric2::IncomingQueueRequests(v),
            Metric::IncomingQueueResponses(v) => Metric2::IncomingQueueResponses(v),
            Metric::IncomingQueueEvents(v) => Metric2::IncomingQueueEvents(v),
            Metric::OutgoingQueueRequests(v) => Metric2::OutgoingQueueRequests(v),
            Metric::OutgoingQueueResponses(v) => Metric2::OutgoingQueueResponses(v),
            Metric::OutgoingQueueEvents(v) => Metric2::OutgoingQueueEvents(v),
            Metric::DbConnections(v) => Metric2::DbConnections(v),
            Metric::IdleDbConnections(v) => Metric2::IdleDbConnections(v),
            Metric::RedisConnections(v) => Metric2::RedisConnections(v),
            Metric::IdleRedisConnections(v) => Metric2::IdleRedisConnections(v),
            Metric::DbPoolCheckinAverage(v) => Metric2::DbPoolCheckinAverage(v),
            Metric::MaxDbPoolCheckin(v) => Metric2::MaxDbPoolCheckin(v),
            Metric::DbPoolCheckoutAverage(v) => Metric2::DbPoolCheckoutAverage(v),
            Metric::MaxDbPoolCheckout(v) => Metric2::MaxDbPoolCheckout(v),
            Metric::DbPoolReleaseAverage(v) => Metric2::DbPoolReleaseAverage(v),
            Metric::MaxDbPoolRelease(v) => Metric2::MaxDbPoolRelease(v),
            Metric::DbPoolTimeoutAverage(v) => Metric2::DbPoolTimeoutAverage(v),
            Metric::MaxDbPoolTimeout(v) => Metric2::MaxDbPoolTimeout(v),
            Metric::OnlineJanusBackendsCount(v) => Metric2::OnlineJanusBackendsCount(v),
            Metric::JanusBackendTotalCapacity(v) => Metric2::JanusBackendTotalCapacity(v),
            Metric::ConnectedAgentsCount(v) => Metric2::ConnectedAgentsCount(v),
            Metric::Dynamic { key, value } => Metric2::Dynamic { key, value },
        }
    }
}

fn serialize_dynamic_metric<K, V, S>(
    key: K,
    metric_value: &MetricValue<V>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    K: std::fmt::Display,
    V: serde::Serialize,
    S: serde::ser::Serializer,
{
    use serde::ser::SerializeMap;

    let mut map = serializer.serialize_map(Some(3))?;
    map.serialize_entry("metric", &format!("apps.conference.{}_total", key))?;
    map.serialize_entry("value", &metric_value.value)?;
    map.serialize_entry("timestamp", &metric_value.timestamp)?;
    map.end()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_derive::Deserialize;

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
