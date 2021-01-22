use chrono::{serde::ts_seconds, DateTime, Utc};
use serde_derive::Serialize;
use svc_agent::{mqtt::ExtraTags, AgentId, Authenticable};

#[derive(Serialize, Clone)]
pub(crate) struct Metric {
    value: serde_json::Value,
    #[serde(flatten)]
    metric: MetricKey,
    #[serde(with = "ts_seconds")]
    timestamp: DateTime<Utc>,
    tags: Tags,
}

impl Metric {
    pub fn new(
        metric: MetricKey,
        value: impl serde::Serialize,
        timestamp: DateTime<Utc>,
        tags: Tags,
    ) -> Self {
        Self {
            metric,
            timestamp,
            tags,
            value: serde_json::to_value(value).unwrap_or(serde_json::value::Value::Null),
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub(crate) struct Metric2 {
    value: serde_json::Value,
    #[serde(flatten)]
    metric: MetricKey2,
    #[serde(with = "ts_seconds")]
    timestamp: DateTime<Utc>,
    tags: Tags,
}

impl Metric2 {
    pub fn tags(&self) -> &Tags {
        &self.tags
    }

    pub fn key(&self) -> &MetricKey2 {
        &self.metric
    }

    pub fn value(&self) -> &serde_json::Value {
        &self.value
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
pub enum Tags {
    Internal {
        version: String,
        agent_label: String,
        account_label: String,
        account_audience: String,
    },
    Queues {
        version: String,
        agent_label: String,
        account_label: String,
        account_audience: String,
        #[serde(flatten)]
        tags: ExtraTags,
    },
    Janus {
        version: String,
        agent_label: String,
        account_label: String,
        account_audience: String,
        backend_label: String,
    },
    RunningFuture {
        version: String,
        agent_label: String,
        account_label: String,
        account_audience: String,
        method: String,
    },
    Empty,
}

impl Tags {
    pub fn build_internal_tags(version: &str, agent_id: &AgentId) -> Self {
        Tags::Internal {
            version: version.to_owned(),
            agent_label: agent_id.label().to_owned(),
            account_label: agent_id.as_account_id().label().to_owned(),
            account_audience: agent_id.as_account_id().audience().to_owned(),
        }
    }

    pub fn build_queues_tags(version: &str, agent_id: &AgentId, tags: ExtraTags) -> Self {
        Tags::Queues {
            version: version.to_owned(),
            agent_label: agent_id.label().to_owned(),
            account_label: agent_id.as_account_id().label().to_owned(),
            account_audience: agent_id.as_account_id().audience().to_owned(),
            tags,
        }
    }

    pub fn build_janus_tags(version: &str, agent_id: &AgentId, janus_id: &AgentId) -> Self {
        Tags::Janus {
            version: version.to_owned(),
            agent_label: agent_id.label().to_owned(),
            account_label: agent_id.as_account_id().label().to_owned(),
            account_audience: agent_id.as_account_id().audience().to_owned(),
            backend_label: janus_id.label().to_owned(),
        }
    }

    pub fn build_running_futures_tags(version: &str, agent_id: &AgentId, method: String) -> Self {
        Tags::RunningFuture {
            version: version.to_owned(),
            agent_label: agent_id.label().to_owned(),
            account_label: agent_id.as_account_id().label().to_owned(),
            account_audience: agent_id.as_account_id().audience().to_owned(),
            method,
        }
    }
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "metric")]
pub(crate) enum MetricKey {
    #[serde(rename(serialize = "apps.conference.incoming_requests_total"))]
    IncomingQueueRequests,
    #[serde(rename(serialize = "apps.conference.incoming_responses_total"))]
    IncomingQueueResponses,
    #[serde(rename(serialize = "apps.conference.incoming_events_total"))]
    IncomingQueueEvents,
    #[serde(rename(serialize = "apps.conference.outgoing_requests_total"))]
    OutgoingQueueRequests,
    #[serde(rename(serialize = "apps.conference.outgoing_responses_total"))]
    OutgoingQueueResponses,
    #[serde(rename(serialize = "apps.conference.outgoing_events_total"))]
    OutgoingQueueEvents,
    #[serde(rename(serialize = "apps.conference.db_connections_total"))]
    DbConnections,
    #[serde(rename(serialize = "apps.conference.idle_db_connections_total"))]
    IdleDbConnections,
    #[serde(rename(serialize = "apps.conference.redis_connections_total"))]
    RedisConnections,
    #[serde(rename(serialize = "apps.conference.idle_redis_connections_total"))]
    IdleRedisConnections,
    #[serde(rename(serialize = "apps.conference.online_janus_backends_total"))]
    OnlineJanusBackendsCount,
    #[serde(rename(serialize = "apps.conference.janus_backends_capacity_total"))]
    JanusBackendTotalCapacity,
    #[serde(rename(serialize = "apps.conference.connected_agents_total"))]
    ConnectedAgentsCount,
    #[serde(rename(serialize = "apps.conference.janus_backend_reserve_load_total"))]
    JanusBackendReserveLoad,
    #[serde(rename(serialize = "apps.conference.janus_backend_agent_load_total"))]
    JanusBackendAgentLoad,
    #[serde(serialize_with = "serialize_dynamic_metric")]
    Dynamic(String),
    #[serde(rename(serialize = "apps.conference.running_requests_total"))]
    RunningRequests,
    #[serde(rename(serialize = "apps.conference.janus_timeouts_total"))]
    JanusTimeoutsTotal,
    #[serde(rename(serialize = "apps.conference.running_request_p95_microseconds"))]
    RunningRequestDurationP95,
    #[serde(rename(serialize = "apps.conference.running_request_p99_microseconds"))]
    RunningRequestDurationP99,
    #[serde(rename(serialize = "apps.conference.running_request_max_microseconds"))]
    RunningRequestDurationMax,
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "metric")]
pub(crate) enum MetricKey2 {
    #[serde(rename(serialize = "incoming_requests_total"))]
    IncomingQueueRequests,
    #[serde(rename(serialize = "incoming_responses_total"))]
    IncomingQueueResponses,
    #[serde(rename(serialize = "incoming_events_total"))]
    IncomingQueueEvents,
    #[serde(rename(serialize = "outgoing_requests_total"))]
    OutgoingQueueRequests,
    #[serde(rename(serialize = "outgoing_responses_total"))]
    OutgoingQueueResponses,
    #[serde(rename(serialize = "outgoing_events_total"))]
    OutgoingQueueEvents,
    #[serde(rename(serialize = "db_connections_total"))]
    DbConnections,
    #[serde(rename(serialize = "idle_db_connections_total"))]
    IdleDbConnections,
    #[serde(rename(serialize = "redis_connections_total"))]
    RedisConnections,
    #[serde(rename(serialize = "idle_redis_connections_total"))]
    IdleRedisConnections,
    #[serde(rename(serialize = "online_janus_backends_total"))]
    OnlineJanusBackendsCount,
    #[serde(rename(serialize = "janus_backends_capacity_total"))]
    JanusBackendTotalCapacity,
    #[serde(rename(serialize = "connected_agents_total"))]
    ConnectedAgentsCount,
    #[serde(rename(serialize = "janus_backend_reserve_load_total"))]
    JanusBackendReserveLoad,
    #[serde(rename(serialize = "janus_backend_agent_load_total"))]
    JanusBackendAgentLoad,
    #[serde(serialize_with = "serialize_dynamic_metric2")]
    Dynamic(String),
    #[serde(rename(serialize = "running_requests_total"))]
    RunningRequests,
    #[serde(rename(serialize = "janus_timeouts_total"))]
    JanusTimeoutsTotal,
    #[serde(rename(serialize = "running_request_p95_microseconds"))]
    RunningRequestDurationP95,
    #[serde(rename(serialize = "running_request_p99_microseconds"))]
    RunningRequestDurationP99,
    #[serde(rename(serialize = "running_request_max_microseconds"))]
    RunningRequestDurationMax,
}

impl From<MetricKey> for MetricKey2 {
    fn from(m: MetricKey) -> Self {
        match m {
            MetricKey::IncomingQueueRequests => MetricKey2::IncomingQueueRequests,
            MetricKey::IncomingQueueResponses => MetricKey2::IncomingQueueResponses,
            MetricKey::IncomingQueueEvents => MetricKey2::IncomingQueueEvents,
            MetricKey::OutgoingQueueRequests => MetricKey2::OutgoingQueueRequests,
            MetricKey::OutgoingQueueResponses => MetricKey2::OutgoingQueueResponses,
            MetricKey::OutgoingQueueEvents => MetricKey2::OutgoingQueueEvents,
            MetricKey::DbConnections => MetricKey2::DbConnections,
            MetricKey::IdleDbConnections => MetricKey2::IdleDbConnections,
            MetricKey::RedisConnections => MetricKey2::RedisConnections,
            MetricKey::IdleRedisConnections => MetricKey2::IdleRedisConnections,
            MetricKey::OnlineJanusBackendsCount => MetricKey2::OnlineJanusBackendsCount,
            MetricKey::JanusBackendTotalCapacity => MetricKey2::JanusBackendTotalCapacity,
            MetricKey::ConnectedAgentsCount => MetricKey2::ConnectedAgentsCount,
            MetricKey::Dynamic(key) => MetricKey2::Dynamic(key),
            MetricKey::JanusBackendReserveLoad => MetricKey2::JanusBackendReserveLoad,
            MetricKey::JanusBackendAgentLoad => MetricKey2::JanusBackendAgentLoad,
            MetricKey::RunningRequests => MetricKey2::RunningRequests,
            MetricKey::JanusTimeoutsTotal => MetricKey2::JanusTimeoutsTotal,
            MetricKey::RunningRequestDurationP95 => MetricKey2::RunningRequestDurationP95,
            MetricKey::RunningRequestDurationP99 => MetricKey2::RunningRequestDurationP99,
            MetricKey::RunningRequestDurationMax => MetricKey2::RunningRequestDurationMax,
        }
    }
}

impl std::fmt::Display for MetricKey2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricKey2::IncomingQueueRequests => write!(f, "incoming_requests_total"),
            MetricKey2::IncomingQueueResponses => write!(f, "incoming_responses_total"),
            MetricKey2::IncomingQueueEvents => write!(f, "incoming_events_total"),
            MetricKey2::OutgoingQueueRequests => write!(f, "outgoing_responses_total"),
            MetricKey2::OutgoingQueueResponses => write!(f, "outgoing_responses_total"),
            MetricKey2::OutgoingQueueEvents => write!(f, "outgoing_events_total"),
            MetricKey2::DbConnections => write!(f, "db_connections_total"),
            MetricKey2::IdleDbConnections => write!(f, "idle_db_connections_total"),
            MetricKey2::RedisConnections => write!(f, "redis_connections_total"),
            MetricKey2::IdleRedisConnections => write!(f, "idle_redis_connections_total"),
            MetricKey2::OnlineJanusBackendsCount => write!(f, "online_janus_backends_total"),
            MetricKey2::JanusBackendTotalCapacity => write!(f, "janus_backends_capacity_total"),
            MetricKey2::ConnectedAgentsCount => write!(f, "connected_agents_total"),
            MetricKey2::JanusBackendReserveLoad => write!(f, "janus_backend_reserve_load_total"),
            MetricKey2::JanusBackendAgentLoad => write!(f, "janus_backend_agent_load_total"),
            MetricKey2::Dynamic(key) => write!(f, "{}_total", key),
            MetricKey2::RunningRequests => write!(f, "running_requests_total"),
            MetricKey2::JanusTimeoutsTotal => write!(f, "janus_timeouts_total"),
            MetricKey2::RunningRequestDurationP95 => {
                write!(f, "running_request_duration_p95_microseconds")
            }
            MetricKey2::RunningRequestDurationP99 => {
                write!(f, "running_request_duration_p99_microseconds")
            }
            MetricKey2::RunningRequestDurationMax => {
                write!(f, "running_request_duration_max_microseconds")
            }
        }
    }
}

impl From<Metric> for Metric2 {
    fn from(m: Metric) -> Self {
        let Metric {
            metric,
            value,
            timestamp,
            tags,
        } = m;
        Self {
            value,
            timestamp,
            tags,
            metric: metric.into(),
        }
    }
}

fn serialize_dynamic_metric<S>(
    metric_key: &str,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    use serde::ser::SerializeMap;

    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry("metric", &format!("apps.conference.{}_total", metric_key))?;
    map.end()
}

fn serialize_dynamic_metric2<S>(
    metric_key: &str,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    use serde::ser::SerializeMap;

    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry("metric", &format!("{}_total", metric_key))?;
    map.end()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::offset::TimeZone;
    use serde_derive::Deserialize;
    use svc_agent::AccountId;

    #[derive(Deserialize)]
    struct DynamicMetric {
        metric: String,
        value: u64,
        #[serde(with = "ts_seconds")]
        timestamp: DateTime<Utc>,
    }

    #[test]
    fn serialize_dynamic_metric() {
        let now = Utc::now();

        let json = serde_json::json!(Metric::new(
            MetricKey::Dynamic("example".into()),
            123,
            now,
            Tags::build_internal_tags("whatever", &AgentId::new("q", AccountId::new("foo", "bar")))
        ));

        let parsed: DynamicMetric =
            serde_json::from_str(&json.to_string()).expect("Failed to parse json");

        assert_eq!(&parsed.metric, "apps.conference.example_total");
        assert_eq!(parsed.value, 123);
        assert_eq!(parsed.timestamp, Utc.timestamp(now.timestamp(), 0));
    }
}
