use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use prometheus::{Histogram, IntGauge, IntGaugeVec, Opts, Registry};
use slog::error;

use crate::db::{agent_connection, ConnectionPool};

pub trait HistogramExt {
    fn observe_timestamp(&self, start: DateTime<Utc>);
}

impl HistogramExt for Histogram {
    fn observe_timestamp(&self, start: DateTime<Utc>) {
        let elapsed = (Utc::now() - start).to_std();
        if let Ok(std_time) = elapsed {
            self.observe(duration_to_seconds(std_time))
        }
    }
}

#[inline]
fn duration_to_seconds(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos()) / 1e9;
    d.as_secs() as f64 + nanos
}

pub struct Metrics {
    online: IntGauge,
    total: IntGauge,
    connected_agents: IntGauge,
    load: IntGaugeVec,
}

impl Metrics {
    pub fn new(registry: &Registry) -> anyhow::Result<Self> {
        let janus_basic_metrics = IntGaugeVec::new(
            Opts::new("janus_basic", "Janus basic metrics"),
            &["capacity"],
        )?;
        let online = janus_basic_metrics.get_metric_with_label_values(&["online"])?;
        let total = janus_basic_metrics.get_metric_with_label_values(&["total"])?;
        let connected_agents =
            janus_basic_metrics.get_metric_with_label_values(&["connected_agents"])?;
        let load = IntGaugeVec::new(
            Opts::new("janus_load", "Janus load metrics"),
            &["kind", "agent"],
        )?;
        registry.register(Box::new(janus_basic_metrics))?;
        registry.register(Box::new(load.clone()))?;
        Ok(Self {
            online,
            total,
            connected_agents,
            load,
        })
    }

    pub fn start_collector(self, connection_pool: ConnectionPool, collect_interval: Duration) {
        loop {
            if let Err(err) = self.collect(&connection_pool) {
                error!(crate::LOG, "Janus' metrics collecting errored: {:#}", err);
            }
            std::thread::sleep(collect_interval);
        }
    }

    fn collect(&self, pool: &ConnectionPool) -> anyhow::Result<()> {
        let conn = pool.get()?;

        let online_backends_count =
            crate::db::janus_backend::count(&conn).context("Failed to get janus backends count")?;
        self.online.set(online_backends_count);

        let total_capacity = crate::db::janus_backend::total_capacity(&conn)
            .context("Failed to get janus backends total capacity")?;
        self.total.set(total_capacity);

        let connected_agents_count = agent_connection::CountQuery::new()
            .execute(&conn)
            .context("Failed to get connected agents count")?;
        self.connected_agents.set(connected_agents_count);

        let backend_load = crate::db::janus_backend::reserve_load_for_each_backend(&conn)
            .context("Failed to get janus backends reserve load")?;
        for backend_load in backend_load {
            let reserve = self
                .load
                .get_metric_with_label_values(&["reserve", backend_load.backend_id.label()])?;
            let agent_load = self
                .load
                .get_metric_with_label_values(&["taken", backend_load.backend_id.label()])?;
            reserve.set(backend_load.load);
            agent_load.set(backend_load.taken);
        }

        Ok(())
    }
}
