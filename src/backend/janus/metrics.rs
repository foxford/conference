use std::time::Duration;

use anyhow::Context;
use prometheus::{IntGauge, IntGaugeVec, Opts, Registry};
use tracing::error;

use crate::db::agent_connection;

use super::client_pool::Clients;

pub struct Metrics {
    online: IntGauge,
    total: IntGauge,
    connected_agents: IntGauge,
    load: IntGaugeVec,
    polling_janusses: IntGauge,
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
        let polling_janusses =
            janus_basic_metrics.get_metric_with_label_values(&["polling_janusses_count"])?;
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
            polling_janusses,
        })
    }

    pub async fn start_collector(
        self,
        connection_pool: sqlx::PgPool,
        clients: Clients,
        collect_interval: Duration,
    ) {
        loop {
            if let Err(err) = self.collect(&connection_pool, &clients).await {
                error!(?err, "Janus' metrics collecting errored");
            }
            tokio::time::sleep(collect_interval).await;
        }
    }

    async fn collect(&self, pool: &sqlx::PgPool, clients: &Clients) -> anyhow::Result<()> {
        let mut conn = pool.acquire().await?;

        let online_backends = crate::db::janus_backend::count(&mut conn)
            .await
            .context("Failed to get janus backends count")?;
        self.online.set(online_backends.count);

        let r = crate::db::janus_backend::total_capacity(&mut conn)
            .await
            .context("Failed to get janus backends total capacity")?;
        self.total.set(r.total_capacity());

        let connected_agents = agent_connection::CountQuery::new()
            .execute(&mut conn)
            .await
            .context("Failed to get connected agents count")?;
        self.connected_agents.set(connected_agents.count);

        let backend_load = crate::db::janus_backend::reserve_load_for_each_backend(&mut conn)
            .await
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

        self.polling_janusses.set(clients.clients_count() as i64);

        Ok(())
    }
}
