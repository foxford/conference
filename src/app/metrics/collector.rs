use chrono::{DateTime, Utc};

use crate::app::context::GlobalContext;
use crate::app::metrics::{Metric, MetricKey, Tags};

pub(crate) struct Collector<'a, C: GlobalContext> {
    context: &'a C,
}

impl<'a, C: GlobalContext> Collector<'a, C> {
    pub(crate) fn new(context: &'a C) -> Self {
        Self { context }
    }

    pub(crate) fn get(&self) -> anyhow::Result<Vec<crate::app::metrics::Metric>> {
        let now = Utc::now();
        let mut metrics = vec![];

        append_mqtt_stats(&mut metrics, self.context, now)?;
        append_internal_stats(&mut metrics, self.context, now);
        append_redis_pool_metrics(&mut metrics, self.context, now);
        append_dynamic_stats(&mut metrics, self.context, now)?;

        append_janus_stats(&mut metrics, self.context, now)?;

        Ok(metrics)
    }
}

fn append_mqtt_stats(
    metrics: &mut Vec<Metric>,
    context: &impl GlobalContext,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    if let Some(qc) = context.queue_counter() {
        let stats = qc
            .get_stats()
            .map_err(|err| anyhow!(err).context("Failed to get stats"))?;

        stats.into_iter().for_each(|(tags, value)| {
            let tags = Tags::build_queues_tags(crate::APP_VERSION, context.agent_id(), tags);

            if value.incoming_requests > 0 {
                metrics.push(Metric::new(
                    MetricKey::IncomingQueueRequests,
                    value.incoming_requests,
                    now,
                    tags.clone(),
                ));
            }
            if value.incoming_responses > 0 {
                metrics.push(Metric::new(
                    MetricKey::IncomingQueueResponses,
                    value.incoming_responses,
                    now,
                    tags.clone(),
                ));
            }
            if value.incoming_events > 0 {
                metrics.push(Metric::new(
                    MetricKey::IncomingQueueEvents,
                    value.incoming_events,
                    now,
                    tags.clone(),
                ));
            }
            if value.outgoing_requests > 0 {
                metrics.push(Metric::new(
                    MetricKey::OutgoingQueueRequests,
                    value.outgoing_requests,
                    now,
                    tags.clone(),
                ));
            }
            if value.outgoing_responses > 0 {
                metrics.push(Metric::new(
                    MetricKey::OutgoingQueueResponses,
                    value.outgoing_responses,
                    now,
                    tags.clone(),
                ));
            }
            if value.outgoing_events > 0 {
                metrics.push(Metric::new(
                    MetricKey::OutgoingQueueEvents,
                    value.outgoing_events,
                    now,
                    tags,
                ));
            }
        });
    }

    Ok(())
}

fn append_internal_stats(
    metrics: &mut Vec<Metric>,
    context: &impl GlobalContext,
    now: DateTime<Utc>,
) {
    let tags = Tags::build_internal_tags(crate::APP_VERSION, context.agent_id());
    let state = context.db().state();

    metrics.extend_from_slice(&[
        Metric::new(
            MetricKey::DbConnections,
            state.connections,
            now,
            tags.clone(),
        ),
        Metric::new(
            MetricKey::IdleDbConnections,
            state.idle_connections,
            now,
            tags,
        ),
    ])
}

fn append_redis_pool_metrics(
    metrics: &mut Vec<Metric>,
    context: &impl GlobalContext,
    now: DateTime<Utc>,
) {
    if let Some(pool) = context.redis_pool() {
        let state = pool.state();
        let tags = Tags::build_internal_tags(crate::APP_VERSION, context.agent_id());

        metrics.extend_from_slice(&[
            Metric::new(
                MetricKey::RedisConnections,
                state.connections,
                now,
                tags.clone(),
            ),
            Metric::new(
                MetricKey::IdleRedisConnections,
                state.idle_connections,
                now,
                tags,
            ),
        ]);
    }
}

fn append_dynamic_stats(
    metrics: &mut Vec<Metric>,
    context: &impl GlobalContext,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    if let Some(dynamic_stats) = context.dynamic_stats() {
        for (key, value) in dynamic_stats.flush()? {
            metrics.push(Metric::new(
                MetricKey::Dynamic(key),
                value,
                now,
                Tags::Empty,
            ));
        }
    }

    Ok(())
}

fn append_janus_stats(
    metrics: &mut Vec<Metric>,
    context: &impl GlobalContext,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    use crate::db::agent;
    use anyhow::Context;
    let conn = context.get_conn().unwrap();

    let tags = Tags::build_internal_tags(crate::APP_VERSION, context.agent_id());

    // The number of online janus backends.
    let online_backends_count =
        crate::db::janus_backend::count(&conn).context("Failed to get janus backends count")?;

    metrics.push(Metric::new(
        MetricKey::OnlineJanusBackendsCount,
        online_backends_count,
        now,
        tags.clone(),
    ));

    // Total capacity of online janus backends.
    let total_capacity = crate::db::janus_backend::total_capacity(&conn)
        .context("Failed to get janus backends total capacity")?;

    metrics.push(Metric::new(
        MetricKey::JanusBackendTotalCapacity,
        total_capacity,
        now,
        tags.clone(),
    ));

    // The number of agents connect to an RTC.
    let connected_agents_count = agent::CountQuery::new()
        .status(agent::Status::Connected)
        .execute(&conn)
        .context("Failed to get connected agents count")?;

    metrics.push(Metric::new(
        MetricKey::ConnectedAgentsCount,
        connected_agents_count,
        now,
        tags,
    ));

    let backend_load = crate::db::janus_backend::reserve_load_for_each_backend(&conn)
        .context("Failed to get janus backends reserve load")?
        .into_iter()
        .fold(vec![], |mut v, load_row| {
            let tags = Tags::build_janus_tags(
                crate::APP_VERSION,
                context.agent_id(),
                &load_row.backend_id,
            );

            v.push(Metric::new(
                MetricKey::JanusBackendReserveLoad,
                load_row.load,
                now,
                tags.clone(),
            ));
            v.push(Metric::new(
                MetricKey::JanusBackendAgentLoad,
                load_row.taken,
                now,
                tags,
            ));
            v
        });

    metrics.extend(backend_load);

    Ok(())
}
