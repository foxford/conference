use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};
use async_std::channel::Sender;
use async_std::stream::StreamExt;
use chrono::{serde::ts_seconds, DateTime, Utc};
use serde_derive::Deserialize;

use crate::app::metrics::Metric2;
use crate::app::{context::GlobalContext, MessageHandler};

#[derive(Clone)]
pub(crate) struct StatsRoute<C: GlobalContext> {
    message_handler: Arc<MessageHandler<C>>,
}

#[derive(Debug, Clone)]
struct StatsHandle {
    tx: async_std::channel::Sender<StatsRouteCommand>,
}

enum StatsRouteCommand {
    GetStats(Sender<Result<String>>),
}

impl<C: GlobalContext + Send + 'static> StatsRoute<C> {
    pub fn start(config: crate::app::config::Config, message_handler: Arc<MessageHandler<C>>) {
        if let Some(metrics_conf) = config.metrics {
            let (tx, mut rx) = async_std::channel::bounded(1000);
            let handle = StatsHandle { tx };

            let route = Self { message_handler };

            async_std::task::spawn(async move {
                loop {
                    if let Some(x) = rx.next().await {
                        match x {
                            StatsRouteCommand::GetStats(chan) => {
                                chan.send(route.get_stats()).await;
                            }
                        }
                    }
                }
            });

            std::thread::Builder::new()
                .name(String::from("tide-metrics-server"))
                .spawn(move || {
                    warn!(
                        crate::LOG,
                        "StatsRoute listening on http://{}", metrics_conf.http.bind_address
                    );

                    let mut app = tide::with_state(handle);
                    app.at("/metrics")
                        .get(|req: tide::Request<StatsHandle>| async move {
                            match req.state().get_stats().await {
                                Ok(Ok(text)) => {
                                    let mut res = tide::Response::new(200);
                                    res.set_body(tide::Body::from_string(text));
                                    Ok(res)
                                }
                                Ok(Err(e)) => {
                                    error!(crate::LOG, "Something went wrong: {:?}", e);
                                    let mut res = tide::Response::new(500);
                                    res.set_body(tide::Body::from_string(
                                        "Something went wrong".into(),
                                    ));
                                    Ok(res)
                                }
                                Err(e) => {
                                    error!(crate::LOG, "Something went wrong: {:?}", e);
                                    let mut res = tide::Response::new(500);
                                    res.set_body(tide::Body::from_string(
                                        "Something went wrong".into(),
                                    ));
                                    Ok(res)
                                }
                            }
                        });

                    if let Err(e) =
                        async_std::task::block_on(app.listen(metrics_conf.http.bind_address))
                    {
                        error!(crate::LOG, "Tide future completed with error: {:?}", e);
                    }
                })
                .expect("Failed to spawn tide-metrics-server thread");
        }
    }

    fn get_stats(&self) -> Result<String> {
        let mut acc = String::new();

        let metrics = self
            .message_handler
            .global_context()
            .get_metrics()
            .context("Failed to get metrics")?;

        for metric in metrics {
            let metric: Metric2 = metric.into();

            // metrics with Dynamic key have Empty tag so we skip them
            if *metric.tags() == super::metric::Tags::Empty {
                continue;
            }

            match serde_json::to_value(metric.tags()) {
                Ok(tags) => {
                    if let Some(tags) = tags.as_object() {
                        let tags_acc = tags.iter().filter_map(|(key, val)| {
                            let val = if val.is_null() {
                                Some("")
                            } else if val.is_string() {
                                val.as_str()
                            } else {
                                warn!(
                                    crate::LOG,
                                    "StatsRoute: improper tag value, expected string or null, metric: {:?}",
                                    metric
                                );

                                None
                            };
                            val.map(|v| format!("{}=\"{}\"", key, v))
                        }).collect::<Vec<String>>().join(", ");

                        acc.push_str(&format!(
                            "{}{{{}}} {}\n",
                            metric.key(),
                            tags_acc,
                            metric.value()
                        ));
                    } else {
                        warn!(
                            crate::LOG,
                            "StatsRoute: failed to parse metric tags, metric: {:?}", metric
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        crate::LOG,
                        "Conversion from Metric to MetricHelper failed, reason = {:?}", e
                    );
                }
            }
        }

        Ok(acc)
    }
}

#[derive(Deserialize, Debug)]
struct MetricHelper {
    pub value: serde_json::Value,
    #[serde(rename = "metric")]
    pub key: String,
    #[serde(with = "ts_seconds")]
    pub timestamp: DateTime<Utc>,
    pub tags: serde_json::Value,
}

impl StatsHandle {
    pub async fn get_stats(&self) -> Result<Result<String>> {
        let (tx, rx) = async_std::channel::bounded(1);
        self.tx.send(StatsRouteCommand::GetStats(tx)).await;
        rx.recv().await.context("Stats handle recv error")
    }
}
