use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use enum_iterator::IntoEnumIterator;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
};
use prometheus_static_metric::make_static_metric;

use super::{endpoint, error::ErrorKind};

pub trait HistogramExt {
    fn observe_timestamp(&self, start: DateTime<Utc>);
}

impl HistogramExt for Histogram {
    fn observe_timestamp(&self, start: DateTime<Utc>) {
        let elapsed = (Utc::now() - start).to_std();
        if let Ok(elapsed) = elapsed {
            self.observe(duration_to_seconds(elapsed))
        }
    }
}

#[inline]
fn duration_to_seconds(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos()) / 1e9;
    d.as_secs() as f64 + nanos
}

make_static_metric! {
    struct RequestDuration: Histogram {
        "method" => {
            agent_list,
            agent_reader_config_read,
            agent_reader_config_update,
            agent_writer_config_read,
            agent_writer_config_update,
            message_broadcast,
            message_callback,
            message_unicast_request,
            message_unicast_response,
            room_close,
            room_create,
            room_enter,
            room_leave,
            room_read,
            room_update,
            rtc_connect,
            rtc_create,
            rtc_list,
            rtc_read,
            rtc_signal_create,
            rtc_signal_trickle,
            rtc_signal_read,
            rtc_stream_list,
            upload_stream,
            subscription_create,
            subscription_delete_event,
            subscription_delete_response,
        },
    }
}

pub struct Metrics {
    pub request_duration: RequestDuration,
    pub app_result_ok: IntCounter,
    pub app_results_errors: HashMap<ErrorKind, IntCounter>,
    pub mqtt_reconnection: IntCounter,
    pub mqtt_disconnect: IntCounter,
    pub mqtt_connection_error: IntCounter,
    pub total_requests: IntCounter,
    pub authorization_time: Histogram,
    pub running_requests_total: IntGauge,
}

impl Metrics {
    pub fn new(registry: &Registry) -> anyhow::Result<Self> {
        let request_duration = HistogramVec::new(
            HistogramOpts::new("request_duration", "Request duration"),
            &["method"],
        )?;
        let request_stats =
            IntCounterVec::new(Opts::new("request_stats", "Request stats"), &["status"])?;
        let total_requests = IntCounter::new("incoming_requests_total", "Total requests")?;
        let authorization_time =
            Histogram::with_opts(HistogramOpts::new("auth_time", "Authorization time"))?;
        let running_requests_total =
            IntGauge::new("running_requests_total", "Total running requests")?;
        let mqtt_errors = IntCounterVec::new(
            Opts::new("mqtt_messages", "Mqtt message types"),
            &["status"],
        )?;
        registry.register(Box::new(mqtt_errors.clone()))?;
        registry.register(Box::new(request_duration.clone()))?;
        registry.register(Box::new(request_stats.clone()))?;
        registry.register(Box::new(total_requests.clone()))?;
        registry.register(Box::new(authorization_time.clone()))?;
        registry.register(Box::new(running_requests_total.clone()))?;
        Ok(Self {
            request_duration: RequestDuration::from(&request_duration),
            total_requests,
            app_result_ok: request_stats.get_metric_with_label_values(&["ok"])?,
            app_results_errors: ErrorKind::into_enum_iter()
                .map(|kind| {
                    Ok((
                        kind,
                        request_stats.get_metric_with_label_values(&[kind.kind()])?,
                    ))
                })
                .collect::<anyhow::Result<_>>()?,
            authorization_time,
            running_requests_total,
            mqtt_connection_error: mqtt_errors
                .get_metric_with_label_values(&["connection_error"])?,
            mqtt_disconnect: mqtt_errors.get_metric_with_label_values(&["disconnect"])?,
            mqtt_reconnection: mqtt_errors.get_metric_with_label_values(&["reconnect"])?,
        })
    }

    pub fn observe_auth(&self, elapsed: chrono::Duration) {
        if let Ok(elapsed) = elapsed.to_std() {
            self.authorization_time
                .observe(duration_to_seconds(elapsed))
        }
    }

    /// This is helpful with HTTP.
    pub fn observe_app_ok(&self) {
        self.app_result_ok.inc();
    }

    /// This is helpful with HTTP.
    pub fn observe_app_error(&self, err: &ErrorKind) {
        if let Some(m) = self.app_results_errors.get(err) {
            m.inc()
        }
    }

    /// This is helpful in MQTT handlers.
    pub fn observe_app_result(&self, result: &endpoint::RequestResult) {
        match result {
            Ok(_) => {
                self.observe_app_ok();
            }
            Err(err) => {
                self.observe_app_error(&err.error_kind());
            }
        }
    }

    pub fn request_started(self: Arc<Self>) -> StartedRequest {
        StartedRequest::new(self)
    }
}

pub struct StartedRequest {
    metric: Arc<Metrics>,
}

impl StartedRequest {
    fn new(metric: Arc<Metrics>) -> Self {
        metric.running_requests_total.inc();
        Self { metric }
    }
}

impl Drop for StartedRequest {
    fn drop(&mut self) {
        self.metric.running_requests_total.dec();
    }
}
