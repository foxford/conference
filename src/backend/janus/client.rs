use std::collections::HashMap;
use std::thread;
use std::time::Duration as StdDuration;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value as JsonValue};
use svc_agent::{
    mqtt::{
        IncomingResponseProperties, OutgoingRequestProperties, ResponseStatus, SubscriptionTopic,
    },
    AgentId, Subscription,
};
use svc_error::{extension::sentry, Error as SvcError};

use crate::config::BackendConfig;

use super::{JANUS_API_VERSION, STREAM_UPLOAD_METHOD};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct RequestInfo {
    timeout: Duration,
    to: AgentId,
    start_timestamp: DateTime<Utc>,
    payload: JsonValue,
}

enum TransactionWatchdogMessage {
    Halt,
    Insert(String, RequestInfo),
    Remove(String),
}

pub(crate) struct Client {
    me: AgentId,
    transaction_watchdog_tx: crossbeam_channel::Sender<TransactionWatchdogMessage>,
    default_timeout: Duration,
    stream_upload_timeout: Duration,
}

impl Client {
    pub(crate) fn start(config: &BackendConfig, me: AgentId) -> Result<Self> {
        let period = StdDuration::from_secs(config.transaction_watchdog_check_period);
        let (tx, rx) = crossbeam_channel::unbounded();

        thread::spawn(move || {
            let mut state: HashMap<String, RequestInfo> = HashMap::new();

            loop {
                if let Ok(message) = rx.recv_timeout(period) {
                    match message {
                        TransactionWatchdogMessage::Halt => break,
                        TransactionWatchdogMessage::Insert(corr_data, info) => {
                            state.insert(corr_data, info);
                        }
                        TransactionWatchdogMessage::Remove(ref corr_data) => {
                            state.remove(corr_data);
                        }
                    }
                }

                state = state
                    .into_iter()
                    .filter(|(corr_data, info)| {
                        if info.start_timestamp + info.timeout < Utc::now() {
                            let msg =
                                format!("Janus request timed out ({}): {:?}", corr_data, info);

                            error!(crate::LOG, "{}", msg);

                            let svc_error = SvcError::builder()
                                .status(ResponseStatus::GATEWAY_TIMEOUT)
                                .kind("janus_request_timed_out", "Janus request timed out")
                                .detail(&msg)
                                .build();

                            sentry::send(svc_error).unwrap_or_else(|err| {
                                warn!(crate::LOG, "Error sending error to Sentry: {}", err);
                            });

                            false
                        } else {
                            true
                        }
                    })
                    .collect();
            }
        });

        Ok(Self {
            me,
            transaction_watchdog_tx: tx,
            default_timeout: Duration::seconds(config.default_timeout as i64),
            stream_upload_timeout: Duration::seconds(config.stream_upload_timeout as i64),
        })
    }

    pub(super) fn register_transaction<P: serde::Serialize>(
        &self,
        to: &AgentId,
        start_timestamp: DateTime<Utc>,
        reqp: &OutgoingRequestProperties,
        payload: &P,
        timeout: Duration,
    ) {
        let request_info = RequestInfo {
            timeout,
            to: to.to_owned(),
            start_timestamp,
            payload: json!(payload),
        };

        self.transaction_watchdog_tx
            .send(TransactionWatchdogMessage::Insert(
                reqp.correlation_data().to_owned(),
                request_info,
            ))
            .unwrap_or_else(|err| {
                error!(
                    crate::LOG,
                    "Failed to register janus client transaction: {}", err
                );
            });
    }

    pub(super) fn finish_transaction(&self, respp: &IncomingResponseProperties) {
        let corr_data = respp.correlation_data().to_owned();

        self.transaction_watchdog_tx
            .send(TransactionWatchdogMessage::Remove(corr_data))
            .unwrap_or_else(|err| {
                error!(
                    crate::LOG,
                    "Failed to remove janus client transaction: {}", err
                );
            });
    }

    pub(super) fn timeout(&self, method: &str) -> Duration {
        match method {
            STREAM_UPLOAD_METHOD => self.stream_upload_timeout,
            _ => self.default_timeout,
        }
    }

    pub(super) fn response_topic(&self, to: &AgentId) -> Result<String> {
        Subscription::unicast_responses_from(to)
            .subscription_topic(&self.me, JANUS_API_VERSION)
            .context("Failed to build subscription topic")
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.transaction_watchdog_tx
            .send(TransactionWatchdogMessage::Halt)
            .unwrap_or_else(|err| {
                error!(
                    crate::LOG,
                    "Failed to stop janus client transaction watchdog: {}", err
                );
            });
    }
}
