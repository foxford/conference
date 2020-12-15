use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use svc_agent::AgentId;

enum Message {
    Register {
        key: String,
        value: usize,
    },
    Flush {
        tx: crossbeam_channel::Sender<Vec<(String, usize)>>,
    },
    JanusTimeout(AgentId),
    Stop,
    GetJanusTimeouts {
        tx: crossbeam_channel::Sender<Vec<(String, u64)>>,
    },
    HandlerTiming {
        duration: Duration,
        method: String,
    },
    GetHandlerTimings {
        tx: crossbeam_channel::Sender<Vec<(String, PercentileReport)>>,
    },
}

#[derive(Clone)]
pub(crate) struct DynamicStatsCollector {
    tx: crossbeam_channel::Sender<Message>,
}

pub(crate) struct PercentileReport {
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

struct State {
    data: BTreeMap<String, usize>,
    janus_timeouts: BTreeMap<String, u64>,
    futures_timings: BTreeMap<String, Vec<u64>>,
}

impl DynamicStatsCollector {
    pub(crate) fn start() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();

        thread::spawn(move || {
            let mut state = State {
                data: BTreeMap::new(),
                janus_timeouts: BTreeMap::new(),
                futures_timings: BTreeMap::new(),
            };

            for message in rx {
                match message {
                    Message::Register { key, value } => {
                        let current_value = state.data.get_mut(&key).map(|v| *v);

                        match current_value {
                            Some(current_value) => state.data.insert(key, current_value + value),
                            None => state.data.insert(key, value),
                        };
                    }
                    Message::Flush { tx } => {
                        let report = state.data.into_iter().collect();

                        if let Err(err) = tx.send(report) {
                            warn!(
                                crate::LOG,
                                "Failed to send dynamic stats collector report: {}", err,
                            );
                        }

                        state.data = BTreeMap::new();
                    }
                    Message::Stop => break,
                    Message::JanusTimeout(agent_id) => {
                        let entry = state
                            .janus_timeouts
                            .entry(agent_id.to_string())
                            .or_insert(0);
                        *entry += 1;
                    }
                    Message::GetJanusTimeouts { tx } => {
                        let report = state
                            .janus_timeouts
                            .iter()
                            .map(|(aid, c)| (aid.clone(), *c))
                            .collect();

                        if let Err(err) = tx.send(report) {
                            warn!(
                                crate::LOG,
                                "Failed to send dynamic stats collector report: {}", err,
                            );
                        }
                    }
                    Message::HandlerTiming { duration, method } => {
                        let vec = state.futures_timings.entry(method).or_default();
                        let micros = match u64::try_from(duration.as_micros()) {
                            Ok(micros) => micros,
                            Err(_) => u64::MAX,
                        };

                        vec.push(micros);
                    }
                    Message::GetHandlerTimings { tx } => {
                        let vec = state
                            .futures_timings
                            .into_iter()
                            .map(|(method, mut values)| {
                                values.sort_unstable();

                                let count = values.len();
                                let p95_idx = (count as f32 * 0.95) as usize;
                                let p99_idx = (count as f32 * 0.99) as usize;
                                let max_idx = count - 1;
                                let max = values[max_idx];

                                let p95 = if p95_idx < max_idx {
                                    (values[p95_idx] + max) / 2
                                } else {
                                    max
                                };

                                let p99 = if p99_idx < max_idx {
                                    (values[p99_idx] + max) / 2
                                } else {
                                    max
                                };

                                (method, PercentileReport { p95, p99, max })
                            })
                            .collect::<Vec<_>>();

                        if let Err(err) = tx.send(vec) {
                            warn!(
                                crate::LOG,
                                "Failed to send dynamic stats collector report: {}", err,
                            );
                        }

                        state.futures_timings = BTreeMap::new();
                    }
                }
            }
        });

        Self { tx }
    }

    pub(crate) fn collect(&self, key: impl Into<String>, value: usize) {
        let message = Message::Register {
            key: key.into(),
            value,
        };

        if let Err(err) = self.tx.send(message) {
            warn!(
                crate::LOG,
                "Failed to register dynamic stats collector value: {}", err
            );
        }
    }

    pub(crate) fn flush(&self) -> Result<Vec<(String, usize)>> {
        let (tx, rx) = crossbeam_channel::bounded(1);

        self.tx
            .send(Message::Flush { tx })
            .context("Failed to send flush message to the dynamic stats collector")?;

        rx.recv()
            .context("Failed to receive dynamic stats collector report")
    }

    pub(crate) fn record_janus_timeout(&self, janus: AgentId) {
        if let Err(err) = self.tx.send(Message::JanusTimeout(janus)) {
            warn!(
                crate::LOG,
                "Failed to register dynamic stats collector value: {}", err
            );
        }
    }

    pub(crate) fn get_janus_timeouts(&self) -> Result<Vec<(String, u64)>> {
        let (tx, rx) = crossbeam_channel::bounded(1);

        self.tx
            .send(Message::GetJanusTimeouts { tx })
            .context("Failed to send GetJanusTimeouts message to the dynamic stats collector")?;

        rx.recv()
            .context("Failed to receive dynamic stats collector report")
    }

    pub(crate) fn record_future_time(&self, duration: Duration, method: String) {
        if let Err(err) = self.tx.send(Message::HandlerTiming { duration, method }) {
            warn!(
                crate::LOG,
                "Failed to register dynamic stats collector value: {}", err
            );
        }
    }

    pub(crate) fn get_handler_timings(&self) -> Result<Vec<(String, PercentileReport)>> {
        let (tx, rx) = crossbeam_channel::bounded(1);

        self.tx
            .send(Message::GetHandlerTimings { tx })
            .context("Failed to send GetHandlerTimings message to the dynamic stats collector")?;

        rx.recv()
            .context("Failed to receive dynamic stats collector report")
    }
}

impl Drop for DynamicStatsCollector {
    fn drop(&mut self) {
        if let Err(err) = self.tx.send(Message::Stop) {
            warn!(
                crate::LOG,
                "Failed to stop dynamic stats collector: {}", err
            );
        }
    }
}
