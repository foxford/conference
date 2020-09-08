use std::collections::BTreeMap;
use std::thread;

use anyhow::{Context, Result};
use log::warn;

enum Message {
    Register {
        key: String,
        value: usize,
    },
    Flush {
        tx: crossbeam_channel::Sender<Vec<(String, usize)>>,
    },
    Stop,
}

pub(crate) struct DynamicStatsCollector {
    tx: crossbeam_channel::Sender<Message>,
}

impl DynamicStatsCollector {
    pub(crate) fn start() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();

        thread::spawn(move || {
            let mut data: BTreeMap<String, usize> = BTreeMap::new();

            for message in rx {
                match message {
                    Message::Register { key, value } => {
                        let current_value = data.get_mut(&key).map(|v| *v);

                        match current_value {
                            Some(current_value) => data.insert(key, current_value + value),
                            None => data.insert(key, value),
                        };
                    }
                    Message::Flush { tx } => {
                        let report = data.into_iter().collect::<Vec<(String, usize)>>();

                        if let Err(err) = tx.send(report) {
                            warn!("Failed to send dynamic stats collector report: {:?}", err);
                        }

                        data = BTreeMap::new();
                    }
                    Message::Stop => break,
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
                "Failed to register dynamic stats collector value: {:?}",
                err
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
}

impl Drop for DynamicStatsCollector {
    fn drop(&mut self) {
        if let Err(err) = self.tx.send(Message::Stop) {
            warn!("Failed to stop dynamic stats collector: {:?}", err);
        }
    }
}
