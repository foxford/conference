use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::sync::{
    mpsc::{self, UnboundedSender},
    oneshot,
};
use tracing::warn;

#[derive(Debug)]
pub enum WaitListError {
    OtherSideDropped,
    InternalTaskStopped,
}

impl std::fmt::Display for WaitListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for WaitListError {}

pub struct WaitList<T> {
    counter: Arc<AtomicUsize>,
    sender: UnboundedSender<WaitListCmd<T>>,
}

// Not deriving Clone because T may be not Cloneable but
// WaitList is always possible to clone.
impl<T> Clone for WaitList<T> {
    fn clone(&self) -> Self {
        Self {
            counter: self.counter.clone(),
            sender: self.sender.clone(),
        }
    }
}

enum WaitListCmd<T> {
    Register {
        id: usize,
        sender: oneshot::Sender<T>,
    },
    Fire {
        id: usize,
        evt: T,
    },
    Cleanup,
}

impl<T: Send + 'static> WaitList<T> {
    pub fn new(epoch_duration: std::time::Duration) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Asking main task to remove events that are waited for too much.
        tokio::task::spawn({
            let sender = sender.clone();
            async move {
                loop {
                    if let Err(_err) = sender.send(WaitListCmd::Cleanup) {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                }
            }
        });

        tokio::task::spawn(async move {
            let mut epochs = [HashMap::new(), HashMap::new()];
            let mut start = std::time::Instant::now();
            let mut current_epoch = 0;

            while let Some(cmd) = receiver.recv().await {
                // advance epochs
                if start.elapsed() > epoch_duration {
                    current_epoch = (current_epoch + 1) % epochs.len();
                    // dropping the old events
                    epochs[current_epoch] = HashMap::new();
                    // we don't care about precise epochs here
                    start = std::time::Instant::now();
                }

                match cmd {
                    WaitListCmd::Register { id, sender } => {
                        epochs[current_epoch].insert(id, sender);
                    }
                    WaitListCmd::Fire { id, evt } => {
                        let mut maybe_entry = None;

                        for epoch in epochs.iter_mut() {
                            if let Some(entry) = epoch.remove(&id) {
                                maybe_entry = Some(entry);
                                break;
                            }
                        }

                        match maybe_entry {
                            Some(entry) => {
                                if let Err(_err) = entry.send(evt) {
                                    warn!("no one is waiting for event anymore, id: {}", id);
                                };
                            }
                            None => {
                                warn!("unknown event id in waitlist: {}", id);
                            }
                        }
                    }
                    WaitListCmd::Cleanup => {
                        // all is done above, just wanted to advance
                    }
                }
            }
        });

        Self {
            counter: Arc::new(AtomicUsize::new(0)),
            sender,
        }
    }

    pub fn register(&self) -> Result<WaitEventHandle<T>, WaitListError> {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = oneshot::channel();

        self.sender
            .send(WaitListCmd::Register { id, sender })
            .map_err(|_| WaitListError::InternalTaskStopped)?;

        Ok(WaitEventHandle { id, receiver })
    }

    pub fn fire(&self, id: usize, evt: T) -> Result<(), WaitListError> {
        self.sender
            .send(WaitListCmd::Fire { id, evt })
            .map_err(|_| WaitListError::InternalTaskStopped)
    }
}

pub struct WaitEventHandle<T> {
    id: usize,
    receiver: oneshot::Receiver<T>,
}

impl<T> WaitEventHandle<T> {
    pub async fn wait(self) -> Result<T, WaitListError> {
        self.receiver
            .await
            .map_err(|_| WaitListError::OtherSideDropped)
    }

    pub fn id(&self) -> usize {
        self.id
    }
}
