use std::{collections::HashMap, sync::atomic};

use tokio::sync::{mpsc, oneshot};

#[derive(Debug, PartialEq)]
pub enum Error {
    OtherSideDropped,
    InternalTaskStopped,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

/// Usable if you want to wait for some external events.
pub struct WaitList<T> {
    counter: std::sync::Arc<atomic::AtomicUsize>,
    sender: mpsc::UnboundedSender<Cmd<T>>,
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

enum Cmd<T> {
    Register {
        id: usize,
        sender: oneshot::Sender<T>,
    },
    Fire {
        id: usize,
        evt: T,
    },
    Tick,
}

impl<T: Send + 'static> WaitList<T> {
    pub fn new(epoch_duration: std::time::Duration) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Asking main task to advance epochs if there were no other requests.
        tokio::task::spawn({
            let sender = sender.clone();
            async move {
                loop {
                    if let Err(_err) = sender.send(Cmd::Tick) {
                        break;
                    }
                    tokio::time::sleep(epoch_duration).await;
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
                    Cmd::Register { id, sender } => {
                        epochs[current_epoch].insert(id, sender);
                    }
                    Cmd::Fire { id, evt } => {
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
                                    tracing::warn!(
                                        "no one is waiting for event anymore, id: {}",
                                        id
                                    );
                                };
                            }
                            None => {
                                tracing::warn!("unknown event id in waitlist: {}", id);
                            }
                        }
                    }
                    Cmd::Tick => {
                        // all is done above, just wanted to advance the epoch and drop
                        // all expired event senders
                    }
                }
            }
        });

        Self {
            counter: std::sync::Arc::new(atomic::AtomicUsize::new(0)),
            sender,
        }
    }

    /// Register an upcoming event. Make sure handle id will be known to
    /// some external event stream so you can look for it.
    pub fn register(&self) -> Result<Handle<T>, Error> {
        let id = self.counter.fetch_add(1, atomic::Ordering::SeqCst);
        let (sender, receiver) = oneshot::channel();

        self.sender
            .send(Cmd::Register { id, sender })
            .map_err(|_| Error::InternalTaskStopped)?;

        Ok(Handle { id, receiver })
    }

    /// The event is here, let's proceed.
    pub fn fire(&self, id: usize, evt: T) -> Result<(), Error> {
        self.sender
            .send(Cmd::Fire { id, evt })
            .map_err(|_| Error::InternalTaskStopped)
    }
}

pub struct Handle<T> {
    id: usize,
    receiver: oneshot::Receiver<T>,
}

impl<T> Handle<T> {
    pub async fn wait(self) -> Result<T, Error> {
        self.receiver.await.map_err(|_| Error::OtherSideDropped)
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD_ENOUGH: std::time::Duration = std::time::Duration::from_micros(10);

    #[tokio::test]
    async fn simple() {
        let waitlist = WaitList::new(GOOD_ENOUGH);
        let handle = waitlist.register().unwrap();
        waitlist.fire(handle.id(), ()).unwrap();
        assert_eq!(handle.wait().await, Ok(()));
    }

    #[tokio::test]
    async fn wrong_id() {
        let waitlist: WaitList<()> = WaitList::new(GOOD_ENOUGH);
        let handle = waitlist.register().unwrap();
        waitlist.fire(handle.id() + 1, ()).unwrap();
        assert_eq!(handle.wait().await, Err(Error::OtherSideDropped));
    }

    #[tokio::test]
    async fn event_expired() {
        let waitlist: WaitList<usize> = WaitList::new(GOOD_ENOUGH);

        let handle1 = waitlist.register().unwrap();
        let handle2 = waitlist.register().unwrap();

        waitlist.fire(handle2.id(), 1000).unwrap();

        assert_eq!(handle1.wait().await, Err(Error::OtherSideDropped));
        assert_eq!(handle2.wait().await, Ok(1000));
    }

    #[tokio::test]
    async fn drop_one_of_the_handles() {
        let waitlist: WaitList<()> = WaitList::new(GOOD_ENOUGH);

        let handle1 = waitlist.register().unwrap();
        let handle2 = waitlist.register().unwrap();

        drop(handle1);
        waitlist.fire(handle2.id(), ()).unwrap();

        assert_eq!(handle2.wait().await, Ok(()));
    }

    #[tokio::test]
    async fn correct_data() {
        let waitlist: WaitList<usize> = WaitList::new(GOOD_ENOUGH);

        let handle1 = waitlist.register().unwrap();
        let handle2 = waitlist.register().unwrap();

        waitlist.fire(handle1.id(), 10).unwrap();
        waitlist.fire(handle2.id(), 1000).unwrap();

        assert_eq!(handle2.wait().await, Ok(1000));
        assert_eq!(handle1.wait().await, Ok(10));
    }
}
