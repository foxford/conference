use std::{collections::HashMap, sync::atomic};

use futures::TryFutureExt;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    OtherSideDropped,
    InternalTaskStopped,
    Timeout,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
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
}

struct WaitListInner<T> {
    epochs: [HashMap<usize, oneshot::Sender<T>>; 2],
    current_epoch: usize,
}

impl<T> WaitListInner<T> {
    pub fn new() -> Self {
        Self {
            epochs: [HashMap::new(), HashMap::new()],
            current_epoch: 0,
        }
    }

    pub fn insert(&mut self, id: usize, sender: oneshot::Sender<T>) {
        self.epochs[self.current_epoch].insert(id, sender);
    }

    pub fn lookup(&mut self, id: usize) -> Option<oneshot::Sender<T>> {
        for epoch in self.epochs.iter_mut() {
            if let Some(entry) = epoch.remove(&id) {
                return Some(entry);
            }
        }

        None
    }

    pub fn advance(&mut self) {
        self.current_epoch = (self.current_epoch + 1) % self.epochs.len();
        // dropping the old events
        self.epochs[self.current_epoch] = HashMap::new();
    }
}

impl<T: Send + 'static> WaitList<T> {
    pub fn new(epoch_duration: std::time::Duration) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Asking to advance epochs.
        let mut tick_interval = tokio::time::interval(epoch_duration);
        // It's ok to miss some ticks.
        tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tokio::task::spawn(async move {
            let mut state = WaitListInner::new();

            loop {
                tokio::select! {
                    Some(cmd) = receiver.recv() => {
                        match cmd {
                            Cmd::Register { id, sender } => {
                                state.insert(id, sender);
                            }
                            Cmd::Fire { id, evt } => {
                                match state.lookup(id) {
                                    Some(entry) => {
                                        if let Err(_err) = entry.send(evt) {
                                            tracing::warn!(
                                                %id,
                                                "requester is not waiting for response from waitlist anymore",
                                            );
                                        };
                                    }
                                    None => {
                                        tracing::warn!(%id, "unknown response id in waitlist");
                                    }
                                }
                            }
                        }
                    }
                    _ = tick_interval.tick() => {
                        state.advance();
                    }
                    else => {
                        break;
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
    pub async fn wait(self, timeout: std::time::Duration) -> Result<T, Error> {
        let fut = self.receiver.map_err(|_| Error::OtherSideDropped);

        match tokio::time::timeout(timeout, fut).await {
            Ok(r) => r,
            Err(_) => Err(Error::Timeout),
        }
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
        assert_eq!(handle.wait(GOOD_ENOUGH * 10).await, Ok(()));
    }

    #[tokio::test]
    async fn wrong_id() {
        let waitlist: WaitList<()> = WaitList::new(GOOD_ENOUGH);
        let handle = waitlist.register().unwrap();
        waitlist.fire(handle.id() + 1, ()).unwrap();
        assert_eq!(handle.wait(GOOD_ENOUGH * 10).await, Err(Error::Timeout));
    }

    #[tokio::test]
    async fn event_expired() {
        let waitlist: WaitList<usize> = WaitList::new(GOOD_ENOUGH);

        let handle1 = waitlist.register().unwrap();
        let handle2 = waitlist.register().unwrap();

        waitlist.fire(handle2.id(), 1000).unwrap();

        assert_eq!(handle1.wait(GOOD_ENOUGH * 10).await, Err(Error::Timeout));
        assert_eq!(handle2.wait(GOOD_ENOUGH * 10).await, Ok(1000));
    }

    #[tokio::test]
    async fn drop_one_of_the_handles() {
        let waitlist: WaitList<()> = WaitList::new(GOOD_ENOUGH);

        let handle1 = waitlist.register().unwrap();
        let handle2 = waitlist.register().unwrap();

        drop(handle1);
        waitlist.fire(handle2.id(), ()).unwrap();

        assert_eq!(handle2.wait(GOOD_ENOUGH * 10).await, Ok(()));
    }

    #[tokio::test]
    async fn correct_data() {
        let waitlist: WaitList<usize> = WaitList::new(GOOD_ENOUGH);

        let handle1 = waitlist.register().unwrap();
        let handle2 = waitlist.register().unwrap();

        waitlist.fire(handle1.id(), 10).unwrap();
        waitlist.fire(handle2.id(), 1000).unwrap();

        assert_eq!(handle2.wait(GOOD_ENOUGH * 10).await, Ok(1000));
        assert_eq!(handle1.wait(GOOD_ENOUGH * 10).await, Ok(10));
    }
}
