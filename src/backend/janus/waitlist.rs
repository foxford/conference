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
}

struct WaitListInner<T> {
    epochs: [HashMap<usize, oneshot::Sender<T>>; 2],
    epoch_start: std::time::Instant,
    current_epoch: usize,
    epoch_duration: std::time::Duration,
}

impl<T> WaitListInner<T> {
    pub fn new(epoch_duration: std::time::Duration) -> Self {
        Self {
            epochs: [HashMap::new(), HashMap::new()],
            epoch_start: std::time::Instant::now(),
            current_epoch: 0,
            epoch_duration,
        }
    }

    pub fn insert(&mut self, id: usize, sender: oneshot::Sender<T>) {
        self.advance();
        self.epochs[self.current_epoch].insert(id, sender);
    }

    pub fn lookup(&mut self, id: usize) -> Option<oneshot::Sender<T>> {
        self.advance();

        for epoch in self.epochs.iter_mut() {
            if let Some(entry) = epoch.remove(&id) {
                return Some(entry);
            }
        }

        None
    }

    pub fn advance(&mut self) {
        // advance epochs
        if self.epoch_start.elapsed() > self.epoch_duration {
            self.current_epoch = (self.current_epoch + 1) % self.epochs.len();
            // dropping the old events
            self.epochs[self.current_epoch] = HashMap::new();
            // we don't care about precise epochs here
            self.epoch_start = std::time::Instant::now();
        }
    }
}

impl<T: Send + 'static> WaitList<T> {
    pub fn new(epoch_duration: std::time::Duration) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Asking to advance epochs if there were no other requests.
        let mut tick_interval = tokio::time::interval(epoch_duration);
        tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tokio::task::spawn(async move {
            let mut state = WaitListInner::new(epoch_duration);

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
