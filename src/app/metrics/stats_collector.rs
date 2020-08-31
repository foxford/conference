use std::sync::Arc;
use std::sync::Mutex;

use diesel::r2d2::event::*;
use log::error;

#[derive(Debug, Default)]
struct Stat {
    total: u128,
    micros_sum: u128,
    max_micros: u128,
}

impl Stat {
    fn reset(&mut self) {
        self.total = 0;
        self.micros_sum = 0;
        self.max_micros = 0;
    }

    fn update(&mut self, micros: u128) {
        self.total += 1;
        self.micros_sum += micros;
        self.max_micros = std::cmp::max(self.max_micros, micros)
    }
}

#[derive(Debug, Default)]
struct Inner {
    checkin: Stat,
    checkout: Stat,
    release: Stat,
    timeout: Stat,
}

enum Update {
    Checkin(u128),
    Checkout(u128),
    Release(u128),
    Timeout(u128),
}

impl Inner {
    fn get_stats(&mut self) -> Stats {
        let stats = Stats {
            avg_checkin: (self.checkin.micros_sum as f64) / (self.checkin.total as f64),
            avg_checkout: (self.checkout.micros_sum as f64) / (self.checkout.total as f64),
            avg_release: (self.release.micros_sum as f64) / (self.release.total as f64),
            avg_timeout: (self.timeout.micros_sum as f64) / (self.timeout.total as f64),
            max_checkin: self.checkin.max_micros,
            max_checkout: self.checkout.max_micros,
            max_release: self.release.max_micros,
            max_timeout: self.timeout.max_micros,
        };

        self.reset();

        stats
    }

    fn update(&mut self, update: Update) {
        match update {
            Update::Checkin(micros) => {
                self.checkin.update(micros);
            }
            Update::Checkout(micros) => {
                self.checkout.update(micros);
            }
            Update::Timeout(micros) => {
                self.timeout.update(micros);
            }
            Update::Release(micros) => {
                self.release.update(micros);
            }
        }
    }

    fn reset(&mut self) {
        self.checkin.reset();
        self.checkout.reset();
        self.release.reset();
        self.timeout.reset();
    }
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub avg_checkin: f64,
    pub avg_checkout: f64,
    pub avg_release: f64,
    pub avg_timeout: f64,
    pub max_checkin: u128,
    pub max_checkout: u128,
    pub max_release: u128,
    pub max_timeout: u128,
}

#[derive(Debug, Clone)]
pub struct StatsCollector {
    inner: Arc<Mutex<Inner>>,
}

impl StatsCollector {
    pub fn new() -> (Self, StatsTransmitter) {
        let inner: Arc<Mutex<Inner>> = Default::default();
        let collector = Self {
            inner: inner.clone(),
        };

        let (tx, rx) = crossbeam_channel::bounded(100);
        let transmitter = StatsTransmitter {
            inner: Arc::new(Mutex::new(StatsTransmitterInner(tx))),
        };

        std::thread::Builder::new()
            .name("dbpool-stats-collector".to_string())
            .spawn(move || {
                for update in rx {
                    inner
                        .lock()
                        .expect("Lock failed, poisoned mutex?")
                        .update(update);
                }
            })
            .expect("Failed to start dbpool-stats-collector thread");

        (collector, transmitter)
    }

    pub fn get_stats(&self) -> Stats {
        let mut inner = self.inner.lock().expect("Lock failed, poisoned mutex?");
        inner.get_stats()
    }
}

#[derive(Debug)]
struct StatsTransmitterInner(crossbeam_channel::Sender<Update>);

#[derive(Debug)]
pub struct StatsTransmitter {
    inner: Arc<Mutex<StatsTransmitterInner>>,
}

impl diesel::r2d2::HandleEvent for StatsTransmitter {
    fn handle_checkin(&self, event: CheckinEvent) {
        let micros = event.duration().as_micros();

        let inner = self.inner.lock().expect("Lock failed, poisoned mutex?");
        if let Err(e) = inner.0.send(Update::Checkin(micros)) {
            error!(
                "Failed to send checkin micros in StatsTransmiiter, reason = {}",
                e
            );
        }
    }

    fn handle_checkout(&self, event: CheckoutEvent) {
        let micros = event.duration().as_micros();

        let inner = self.inner.lock().expect("Lock failed, poisoned mutex?");
        if let Err(e) = inner.0.send(Update::Checkout(micros)) {
            error!(
                "Failed to send checkout micros in StatsTransmiiter, reason = {}",
                e
            );
        }
    }

    fn handle_release(&self, event: ReleaseEvent) {
        let micros = event.age().as_micros();

        let inner = self.inner.lock().expect("Lock failed, poisoned mutex?");
        if let Err(e) = inner.0.send(Update::Release(micros)) {
            error!(
                "Failed to send checkout micros in StatsTransmiiter, reason = {}",
                e
            );
        }
    }

    fn handle_timeout(&self, event: TimeoutEvent) {
        let micros = event.timeout().as_micros();

        let inner = self.inner.lock().expect("Lock failed, poisoned mutex?");
        if let Err(e) = inner.0.send(Update::Timeout(micros)) {
            error!(
                "Failed to send checkout micros in StatsTransmiiter, reason = {}",
                e
            );
        }
    }

    fn handle_acquire(&self, _event: AcquireEvent) {}
}
