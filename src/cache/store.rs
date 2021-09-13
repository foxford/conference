use std::{borrow::Borrow, cmp::Ordering, fmt::Debug, hash::Hash, time::Duration, time::Instant};

use priority_queue::PriorityQueue;

pub trait GetTime {
    fn get_time(&self) -> Instant;
}

#[derive(Debug)]
pub struct CurrentTime;

impl GetTime for CurrentTime {
    fn get_time(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Debug)]
pub struct TtlCache<K, V, GT = CurrentTime>
where
    K: Hash + Eq,
{
    cache: PriorityQueue<K, ExpiresAt<V>>,
    time: GT,
}

impl<K: Hash + Eq, V> TtlCache<K, V, CurrentTime> {
    pub fn new() -> Self {
        Self::with_get_time(CurrentTime)
    }
}

impl<K, V, GT> TtlCache<K, V, GT>
where
    K: Hash + Eq,
    GT: GetTime,
{
    pub fn with_get_time(time: GT) -> Self {
        Self {
            cache: PriorityQueue::new(),
            time,
        }
    }

    pub fn insert(&mut self, k: K, v: V, ttl: Duration) -> Option<V> {
        self.cache
            .push(
                k,
                ExpiresAt {
                    expires_at: self.time.get_time() + ttl,
                    value: v,
                },
            )
            .map(|x| x.value)
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn remove_oldest(&mut self) {
        self.cache.pop();
    }

    pub fn remove<Q>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.cache.remove(k).map(|(_, val)| val.value)
    }

    pub fn remove_expired(&mut self, remove_count: usize) -> usize {
        let current_time = self.time.get_time();
        let mut removed = 0;
        while self
            .cache
            .peek()
            .filter(|(_, v)| v.is_expired(current_time) && remove_count != removed)
            .is_some()
        {
            removed += 1;
            self.cache.pop();
        }
        removed
    }

    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.cache
            .get_priority(k)
            .filter(|v| !v.is_expired(self.time.get_time()))
            .map(|v| &v.value)
    }
}

#[derive(Debug)]
struct ExpiresAt<T> {
    expires_at: Instant,
    value: T,
}

impl<T> ExpiresAt<T> {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at < now
    }
}

impl<T> Ord for ExpiresAt<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.expires_at.cmp(&self.expires_at)
    }
}

impl<T> PartialOrd for ExpiresAt<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.expires_at.partial_cmp(&self.expires_at)
    }
}

impl<T> Eq for ExpiresAt<T> {}

impl<T> PartialEq for ExpiresAt<T> {
    fn eq(&self, other: &Self) -> bool {
        self.expires_at.eq(&other.expires_at)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        rc::Rc,
        time::{Duration, Instant},
    };

    use super::{GetTime, TtlCache};

    #[derive(Clone)]
    struct TimeMock {
        time: Rc<Cell<Instant>>,
    }

    impl TimeMock {
        fn now() -> Self {
            Self {
                time: Rc::new(Cell::new(Instant::now())),
            }
        }

        fn add(&self, d: Duration) {
            self.replace(self.time.get() + d);
        }

        fn replace(&self, new_time: Instant) {
            self.time.replace(new_time);
        }
    }

    impl GetTime for TimeMock {
        fn get_time(&self) -> Instant {
            self.time.get()
        }
    }

    #[test]
    fn should_remove_expired() {
        let time = TimeMock::now();
        let mut cache = TtlCache::with_get_time(time.clone());
        cache.insert(1, 2, Duration::from_secs(1));
        time.add(Duration::from_secs(2));

        let removed = cache.remove_expired(5);

        assert_eq!(removed, 1);
        assert!(cache.get(&1).is_none())
    }

    #[test]
    fn should_not_return_expired() {
        let time = TimeMock::now();
        let mut cache = TtlCache::with_get_time(time.clone());
        cache.insert(1, 2, Duration::from_secs(1));
        time.add(Duration::from_secs(2));

        let expired = cache.get(&1);

        assert!(expired.is_none())
    }

    #[test]
    fn should_return_actual() {
        let time = TimeMock::now();
        let mut cache = TtlCache::with_get_time(time.clone());
        cache.insert(1, 2, Duration::from_secs(1));

        let actual = cache.get(&1);

        assert_eq!(actual, Some(&2))
    }

    #[test]
    fn should_not_remove_actual() {
        let time = TimeMock::now();
        let mut cache = TtlCache::with_get_time(time.clone());
        cache.insert(1, 2, Duration::from_secs(1));

        let removed = cache.remove_expired(5);

        assert_eq!(removed, 0);
        assert_eq!(cache.get(&1), Some(&2))
    }

    #[test]
    fn should_remove_only_specified_count() {
        let time = TimeMock::now();
        let now = time.get_time();
        let mut cache = TtlCache::with_get_time(time.clone());
        cache.insert(1, 2, Duration::from_secs(0));
        cache.insert(2, 2, Duration::from_secs(1));
        cache.insert(3, 2, Duration::from_secs(2));
        cache.insert(4, 4, Duration::from_secs(3));
        time.add(Duration::from_secs(4));

        let removed = cache.remove_expired(2);

        assert_eq!(removed, 2);
        assert!(cache.get(&4).is_none());
        time.replace(now);
        assert_eq!(cache.get(&4), Some(&4));
        assert_eq!(cache.get(&3), Some(&2));
        assert!(cache.get(&1).is_none());
        assert!(cache.get(&2).is_none());
    }
}
