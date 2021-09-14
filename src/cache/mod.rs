use derive_more::Display;
use futures::{
    future::{BoxFuture, Shared},
    Future, FutureExt, TryFutureExt,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::{hash::Hash, time::Duration};

use self::store::TtlCache;
use parking_lot::RwLock;

mod store;

pub struct AsyncTtlCache<K, V>
where
    K: Hash + Eq,
{
    cache: Arc<RwLock<TtlCache<K, CacheItem<V>>>>,
    statistics: Arc<Statistics>,
    ttl: Duration,
    max_capacity: usize,
}

impl<K, V> Clone for AsyncTtlCache<K, V>
where
    K: Hash + Eq,
{
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            statistics: self.statistics.clone(),
            ttl: self.ttl,
            max_capacity: self.max_capacity,
        }
    }
}

impl<K, V> AsyncTtlCache<K, V>
where
    K: Hash + Eq,
{
    pub fn new(ttl: Duration, max_capacity: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(TtlCache::new())),
            ttl,
            max_capacity,
            statistics: Arc::new(Statistics::new()),
        }
    }
}

impl<K, V> AsyncTtlCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub async fn get_or_insert<F>(&self, key: K, val_fut: F) -> anyhow::Result<Option<V>>
    where
        F: Future<Output = anyhow::Result<Option<V>>> + Send + 'static,
    {
        if let Some(x) = self.get(&key) {
            self.statistics().inc_hit();
            return Ok(x.await?);
        }
        let get_value = val_fut.map_err(CloneError::new).boxed().shared();

        Ok(self.insert(key, get_value).await?)
    }

    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }

    fn get(&self, key: &K) -> Option<CacheItem<V>> {
        let running_futures = self.cache.read();
        let cache_item = running_futures.get(key)?;
        Some(cache_item.clone())
    }

    async fn insert(
        &self,
        key: K,
        get_value: Shared<BoxFuture<'static, Result<Option<V>, CloneError>>>,
    ) -> Result<Option<V>, CloneError> {
        let key_or_cache = {
            let mut cache = self.cache.write();
            if let Some(cache_item) = cache.get(&key) {
                self.statistics.inc_hit();
                KeyOrCacheItem::Cache(cache_item.clone())
            } else {
                if cache.len() >= self.max_capacity {
                    cache.remove_oldest();
                } else {
                    cache.remove_expired(2);
                }
                let old = cache.insert(key.clone(), get_value.clone(), self.ttl);
                if old.is_some() {
                    self.statistics().inc_replace();
                } else {
                    self.statistics().inc_miss();
                }
                self.statistics().set_len(cache.len());
                KeyOrCacheItem::Key(key)
            }
        };
        match key_or_cache {
            KeyOrCacheItem::Key(key) => {
                let remove_guard = RemoveGuard {
                    map: &*self.cache,
                    key: &key,
                };
                match get_value.await {
                    Ok(Some(result)) => {
                        std::mem::forget(remove_guard);
                        Ok(Some(result))
                    }
                    Ok(None) => Ok(None),
                    Err(err) => Err(err),
                }
            }
            KeyOrCacheItem::Cache(c) => c.await,
        }
    }
}

enum KeyOrCacheItem<K, V> {
    Key(K),
    Cache(CacheItem<V>),
}

pub struct Statistics {
    hits: AtomicUsize,
    misses: AtomicUsize,
    replaces: AtomicUsize,
    len: AtomicUsize,
}

impl Statistics {
    fn new() -> Self {
        Self {
            hits: AtomicUsize::default(),
            misses: AtomicUsize::default(),
            replaces: AtomicUsize::default(),
            len: AtomicUsize::default(),
        }
    }

    pub fn hits(&self) -> usize {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> usize {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn replaces(&self) -> usize {
        self.replaces.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    fn inc_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_replace(&self) {
        self.replaces.fetch_add(1, Ordering::Relaxed);
    }

    fn set_len(&self, len: usize) {
        self.len.store(len, Ordering::Relaxed)
    }
}

type CacheItem<V> = Shared<BoxFuture<'static, Result<Option<V>, CloneError>>>;

#[derive(Clone, Debug, Display)]
struct CloneError(Arc<anyhow::Error>);

impl CloneError {
    fn new(err: anyhow::Error) -> Self {
        Self(Arc::new(err))
    }
}

impl std::error::Error for CloneError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some((*self.0).as_ref())
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

struct RemoveGuard<'a, K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    map: &'a RwLock<TtlCache<K, CacheItem<V>>>,
    key: &'a K,
}

impl<'a, K, V> Drop for RemoveGuard<'a, K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    fn drop(&mut self) {
        let mut guard = self.map.write();
        guard.remove(self.key);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::AsyncTtlCache;
    use anyhow::anyhow;

    #[async_std::test]
    async fn should_cache_computing_futures() {
        let cache = AsyncTtlCache::new(Duration::from_secs(1), 5);
        let f2_ran = Arc::new(Mutex::new(false));
        let f1 = async {
            async_std::task::sleep(std::time::Duration::from_millis(50)).await;
            Ok(Some(1))
        };
        let f2 = {
            let f2_ran = f2_ran.clone();
            async move {
                *f2_ran.lock().unwrap() = true;
                Ok(Some(2))
            }
        };

        let (res1, res2) =
            futures::future::try_join(cache.get_or_insert(1, f1), cache.get_or_insert(1, f2))
                .await
                .unwrap();

        assert!(!*f2_ran.lock().unwrap());
        assert_eq!(res1, Some(1));
        assert_eq!(res2, Some(1));
        assert_eq!(cache.statistics().hits(), 1);
        assert_eq!(cache.statistics().misses(), 1);
    }

    #[async_std::test]
    async fn should_not_cache_failed_futures() {
        let cache = AsyncTtlCache::new(Duration::from_secs(1), 5);
        let f2_ran = Arc::new(Mutex::new(false));
        let f1 = async {
            async_std::task::sleep(std::time::Duration::from_millis(50)).await;
            Err(anyhow!("Err"))
        };
        let f2 = {
            let f2_ran = f2_ran.clone();
            async move {
                *f2_ran.lock().unwrap() = true;
                Ok(Some(2))
            }
        };

        let (res1, res2) =
            futures::future::join(cache.get_or_insert(1, f1), cache.get_or_insert(1, f2)).await;
        let res3 = cache.get_or_insert(1, async { Ok(Some(5)) }).await.unwrap();

        assert!(!*f2_ran.lock().unwrap());
        assert!(res1.is_err());
        assert!(res2.is_err());
        assert_eq!(res3, Some(5));
        assert_eq!(cache.statistics().hits(), 1);
        assert_eq!(cache.statistics().misses(), 2);
    }

    #[async_std::test]
    async fn should_not_cache_nones() {
        let cache = AsyncTtlCache::new(Duration::from_secs(1), 5);
        let f2_ran = Arc::new(Mutex::new(false));
        let f1 = async {
            async_std::task::sleep(std::time::Duration::from_millis(50)).await;
            Ok(None)
        };
        let f2 = {
            let f2_ran = f2_ran.clone();
            async move {
                *f2_ran.lock().unwrap() = true;
                Ok(Some(2))
            }
        };

        let (res1, res2) =
            futures::future::try_join(cache.get_or_insert(1, f1), cache.get_or_insert(1, f2))
                .await
                .unwrap();
        let res3 = cache.get_or_insert(1, async { Ok(Some(5)) }).await.unwrap();

        assert!(!*f2_ran.lock().unwrap());
        assert!(res1.is_none());
        assert!(res2.is_none());
        assert_eq!(res3, Some(5));
        assert_eq!(cache.statistics().hits(), 1);
        assert_eq!(cache.statistics().misses(), 2);
    }

    #[async_std::test]
    async fn should_recompute_when_ttl_expired() {
        let cache = AsyncTtlCache::new(Duration::from_millis(10), 5);
        let f2_ran = Arc::new(Mutex::new(false));
        let f1 = async {
            async_std::task::sleep(std::time::Duration::from_millis(50)).await;
            Ok(Some(1))
        };
        let f2 = {
            let f2_ran = f2_ran.clone();
            async move {
                *f2_ran.lock().unwrap() = true;
                Ok(Some(2))
            }
        };

        let res1 = cache.get_or_insert(1, f1).await.unwrap();
        let res2 = cache.get_or_insert(1, f2).await.unwrap();

        assert!(*f2_ran.lock().unwrap());
        assert_eq!(res1, Some(1));
        assert_eq!(res2, Some(2));
        assert_eq!(cache.statistics().hits(), 0);
        assert_eq!(cache.statistics().misses(), 1);
        assert_eq!(cache.statistics().replaces(), 1);
    }

    #[async_std::test]
    async fn should_not_exceed_capacity() {
        let cache = AsyncTtlCache::new(Duration::from_millis(10), 3);
        cache.get_or_insert(1, async { Ok(Some(1)) }).await.unwrap();
        cache.get_or_insert(2, async { Ok(Some(1)) }).await.unwrap();
        cache.get_or_insert(3, async { Ok(Some(1)) }).await.unwrap();
        assert_eq!(cache.statistics().len(), 3);

        cache.get_or_insert(4, async { Ok(Some(1)) }).await.unwrap();

        assert_eq!(cache.statistics().len(), 3);
    }
}
