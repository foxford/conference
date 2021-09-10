use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Duration, Utc};
use derive_more::Display;
use futures::{
    future::{BoxFuture, Shared},
    Future, FutureExt, TryFutureExt,
};
pub struct Cache<K, V>
where
    K: std::hash::Hash + Eq + PartialEq + Clone,
    V: Send + Clone,
{
    cache: Arc<RwLock<HashMap<K, CacheItem<V>>>>,
    ttl: Duration,
    max_capacity: usize,
}

impl<K, V> Cache<K, V>
where
    K: std::hash::Hash + Eq + PartialEq + Clone,
    V: Send + Clone,
{
    pub async fn get_or_insert<F>(
        &self,
        key: K,
        val_fut: F,
        now: DateTime<Utc>,
    ) -> Result<V, anyhow::Error>
    where
        F: Future<Output = Result<V, anyhow::Error>> + Send + 'static,
    {
        if let Some(x) = self.get(&key, now).await? {
            return Ok(x);
        }
        let get_value = val_fut.map_err(CloneError::new).boxed().shared();

        Ok(self.insert(key, get_value, now).await?)
    }

    async fn get(&self, key: &K, now: DateTime<Utc>) -> Result<Option<V>, CloneError> {
        let cache_item = {
            let running_futures = self.cache.read().expect("Cache lock poisoned");
            running_futures.get(key).cloned()
        };
        match cache_item {
            Some(CacheItem { added_at, item }) if added_at + self.ttl > now => {
                Ok(Some(item.await?))
            }
            Some(_) => Ok(None),
            None => Ok(None),
        }
    }

    async fn insert(
        &self,
        key: K,
        get_value: Shared<BoxFuture<'static, Result<V, CloneError>>>,
        now: DateTime<Utc>,
    ) -> Result<V, CloneError> {
        let mut cache = self.cache.write().expect("Cache lock poisoned");
        if cache.len() > self.max_capacity {
            *cache = HashMap::new()
        }
        match cache.entry(key) {
            Entry::Occupied(o) => o.get().item.clone().await,
            Entry::Vacant(v) => {
                let key = v.key().clone();
                v.insert(CacheItem {
                    item: get_value.clone(),
                    added_at: now,
                });
                drop(cache);
                let remove_guard = RemoveGuard {
                    map: &*self.cache,
                    key: &key,
                };
                match get_value.await {
                    Ok(result) => {
                        std::mem::forget(remove_guard);
                        Ok(result)
                    }
                    Err(err) => Err(err),
                }
            }
        }
    }
}

#[derive(Clone)]
struct CacheItem<V> {
    item: Shared<BoxFuture<'static, Result<V, CloneError>>>,
    added_at: DateTime<Utc>,
}

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
    K: std::hash::Hash + Eq + PartialEq + Clone,
    V: Send + Clone,
{
    map: &'a RwLock<HashMap<K, CacheItem<V>>>,
    key: &'a K,
}

impl<'a, K, V> Drop for RemoveGuard<'a, K, V>
where
    K: std::hash::Hash + Eq + PartialEq + Clone,
    V: Send + Clone,
{
    fn drop(&mut self) {
        let mut guard = self.map.write().expect("Cache lock poisoned");
        guard.remove(self.key);
    }
}
