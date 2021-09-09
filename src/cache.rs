use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, RwLock},
    time::Duration,
};

use derive_more::Display;
use futures::{
    future::{BoxFuture, Shared},
    Future, FutureExt, TryFutureExt,
};
use r2d2::Pool;
use r2d2_redis::{redis::Commands, RedisConnectionManager};
use serde::{de::DeserializeOwned, Serialize};

pub struct Cache {
    redis: Arc<Pool<RedisConnectionManager>>,
    running_futures:
        Arc<RwLock<HashMap<Key, Shared<BoxFuture<'static, Result<Value, CloneError>>>>>>,
    ttl: Duration,
}

impl Cache {
    pub async fn get_or_insert<K, V, F>(&self, key: K, val_fut: F) -> Result<V, anyhow::Error>
    where
        K: AsRef<[u8]>,
        V: Serialize + DeserializeOwned,
        F: Future<Output = Result<V, anyhow::Error>> + Send + 'static,
    {
        let key = Key::from_bytes(key.as_ref())?;
        if let Some(x) = self.get(key.clone()).await? {
            return x.into_val();
        }

        let pool = self.redis.clone();
        let ttl = self.ttl;
        let get_value = {
            let key = key.clone();
            async move {
                let val = Value::from_val(&val_fut.await?)?;
                async_std::task::spawn_blocking(move || {
                    let mut connection = pool.get()?;
                    connection.set_ex(key.as_bytes(), val.as_bytes(), ttl.as_secs() as usize)?;
                    Ok::<_, anyhow::Error>(val)
                })
                .await
            }
        }
        .map_err(CloneError::new)
        .boxed()
        .shared();

        self.insert(key, get_value).await?.into_val()
    }

    async fn get(&self, key: Key) -> Result<Option<Value>, CloneError> {
        let running_future = {
            let running_futures = self.running_futures.read().expect("Cache lock poisoned");
            running_futures.get(&key).cloned()
        };
        match running_future {
            Some(f) => Ok(Some(f.await?)),
            None => {
                let pool = self.redis.clone();
                async_std::task::spawn_blocking(move || {
                    let mut connection = pool.get()?;
                    let bytes: Option<Vec<u8>> = connection.get(key.as_bytes())?;
                    Ok::<_, anyhow::Error>(bytes.map(Value::from_bytes))
                })
                .await
                .map_err(CloneError::new)
            }
        }
    }

    async fn insert(
        &self,
        key: Key,
        get_value: Shared<BoxFuture<'static, Result<Value, CloneError>>>,
    ) -> Result<Value, CloneError> {
        let (running_future, _remove_guard) = {
            let mut running_futures = self.running_futures.write().expect("Cache lock poisoned");
            match running_futures.entry(key) {
                Entry::Occupied(o) => (o.get().clone(), None),
                Entry::Vacant(v) => {
                    let key = v.key().clone();
                    v.insert(get_value.clone());
                    let remove_guard = RemoveGuard {
                        map: &*self.running_futures,
                        key,
                    };
                    (get_value, Some(remove_guard))
                }
            }
        };
        running_future.await
    }
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

struct RemoveGuard<'a> {
    map: &'a RwLock<HashMap<Key, Shared<BoxFuture<'static, Result<Value, CloneError>>>>>,
    key: Key,
}

impl<'a> Drop for RemoveGuard<'a> {
    fn drop(&mut self) {
        let mut guard = self.map.write().expect("Cache lock poisoned");
        guard.remove(&self.key);
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct Key(Vec<u8>);

impl Key {
    fn from_bytes(bytes: &[u8]) -> Result<Self, anyhow::Error> {
        Ok(Self(bytes.to_owned()))
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct Value(Vec<u8>);

impl Value {
    fn from_val(val: &impl Serialize) -> Result<Self, anyhow::Error> {
        Ok(Self(serde_json::to_vec(val)?))
    }

    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    fn into_val<V: DeserializeOwned>(&self) -> Result<V, anyhow::Error> {
        Ok(serde_json::from_slice(&self.0)?)
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
