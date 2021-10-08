use std::{sync::Arc, time::Duration};

use diesel::r2d2::Pool;
use r2d2_redis::{redis::Commands, redis::ToRedisArgs, RedisConnectionManager};
use serde::{de::DeserializeOwned, Serialize};

#[derive(Clone)]
pub struct Wait {
    redis_client: Arc<Pool<RedisConnectionManager>>,
    wait_timeout: Duration,
}

impl Wait {
    pub fn new(redis_client: Arc<Pool<RedisConnectionManager>>, wait_timeout: Duration) -> Self {
        Self {
            redis_client,
            wait_timeout,
        }
    }

    pub async fn wait_key<K, T>(&self, k: K) -> anyhow::Result<T>
    where
        K: ToRedisArgs + Send + 'static,
        T: DeserializeOwned + Send + 'static,
    {
        let client = self.redis_client.clone();
        let wait_timeout = self.wait_timeout;
        Ok(tokio::task::spawn_blocking(move || {
            let mut conn = client.get()?;
            let (_, bytes): (String, String) =
                dbg!(conn.blpop(k, wait_timeout.as_secs() as usize)?);
            Ok::<_, anyhow::Error>(serde_json::from_str(&bytes)?)
        })
        .await??)
    }

    pub async fn put_value<K, T>(&self, k: K, value: T) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + 'static,
        T: Serialize + Send + 'static,
    {
        let client = self.redis_client.clone();
        Ok(tokio::task::spawn_blocking(move || {
            let serialized = serde_json::to_string(&value)?;
            dbg!(&serialized);
            let mut conn = client.get()?;
            conn.lpush(k, serialized)?;
            Ok::<_, anyhow::Error>(())
        })
        .await??)
    }
}
