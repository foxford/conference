#[macro_use]
extern crate diesel;

use std::env::var;

use anyhow::Result;
use svc_authz::cache::{create_pool, Cache};
use tracing_subscriber::{
    fmt::{format::FmtSpan, time},
    prelude::__tracing_subscriber_SubscriberExt,
    EnvFilter,
};

#[async_std::main]
async fn main() -> Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    let subscriber = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        // .with_timer(time::ChronoUtc::rfc3339())
        .json();
    let subscriber = tracing_subscriber::registry()
        // .with(crate::subs::ErrorLayer::default())
        .with(EnvFilter::default())
        .with(subscriber);

    tracing::subscriber::set_global_default(subscriber)?;

    let db = {
        let url = var("DATABASE_URL").expect("DATABASE_URL must be specified");
        let size = var("DATABASE_POOL_SIZE")
            .map(|val| {
                val.parse::<u32>()
                    .expect("Error converting DATABASE_POOL_SIZE variable into u32")
            })
            .unwrap_or_else(|_| 5);

        let idle_size = var("DATABASE_POOL_IDLE_SIZE")
            .map(|val| {
                val.parse::<u32>()
                    .expect("Error converting DATABASE_POOL_IDLE_SIZE variable into u32")
            })
            .ok();

        let timeout = var("DATABASE_POOL_TIMEOUT")
            .map(|val| {
                val.parse::<u64>()
                    .expect("Error converting DATABASE_POOL_TIMEOUT variable into u64")
            })
            .unwrap_or_else(|_| 5);

        crate::db::create_pool(&url, size, idle_size, timeout)
    };

    let (redis_pool, authz_cache) = if let Some("1") = var("CACHE_ENABLED").ok().as_deref() {
        let url = var("CACHE_URL").expect("CACHE_URL must be specified");

        let size = var("CACHE_POOL_SIZE")
            .map(|val| {
                val.parse::<u32>()
                    .expect("Error converting CACHE_POOL_SIZE variable into u32")
            })
            .unwrap_or_else(|_| 5);

        let timeout = var("CACHE_POOL_TIMEOUT")
            .map(|val| {
                val.parse::<u64>()
                    .expect("Error converting CACHE_POOL_TIMEOUT variable into u64")
            })
            .unwrap_or_else(|_| 5);

        let expiration_time = var("CACHE_EXPIRATION_TIME")
            .map(|val| {
                val.parse::<u64>()
                    .expect("Error converting CACHE_EXPIRATION_TIME variable into u64")
            })
            .unwrap_or_else(|_| 300);

        let pool = create_pool(&url, size, timeout);
        let cache = Cache::new(pool.clone(), expiration_time);
        (Some(pool), Some(cache))
    } else {
        (None, None)
    };

    app::run(&db, redis_pool, authz_cache).await
}

mod app;
mod backend;
mod config;
mod db;
#[allow(unused_imports)]
mod schema;
mod serde;
#[cfg(test)]
mod test_helpers;
mod util;
