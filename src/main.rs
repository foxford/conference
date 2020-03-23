#![feature(try_trait)]

extern crate openssl;
#[macro_use]
extern crate diesel;
#[macro_use]
extern crate diesel_derive_enum;

use std::env::var;

use futures::executor;
use svc_authz::cache::{create_pool, Cache};

fn main() {
    env_logger::init();

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

    let authz_cache = var("CACHE_URL").ok().map(|url| {
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

        Cache::new(create_pool(&url, size, timeout), expiration_time)
    });

    executor::block_on(app::run(&db, authz_cache)).expect("Error running an executor");
}

mod app;
mod backend;
mod db;
#[allow(unused_imports)]
mod schema;
mod serde;
#[cfg(test)]
mod test_helpers;
mod util;
