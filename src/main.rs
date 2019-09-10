extern crate openssl;
#[macro_use]
extern crate diesel;
#[macro_use]
extern crate diesel_derive_enum;

use futures::executor;
use std::env::var;

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
        let timeout = var("DATABASE_POOL_TIMEOUT")
            .map(|val| {
                val.parse::<u64>()
                    .expect("Error converting DATABASE_POOL_TIMEOUT variable into u64")
            })
            .unwrap_or_else(|_| 5);

        crate::db::create_pool(&url, size, timeout)
    };

    executor::block_on(app::run(&db)).expect("Error running an executor");
}

mod app;
mod backend;
mod db;
mod schema;
mod serde;
mod util;
