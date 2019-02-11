#[macro_use]
extern crate diesel;

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

        crate::db::create_database_pool(&url, size)
    };

    app::run(&db);
}

mod app;
mod backend;
mod db;
mod schema;
mod serde;
mod transport;
