#[macro_use]
extern crate diesel;

use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use std::sync::Arc;

pub(crate) type PgPool = Arc<Pool<ConnectionManager<PgConnection>>>;

fn main() {
    env_logger::init();
    app::run(&create_database_pool());
}

pub fn create_database_pool() -> PgPool {
    use std::env;

    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be specified");
    let size = env::var("DATABASE_POOL_SIZE")
        .map(|val| {
            val.parse::<u32>()
                .expect("Error converting DATABASE_POOL_SIZE variable into u32")
        })
        .unwrap_or_else(|_| 5);

    let manager = ConnectionManager::<PgConnection>::new(url);
    let pool = Pool::builder()
        .max_size(size)
        .build(manager)
        .expect("Error creating a database pool");

    Arc::new(pool)
}

mod app;
mod backend;
mod schema;
mod transport;

pub mod sql {
    pub use crate::transport::sql::{Account_id, Agent_id};
}
