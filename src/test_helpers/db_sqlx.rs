use std::env::var;

use sqlx::postgres::{PgPool, PgPoolOptions};

#[derive(Clone)]
pub struct TestDb {
    pub pool: PgPool,
}

impl TestDb {
    pub async fn new() -> Self {
        #[cfg(feature = "dotenv")]
        dotenv::dotenv().ok();

        let url = var("DATABASE_URL").expect("DATABASE_URL must be specified");

        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .connect(&url)
            .await
            .expect("Failed to connect to the DB");

        Self { pool }
    }
}
