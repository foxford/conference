use std::env::var;

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Executor;
use tokio::sync::OnceCell;

static DB_TRUNCATE: OnceCell<bool> = OnceCell::const_new();

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

        // todo: we should actually run every test in transaction, but thats not possible for now, maybe in sqlx 0.6
        DB_TRUNCATE
            .get_or_init(|| async {
                let mut conn = pool.acquire().await.expect("Failed to get DB connection");

                conn.execute("TRUNCATE class CASCADE;")
                    .await
                    .expect("Failed to truncate class table");

                true
            })
            .await;
        Self { pool }
    }
}
