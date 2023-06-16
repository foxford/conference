use sqlx::postgres::PgPool;

const TIMEOUT: u64 = 10;

#[derive(Clone)]
pub struct TestDb {
    pub pool: PgPool,
}

impl TestDb {
    pub async fn new() -> Self {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be specified");
        let pool = crate::db::create_pool(&url, 10, None, TIMEOUT, 1800).await;

        Self { pool }
    }

    pub async fn get_conn(&self) -> sqlx::pool::PoolConnection<sqlx::Postgres> {
        self.pool
            .acquire()
            .await
            .expect("failed to acquire connection")
    }
}
