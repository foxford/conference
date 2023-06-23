use sqlx::postgres::PgPool;

#[derive(Clone)]
pub struct TestDb {
    pub pool: PgPool,
}

impl TestDb {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_conn(&self) -> sqlx::pool::PoolConnection<sqlx::Postgres> {
        self.pool
            .acquire()
            .await
            .expect("failed to acquire connection")
    }
}
