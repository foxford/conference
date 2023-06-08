use sqlx::postgres::PgPool;

use super::test_deps::PostgresHandle;

const TIMEOUT: u64 = 10;

#[derive(Clone)]
pub struct TestDb {
    pub pool: PgPool,
}

impl TestDb {
    pub async fn with_local_postgres(postgres: &PostgresHandle<'_>) -> Self {
        let pool =
            crate::db::create_pool_sqlx(&postgres.connection_string, 10, None, TIMEOUT, 1800).await;

        Self { pool }
    }

    pub async fn get_conn(&self) -> sqlx::pool::PoolConnection<sqlx::Postgres> {
        self.pool
            .acquire()
            .await
            .expect("failed to acquire connection")
    }
}
