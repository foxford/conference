use crate::db::{create_pool, ConnectionPool};

use super::test_deps::PostgresHandle;

const TIMEOUT: u64 = 10;

#[derive(Clone)]
pub struct TestDb {
    connection_pool: ConnectionPool,
}

impl TestDb {
    pub fn with_local_postgres(postgres: &PostgresHandle) -> Self {
        let connection_pool = create_pool(&postgres.connection_string, 10, None, TIMEOUT);
        diesel_migrations::run_pending_migrations(
            &connection_pool
                .get()
                .expect("Failed to get connection from pool"),
        )
        .expect("Migrations err");

        Self { connection_pool }
    }

    pub fn connection_pool(&self) -> &ConnectionPool {
        &self.connection_pool
    }

    pub fn get_conn(
        &self,
    ) -> diesel::r2d2::PooledConnection<diesel::r2d2::ConnectionManager<diesel::PgConnection>> {
        self.connection_pool
            .get()
            .expect("failed to get db connection")
    }
}
