use crate::db::{create_pool, ConnectionPool};
use diesel::Connection;
use std::env::var;

const TIMEOUT: u64 = 10;

#[derive(Clone)]
pub struct TestDb {
    connection_pool: ConnectionPool,
}

impl TestDb {
    pub fn new() -> Self {
        let url = var("DATABASE_URL").expect("DATABASE_URL must be specified");
        let connection_pool = create_pool(&url, 1, None, TIMEOUT);

        let conn = connection_pool
            .get()
            .expect("Failed to get connection from pool");

        conn.begin_test_transaction()
            .expect("Failed to begin test transaction");

        Self { connection_pool }
    }

    pub fn connection_pool(&self) -> &ConnectionPool {
        &self.connection_pool
    }
}
