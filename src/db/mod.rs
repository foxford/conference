use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use std::sync::Arc;
use std::time::Duration;

pub(crate) type ConnectionPool = Arc<Pool<ConnectionManager<PgConnection>>>;

pub(crate) fn create_pool(
    url: &str,
    size: u32,
    idle_size: Option<u32>,
    timeout: u64,
) -> ConnectionPool {
    let manager = ConnectionManager::<PgConnection>::new(url);

    let builder = Pool::builder()
        .max_size(size)
        .min_idle(idle_size)
        .connection_timeout(Duration::from_secs(timeout));

    let pool = builder
        .build(manager)
        .expect("Error creating a database pool");
    Arc::new(pool)
}

pub mod sql {
    pub use super::agent::Agent_status;
    pub use super::recording::Recording_status;
    pub use super::room::Room_backend;
    pub use super::rtc::Rtc_sharing_policy;
    pub use svc_agent::sql::{Account_id, Agent_id};
}

pub(crate) mod agent;
pub(crate) mod agent_connection;
pub(crate) mod janus_backend;
pub(crate) mod janus_rtc_stream;
pub(crate) mod recording;
pub(crate) mod room;
pub(crate) mod rtc;
