use diesel::{
    pg::PgConnection,
    r2d2::{ConnectionManager, Pool},
};
use std::{sync::Arc, time::Duration};

pub type ConnectionPool = Arc<Pool<ConnectionManager<PgConnection>>>;

pub fn create_pool(url: &str, size: u32, idle_size: Option<u32>, timeout: u64) -> ConnectionPool {
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
    pub use super::{
        agent::Agent_status, recording::Recording_status, room::Room_backend,
        rtc::Rtc_sharing_policy,
    };
    pub use svc_agent::sql::{Account_id, Agent_id};
}

pub mod agent;
pub mod agent_connection;
pub mod janus_backend;
pub mod janus_rtc_stream;
pub mod recording;
pub mod room;
pub mod rtc;
pub mod rtc_reader_config;
pub mod rtc_writer_config;
