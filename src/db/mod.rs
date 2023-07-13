use std::time::Duration;

pub async fn create_pool(
    url: &str,
    size: u32,
    idle_size: Option<u32>,
    timeout: u64,
    max_lifetime: u64,
) -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(size)
        .min_connections(idle_size.unwrap_or(1))
        .acquire_timeout(Duration::from_secs(timeout))
        .max_lifetime(Duration::from_secs(max_lifetime))
        .connect(url)
        .await
        .expect("Failed to create sqlx database pool")
}

pub mod agent;
pub mod agent_connection;
pub mod group_agent;
pub mod id;
pub mod janus_backend;
pub mod janus_rtc_stream;
pub mod orphaned_room;
pub mod recording;
pub mod room;
pub mod rtc;
pub mod rtc_reader_config;
pub mod rtc_writer_config;
pub mod rtc_writer_config_snapshot;
pub mod ban_account;
