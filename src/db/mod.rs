use std::time::Duration;
use svc_agent::AgentId;

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

#[derive(sqlx::Encode)]
pub struct AgentIds<'a>(&'a [&'a AgentId]);

impl sqlx::Type<sqlx::Postgres> for AgentIds<'_> {
    fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
        AgentId::type_info()
    }
}

#[derive(sqlx::Encode)]
pub struct Ids<'a>(&'a [id::Id]);

impl sqlx::Type<sqlx::Postgres> for Ids<'_> {
    fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
        id::Id::type_info()
    }
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
