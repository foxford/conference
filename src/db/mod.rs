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

pub async fn create_pool_sqlx(
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

macro_rules! impl_jsonb {
    ( $name:ident ) => {
        impl ::diesel::deserialize::FromSql<::diesel::sql_types::Jsonb, ::diesel::pg::Pg>
            for $name
        {
            fn from_sql(bytes: Option<&[u8]>) -> diesel::deserialize::Result<Self> {
                let value = <::serde_json::Value as ::diesel::deserialize::FromSql<
                    ::diesel::sql_types::Jsonb,
                    ::diesel::pg::Pg,
                >>::from_sql(bytes)?;
                Ok(::serde_json::from_value(value)?)
            }
        }

        impl ::diesel::serialize::ToSql<::diesel::sql_types::Jsonb, ::diesel::pg::Pg> for $name {
            fn to_sql<W: ::std::io::Write>(
                &self,
                out: &mut ::diesel::serialize::Output<W, Pg>,
            ) -> ::diesel::serialize::Result {
                let value = ::serde_json::to_value(self)?;
                <::serde_json::Value as ::diesel::serialize::ToSql<
                    ::diesel::sql_types::Jsonb,
                    ::diesel::pg::Pg,
                >>::to_sql(&value, out)
            }
        }
    };
}

pub mod sql {
    pub use super::{
        agent::Agent_status, agent_connection::Agent_connection_status,
        recording::Recording_status, room::Room_backend, rtc::Rtc_sharing_policy,
    };
    pub use svc_agent::sql::{Account_id, Agent_id};
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
