use std::collections::HashMap;

use serde_derive::Deserialize;
use svc_agent::{mqtt::AgentConfig, AccountId};
use svc_authn::jose::Algorithm;
use svc_authz::ConfigMap as Authz;
use svc_error::extension::sentry::Config as SentryConfig;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Config {
    pub(crate) id: AccountId,
    pub(crate) id_token: JwtConfig,
    pub(crate) agent_label: String,
    pub(crate) broker_id: AccountId,
    pub(crate) authz: Authz,
    pub(crate) mqtt: AgentConfig,
    pub(crate) sentry: Option<SentryConfig>,
    pub(crate) backend: BackendConfig,
    pub(crate) upload: UploadConfigs,
    #[serde(default)]
    pub(crate) telemetry: TelemetryConfig,
    #[serde(default)]
    pub(crate) kruonis: KruonisConfig,
    pub(crate) metrics: Option<MetricsConfig>,
    pub(crate) max_room_duration: Option<i64>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct JwtConfig {
    #[serde(deserialize_with = "svc_authn::serde::algorithm")]
    pub(crate) algorithm: Algorithm,
    #[serde(deserialize_with = "svc_authn::serde::file")]
    pub(crate) key: Vec<u8>,
}

pub(crate) fn load() -> Result<Config, config::ConfigError> {
    let mut parser = config::Config::default();
    parser.merge(config::File::with_name("App"))?;
    parser.merge(config::Environment::with_prefix("APP").separator("__"))?;
    parser.try_into::<Config>()
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct BackendConfig {
    pub(crate) id: AccountId,
    pub(crate) default_timeout: u64,
    pub(crate) stream_upload_timeout: u64,
    pub(crate) transaction_watchdog_check_period: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct UploadConfigs {
    pub(crate) shared: UploadConfigMap,
    pub(crate) owned: UploadConfigMap,
}

pub(crate) type UploadConfigMap = HashMap<String, UploadConfig>;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct UploadConfig {
    pub(crate) backend: String,
    pub(crate) bucket: String,
}

#[derive(Clone, Debug, Deserialize, Default)]
pub(crate) struct TelemetryConfig {
    pub(crate) id: Option<AccountId>,
}

#[derive(Clone, Debug, Deserialize, Default)]
pub(crate) struct KruonisConfig {
    pub(crate) id: Option<AccountId>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricsConfig {
    pub http: MetricsHttpConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricsHttpConfig {
    pub bind_address: std::net::SocketAddr,
}
