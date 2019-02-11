use config;
use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    pub(crate) id: crate::transport::util::AccountId,
    pub(crate) backend_id: crate::transport::util::AccountId,
    pub(crate) authz: authz::ConfigMap,
    pub(crate) mqtt: crate::transport::util::mqtt::AgentConfig,
}

pub(crate) fn load() -> Result<Config, config::ConfigError> {
    let mut parser = config::Config::default();
    parser.merge(config::File::with_name("App"))?;
    parser.merge(config::Environment::with_prefix("APP").separator("__"))?;
    parser.try_into::<Config>()
}
