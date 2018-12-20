use crate::transport::AccountId;
use config;
use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct Mqtt {
    pub(crate) uri: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    pub(crate) id: AccountId,
    pub(crate) backend_id: AccountId,
    pub(crate) mqtt: Mqtt,
}

pub(crate) fn load() -> Result<Config, config::ConfigError> {
    let mut parser = config::Config::default();
    parser.merge(config::File::with_name("App"))?;
    parser.merge(config::Environment::with_prefix("APP").separator("__"))?;
    parser.try_into::<Config>()
}
