use config;
use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct Identity {
    pub label: String,
    pub audience: String,
    pub account_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Backend {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Mqtt {
    pub uri: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    pub(crate) identity: Identity,
    pub(crate) backend: Backend,
    pub(crate) mqtt: Mqtt,
}

pub(crate) fn load() -> Result<Config, config::ConfigError> {
    let mut parser = config::Config::default();
    parser.merge(config::File::with_name("App"))?;
    parser.merge(config::Environment::with_prefix("APP").separator("__"))?;
    parser.try_into::<Config>()
}
