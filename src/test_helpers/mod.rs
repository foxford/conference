use failure::{err_msg, format_err, Error};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use svc_agent::mqtt::Publishable;

pub(crate) mod agent;
pub(crate) mod db;
pub(crate) mod factory;

pub(crate) fn extract_payload<T>(message: Box<dyn Publishable>) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    let message_str = message.into_bytes()?;
    let message_value = serde_json::from_slice::<JsonValue>(message_str.as_bytes())?;
    parse_payload(message_value)
}

pub(crate) fn parse_payload<T>(message_value: JsonValue) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    let payload_value = message_value
        .get("payload")
        .ok_or_else(|| err_msg("Missing payload"))?;

    let payload_str = payload_value
        .as_str()
        .ok_or_else(|| err_msg("Failed to cast 'payload' field to string"))?;

    serde_json::from_slice::<T>(payload_str.as_bytes())
        .map_err(|err| format_err!("Failed to parse 'payload' as JSON: {}", err))
}

pub(crate) fn no_authz(audience: &str) -> svc_authz::ClientMap {
    let mut authz_config_map = svc_authz::ConfigMap::new();

    let authz_none_config = svc_authz::Config::None(svc_authz::NoneConfig {});
    authz_config_map.insert(audience.to_owned(), authz_none_config);

    let account_id = svc_authn::AccountId::new("conference", audience);
    svc_authz::ClientMap::new(&account_id, authz_config_map).expect("Failed to build authz")
}
