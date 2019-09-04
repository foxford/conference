use failure::{err_msg, Error};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::Publishable;

pub(crate) mod test_agent;
pub(crate) mod test_db;

pub(crate) fn extract_payload(message: Box<dyn Publishable>) -> Result<JsonValue, Error> {
    let message_str = message.into_bytes()?;
    let message_value = serde_json::from_slice::<JsonValue>(message_str.as_bytes())?;

    let payload_value = message_value
        .get("payload")
        .ok_or_else(|| err_msg("Missing payload"))?;

    let payload_str = payload_value
        .as_str()
        .ok_or_else(|| err_msg("Failed to cast 'payload' field to string"))?;

    serde_json::from_slice::<JsonValue>(payload_str.as_bytes())
        .map_err(|_err| err_msg("Failed to parse 'payload' as JSON"))
}

pub(crate) fn extract_properties(envelope: &OutgoingEnvelope) -> Result<JsonValue, Error> {
    let envelope_str = envelope.to_bytes()?;
    let envelope_value = serde_json::from_slice::<JsonValue>(envelope_str.as_bytes())?;

    envelope_value
        .get("properties")
        .map(|p| p.to_owned())
        .ok_or_else(|| err_msg("Missing properties"))
}

pub(crate) fn build_authz(audience: &str) -> svc_authz::ClientMap {
    let mut authz_config_map = svc_authz::ConfigMap::new();

    let authz_none_config = svc_authz::Config::None(svc_authz::NoneConfig {});
    authz_config_map.insert(audience.to_owned(), authz_none_config);

    let account_id = svc_authn::AccountId::new("conference", audience);
    svc_authz::ClientMap::new(&account_id, authz_config_map).expect("Failed to build authz")
}
