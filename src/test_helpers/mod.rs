use failure::{err_msg, Error};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::Publishable;

pub(crate) mod test_agent;

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
