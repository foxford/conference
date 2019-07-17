use failure::{err_msg, Error};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{compat::OutgoingEnvelope, Publishable};

pub(crate) mod test_agent;

pub(crate) fn extract_payload(envelope: &OutgoingEnvelope) -> Result<JsonValue, Error> {
    let envelope_str = envelope.to_bytes()?;
    let envelope_value = serde_json::from_slice::<JsonValue>(envelope_str.as_bytes())?;

    let payload_value = envelope_value
        .get("payload")
        .ok_or_else(|| err_msg("Missing payload"))?;
        
    let payload_str = payload_value
        .as_str()
        .ok_or_else(|| err_msg("Failed to cast 'payload' field to string"))?;

    serde_json::from_slice::<JsonValue>(payload_str.as_bytes())
        .map_err(|_err| err_msg("Failed to parse 'payload' as JSON"))
}
