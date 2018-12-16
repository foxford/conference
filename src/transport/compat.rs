use super::MessageProperties;
use failure::Error;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Envelope {
    payload: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<MessageProperties>,
}

impl Envelope {
    pub(crate) fn new(payload: &str, properties: Option<MessageProperties>) -> Self {
        Self {
            payload: payload.to_owned(),
            properties: properties,
        }
    }
}

pub(crate) fn to_envelope<T>(
    payload: &T,
    properties: Option<MessageProperties>,
) -> Result<Envelope, Error>
where
    T: serde::Serialize,
{
    let payload_s = serde_json::to_string(payload)?;
    let envelope = Envelope::new(&payload_s, properties);
    Ok(envelope)
}

pub(crate) fn from_envelope<T>(envelope: Envelope) -> Result<(T, MessageProperties), Error>
where
    T: serde::de::DeserializeOwned,
{
    let payload = serde_json::from_str::<T>(&envelope.payload)?;
    let properties = envelope
        .properties
        .expect("An incoming message envelope must contain properties.");
    Ok((payload, properties))
}
