use super::MessageProperties;
use failure::Error;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub(crate) struct LocalEnvelope {
    payload: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<MessageProperties>,
}

impl LocalEnvelope {
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
) -> Result<LocalEnvelope, Error>
where
    T: serde::Serialize,
{
    let payload_s = serde_json::to_string(payload)?;
    let envelope = LocalEnvelope::new(&payload_s, properties);
    Ok(envelope)
}

#[derive(Debug, Deserialize)]
pub(crate) struct Envelope {
    payload: String,
    properties: MessageProperties,
}

impl Envelope {
    pub(crate) fn properties(&self) -> &MessageProperties {
        &self.properties
    }

    pub(crate) fn payload<T>(&self) -> Result<T, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = serde_json::from_str::<T>(&self.payload)?;
        Ok(payload)
    }
}

pub(crate) fn from_envelope<T>(envelope: Envelope) -> Result<(T, MessageProperties), Error>
where
    T: serde::de::DeserializeOwned,
{
    Ok((envelope.payload::<T>()?, envelope.properties))
}
