use failure::{err_msg, format_err, Error};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{Address, IntoPublishableDump};

use crate::app::API_VERSION;
use agent::TestAgent;

pub(crate) const AUDIENCE: &str = "dev.svc.example.org";

#[derive(Deserialize)]
pub(crate) struct MessageProperties {
    #[serde(rename = "type")]
    kind: String,
    method: Option<String>,
}

impl MessageProperties {
    pub(crate) fn kind(&self) -> &str {
        &self.kind
    }

    pub(crate) fn method(&self) -> Option<&str> {
        self.method.as_ref().map(String::as_ref)
    }
}

pub(crate) struct Message<T: DeserializeOwned> {
    topic: String,
    payload: T,
    properties: MessageProperties,
}

impl<T: DeserializeOwned> Message<T> {
    pub(crate) fn from_publishable(message: Box<dyn IntoPublishableDump>) -> Result<Self, Error> {
        let service_agent = TestAgent::new("test", "conference", AUDIENCE);
        let api = Address::new(service_agent.agent_id().to_owned(), API_VERSION);

        let dump = message
            .into_dump(&api)
            .map_err(|err| format_err!("Failed to dump message: {}", err))?;

        let envelope_value = serde_json::from_str::<JsonValue>(dump.payload())?;

        let payload = envelope_value
            .get("payload")
            .ok_or_else(|| err_msg("Missing payload"))
            .and_then(|value| {
                value
                    .as_str()
                    .ok_or_else(|| err_msg("Failed to cast 'payload' field to string"))
            })
            .and_then(|payload_str| {
                serde_json::from_slice::<T>(payload_str.as_bytes())
                    .map_err(|err| format_err!("Failed to parse payload: {}", err))
            })?;

        let properties = envelope_value
            .get("properties")
            .ok_or_else(|| err_msg("Missing properties"))
            .and_then(|value| {
                serde_json::from_value::<MessageProperties>(value.to_owned())
                    .map_err(|err| format_err!("Failed to parse properties: {}", err))
            })?;

        Ok(Self::new(dump.topic(), payload, properties))
    }

    fn new(topic: &str, payload: T, properties: MessageProperties) -> Self {
        Self {
            topic: topic.to_owned(),
            payload,
            properties,
        }
    }

    pub(crate) fn properties(&self) -> &MessageProperties {
        &self.properties
    }

    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) fn payload(&self) -> &T {
        &self.payload
    }
}

pub(crate) mod agent;
pub(crate) mod authz;
pub(crate) mod db;
pub(crate) mod factory;
