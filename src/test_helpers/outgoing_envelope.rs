use serde::{de::DeserializeOwned, Deserialize};
use svc_agent::mqtt::ResponseStatus;

#[derive(Debug, Deserialize)]
pub struct OutgoingEnvelope {
    payload: String,
    properties: OutgoingEnvelopeProperties,
    #[serde(skip)]
    topic: String,
}

impl OutgoingEnvelope {
    pub fn payload<P: DeserializeOwned>(&self) -> P {
        serde_json::from_str::<P>(&self.payload).expect("Failed to parse payload")
    }

    pub fn properties(&self) -> &OutgoingEnvelopeProperties {
        &self.properties
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub(super) fn set_topic(&mut self, topic: &str) -> &mut Self {
        self.topic = topic.to_owned();
        self
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum OutgoingEnvelopeProperties {
    Event(OutgoingEventProperties),
    Response(OutgoingResponseProperties),
    Request(OutgoingRequestProperties),
}

#[derive(Debug, Deserialize)]
pub struct OutgoingEventProperties {
    label: String,
}

impl OutgoingEventProperties {
    pub fn label(&self) -> &str {
        &self.label
    }
}

#[derive(Debug, Deserialize)]
pub struct OutgoingResponseProperties {
    status: String,
    correlation_data: String,
}

impl OutgoingResponseProperties {
    pub fn status(&self) -> ResponseStatus {
        ResponseStatus::from_bytes(self.status.as_bytes()).expect("Invalid status code")
    }

    pub fn correlation_data(&self) -> &str {
        &self.correlation_data
    }
}

#[derive(Debug, Deserialize)]
pub struct OutgoingRequestProperties {
    method: String,
}

impl OutgoingRequestProperties {
    pub fn method(&self) -> &str {
        &self.method
    }
}
