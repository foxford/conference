use chrono::Utc;
use failure::{format_err, Error};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde_json::{json, Value as JsonValue};
use svc_agent::mqtt::{compat, compat::IncomingEnvelope, IncomingEvent, IncomingRequest};
use svc_agent::{AccountId, AgentId};

const CORRELATION_DATA_LENGTH: usize = 16;

pub(crate) struct TestAgent {
    agent_id: AgentId,
    account_id: AccountId,
}

impl TestAgent {
    pub(crate) fn new(agent_label: &str, account_label: &str, audience: &str) -> Self {
        let account_id = AccountId::new(account_label, audience);
        let agent_id = AgentId::new(agent_label, account_id.clone());
        Self {
            agent_id,
            account_id,
        }
    }

    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub(crate) fn build_request<T>(
        &self,
        method: &str,
        payload: &JsonValue,
    ) -> Result<IncomingRequest<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let correlation_data: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(CORRELATION_DATA_LENGTH)
            .collect();

        let conference_account_id = AccountId::new("svc", self.account_id.audience());
        let conference_agent_id = AgentId::new("conference", conference_account_id);
        let response_topic = format!("agents/{}/api/v1/in/{}", self.agent_id, conference_agent_id);
        let now = Utc::now().timestamp_millis().to_string();

        let message = json!({
            "payload": serde_json::to_string(payload)?,
            "properties": {
                "type": "request",
                "correlation_data": correlation_data,
                "method": method,
                "agent_label": self.agent_id.label(),
                "account_label": self.account_id.label(),
                "audience": self.account_id.audience(),
                "connection_mode": "agents",
                "connection_version": "v1",
                "response_topic": response_topic,
                "broker_agent_label": "alpha",
                "broker_account_label": "mqtt-gateway",
                "broker_audience": self.account_id.audience(),
                "broker_timestamp": now,
                "broker_processing_timestamp": now,
                "broker_initial_processing_timestamp": now,
                "tracking_id": "16911d40-0b13-11ea-8171-60f81db6d53e",
                "session_tracking_label": "16cc4294-0b13-11ea-91ae-60f81db6d53e.16ee876e-0b13-11ea-8c32-60f81db6d53e 2565f962-0b13-11ea-9359-60f81db6d53e.25c2b97c-0b13-11ea-9f20-60f81db6d53e",
            }
        });

        let message_str = message.to_string();
        let envelope = serde_json::from_slice::<IncomingEnvelope>(message_str.as_bytes())?;

        compat::into_request(envelope)
            .map_err(|err| format_err!("Failed to build request: {}", err))
    }

    pub(crate) fn build_event<T>(
        &self,
        label: &str,
        payload: &JsonValue,
    ) -> Result<IncomingEvent<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let now = Utc::now().timestamp_millis().to_string();

        let message = json!({
            "payload": serde_json::to_string(payload)?,
            "properties": {
                "type": "event",
                "label": label,
                "agent_label": self.agent_id.label(),
                "account_label": &self.account_id.label(),
                "audience": self.account_id.audience(),
                "connection_mode": "agents",
                "connection_version": "v1",
                "broker_timestamp": now,
                "broker_processing_timestamp": now,
                "broker_initial_processing_timestamp": now,
                "tracking_id": "16911d40-0b13-11ea-8171-60f81db6d53e",
                "session_tracking_label": "16cc4294-0b13-11ea-91ae-60f81db6d53e.16ee876e-0b13-11ea-8c32-60f81db6d53e 2565f962-0b13-11ea-9359-60f81db6d53e.25c2b97c-0b13-11ea-9f20-60f81db6d53e",
            }
        });

        let message_str = message.to_string();
        let envelope = serde_json::from_slice::<IncomingEnvelope>(message_str.as_bytes())?;
        compat::into_event(envelope).map_err(|err| format_err!("Failed to build event: {}", err))
    }
}
