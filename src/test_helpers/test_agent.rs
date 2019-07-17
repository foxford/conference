use failure::{format_err, Error};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use serde_json::{json, Value as JsonValue};

use svc_agent::{AgentId, AccountId};
use svc_agent::mqtt::{compat, compat::IncomingEnvelope, IncomingRequest};

const CORRELATION_DATA_LENGTH: usize = 16;

pub(crate) struct TestAgent {
    agent_id: AgentId,
    account_id: AccountId,
}

impl TestAgent {
    pub(crate) fn new(agent_label: &str, account_label: &str, audience: &str) -> Self {
        let account_id = AccountId::new(account_label, audience);
        let agent_id = AgentId::new(agent_label, account_id.clone());
        Self { agent_id, account_id }
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

        let response_topic =
            format!("agents/{}/api/v1/in/{}", self.agent_id, conference_agent_id);

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
            }
        });

        let message_str = message.to_string();
        let envelope = serde_json::from_slice::<IncomingEnvelope>(message_str.as_bytes())?;

        compat::into_request(envelope)
            .map_err(|err| format_err!("Failed to build request: {}", err))
    }
}
