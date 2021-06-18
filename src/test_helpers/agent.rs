use svc_agent::mqtt::Address;
use svc_agent::{AccountId, AgentId, Authenticable};

use crate::app::API_VERSION;

pub struct TestAgent {
    address: Address,
}

impl TestAgent {
    pub fn new(agent_label: &str, account_label: &str, audience: &str) -> Self {
        let account_id = AccountId::new(account_label, audience);
        let agent_id = AgentId::new(agent_label, account_id.clone());
        let address = Address::new(agent_id, API_VERSION);
        Self { address }
    }

    pub fn address(&self) -> &Address {
        &self.address
    }

    pub fn agent_id(&self) -> &AgentId {
        &self.address.id()
    }

    pub fn account_id(&self) -> &AccountId {
        &self.address.id().as_account_id()
    }
}
