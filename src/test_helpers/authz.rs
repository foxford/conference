use svc_agent::AccountId;
use svc_authz::{
    Authenticable, ClientMap, Config, ConfigMap, LocalWhitelistConfig, LocalWhitelistRecord,
};

use crate::{authz::AuthzObject, test_helpers::USR_AUDIENCE};

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct TestAuthz {
    records: Vec<LocalWhitelistRecord>,
    audience: String,
}

impl TestAuthz {
    pub fn new() -> Self {
        Self {
            records: vec![],
            audience: USR_AUDIENCE.to_owned(),
        }
    }

    pub fn set_audience(&mut self, audience: &str) -> &mut Self {
        self.audience = audience.to_owned();
        self
    }

    pub fn allow<A: Authenticable>(&mut self, subject: &A, object: Vec<&str>, action: &str) {
        let record = LocalWhitelistRecord::new(subject, AuthzObject::new(&object).into(), action);
        self.records.push(record);
    }
}

impl From<TestAuthz> for ClientMap {
    fn from(val: TestAuthz) -> Self {
        let config = LocalWhitelistConfig::new(val.records);

        let mut config_map = ConfigMap::new();
        config_map.insert(val.audience.to_owned(), Config::LocalWhitelist(config));

        let account_id = AccountId::new("conference", &val.audience);
        ClientMap::new(&account_id, None, config_map, None).expect("Failed to build authz")
    }
}
