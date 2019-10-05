use svc_agent::AccountId;
use svc_authz::{
    Authenticable, ClientMap, Config, ConfigMap, LocalWhitelistConfig, LocalWhitelistRecord,
    NoneConfig,
};

pub(crate) struct TestAuthz {
    audience: String,
    records: Vec<LocalWhitelistRecord>,
}

impl TestAuthz {
    pub(crate) fn new(audience: &str) -> Self {
        Self {
            audience: audience.to_string(),
            records: vec![],
        }
    }

    pub(crate) fn allow<A: Authenticable>(&mut self, subject: &A, object: Vec<&str>, action: &str) {
        let record = LocalWhitelistRecord::new(subject, object, action);
        self.records.push(record);
    }
}

impl Into<ClientMap> for TestAuthz {
    fn into(self) -> ClientMap {
        let config = LocalWhitelistConfig::new(self.records);

        let mut config_map = ConfigMap::new();
        config_map.insert(self.audience.to_owned(), Config::LocalWhitelist(config));

        let account_id = AccountId::new("conference", &self.audience);
        ClientMap::new(&account_id, config_map).expect("Failed to build authz")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn no_authz(audience: &str) -> ClientMap {
    let authz_none_config = svc_authz::Config::None(NoneConfig {});

    let mut authz_config_map = ConfigMap::new();
    authz_config_map.insert(audience.to_owned(), authz_none_config);

    let account_id = AccountId::new("conference", audience);
    svc_authz::ClientMap::new(&account_id, authz_config_map).expect("Failed to build authz")
}
