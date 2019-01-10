use crate::authn::{AccountId, AgentId};
use failure::{format_err, Error};
use serde_derive::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct SharedGroup {
    label: String,
    account_id: AccountId,
}

impl SharedGroup {
    pub(crate) fn new(label: &str, account_id: AccountId) -> Self {
        Self {
            label: label.to_owned(),
            account_id,
        }
    }
}

impl fmt::Display for SharedGroup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.label, self.account_id)
    }
}

impl FromStr for SharedGroup {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(2, '.').collect();
        match parts[..] {
            [ref label, ref rest] => Ok(Self::new(label, rest.parse::<AccountId>()?)),
            _ => Err(format_err!(
                "Invalid value for the application group: {}",
                val
            )),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) enum Destination {
    Broadcast(BroadcastUri),
    Multicast(AccountId),
    Unicast(AgentId),
}

pub(crate) type BroadcastUri = String;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct AuthnProperties {
    agent_label: String,
    account_label: String,
    audience: String,
}

impl From<&AuthnProperties> for AccountId {
    fn from(authn: &AuthnProperties) -> Self {
        AccountId::new(&authn.account_label, &authn.audience)
    }
}

impl From<&AuthnProperties> for AgentId {
    fn from(authn: &AuthnProperties) -> Self {
        AgentId::new(&authn.agent_label, AccountId::from(authn))
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) mod correlation_data;
pub(crate) mod mqtt;
