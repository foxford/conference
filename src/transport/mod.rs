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
                "invalid value for the application group: {}",
                val
            )),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) enum Destination {
    // -> event(app-to-any): apps/ACCOUNT_ID(ME)/api/v1/BROADCAST_URI
    Broadcast(String),
    // -> request(one-to-app): agents/AGENT_ID(ME)/api/v1/out/ACCOUNT_ID
    Multicast(AccountId),
    // -> request(one-to-one): agents/AGENT_ID/api/v1/in/ACCOUNT_ID(ME)
    // -> response(one-to-one): agents/AGENT_ID/api/v1/in/ACCOUNT_ID(ME)
    Unicast(AgentId),
}

#[derive(Debug)]
pub(crate) enum Source<'a> {
    // <- event(any-from-app): apps/ACCOUNT_ID/api/v1/BROADCAST_URI
    Broadcast(&'a AccountId, &'a str),
    // <- request(app-from-any): agents/+/api/v1/out/ACCOUNT_ID(ME)
    Multicast,
    // <- request(one-from-one): agents/AGENT_ID(ME)/api/v1/in/ACCOUNT_ID
    // <- request(one-from-any): agents/AGENT_ID(ME)/api/v1/in/+
    // <- response(one-from-one): agents/AGENT_ID(ME)/api/v1/in/ACCOUNT_ID
    // <- response(one-from-any): agents/AGENT_ID(ME)/api/v1/in/+
    Unicast(Option<&'a AccountId>),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize)]
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
