use failure::{err_msg, Error};
use serde_derive::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub(crate) enum MessageProperties {
    Event(EventMessageProperties),
    Request(RequestMessageProperties),
    Response(ResponseMessageProperties),
}

impl MessageProperties {
    pub(crate) fn authn(&self) -> &AuthnMessageProperties {
        match self {
            MessageProperties::Event(ref props) => &props.authn,
            MessageProperties::Request(ref props) => &props.authn,
            MessageProperties::Response(ref props) => &props.authn,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct EventMessageProperties {
    #[serde(flatten)]
    authn: AuthnMessageProperties,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct RequestMessageProperties {
    method: String,
    response_topic: String,
    correlation_data: String,
    #[serde(flatten)]
    authn: AuthnMessageProperties,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ResponseMessageProperties {
    correlation_data: String,
    #[serde(flatten)]
    authn: AuthnMessageProperties,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct AuthnMessageProperties {
    agent_label: String,
    account_id: Uuid,
    audience: String,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct AgentId {
    label: String,
    account_id: Uuid,
    audience: String,
}

impl AgentId {
    pub(crate) fn new(label: &str, account_id: Uuid, audience: &str) -> Self {
        Self {
            label: label.to_owned(),
            account_id: account_id,
            audience: audience.to_owned(),
        }
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}.{}", self.label, self.account_id, self.audience)
    }
}

impl FromStr for AgentId {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(3, '.').collect();
        match parts[..] {
            [ref label, ref account_id, ref audience] => {
                Ok(Self::new(label, Uuid::parse_str(account_id)?, audience))
            }
            _ => Err(err_msg(format!("Invalid value for the agent id: {}", val))),
        }
    }
}

impl From<&MessageProperties> for AgentId {
    fn from(props: &MessageProperties) -> Self {
        AgentId::from(props.authn())
    }
}

impl From<&AuthnMessageProperties> for AgentId {
    fn from(authn: &AuthnMessageProperties) -> Self {
        AgentId::new(&authn.agent_label, authn.account_id, &authn.audience)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ApplicationIdentity {
    label: String,
    audience: String,
    account_id: Uuid,
}

impl ApplicationIdentity {
    pub(crate) fn name(&self) -> ApplicationName {
        ApplicationName::new(&self.label, &self.audience)
    }

    pub(crate) fn group(&self, label: &str) -> ApplicationGroup {
        ApplicationGroup::new(label, self.name())
    }

    pub(crate) fn agent_id(&self, label: &str) -> AgentId {
        AgentId::new(label, self.account_id, &self.audience)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ApplicationName {
    label: String,
    audience: String,
}

impl ApplicationName {
    pub(crate) fn new(label: &str, audience: &str) -> Self {
        Self {
            label: label.to_owned(),
            audience: audience.to_owned(),
        }
    }
}

impl fmt::Display for ApplicationName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.label, self.audience)
    }
}

impl FromStr for ApplicationName {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(2, '.').collect();
        match parts[..] {
            [ref label, ref audience] => Ok(Self::new(label, audience)),
            _ => Err(err_msg(format!(
                "Invalid value for the application name: {}",
                val
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ApplicationGroup {
    label: String,
    app_name: ApplicationName,
}

impl ApplicationGroup {
    pub(crate) fn new(label: &str, app_name: ApplicationName) -> Self {
        Self {
            label: label.to_owned(),
            app_name,
        }
    }
}

impl fmt::Display for ApplicationGroup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.label, self.app_name)
    }
}

impl FromStr for ApplicationGroup {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(2, '.').collect();
        match parts[..] {
            [ref label, ref rest] => Ok(Self::new(label, rest.parse::<ApplicationName>()?)),
            _ => Err(err_msg(format!(
                "Invalid value for the application group: {}",
                val
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) mod compat;
pub(crate) mod correlation_data;
