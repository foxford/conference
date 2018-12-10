use crate::transport;
use failure::{err_msg, Error};
use serde_derive::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct SessionCreateRequest {
    transaction: Transaction,
    janus: String,
}

impl SessionCreateRequest {
    pub(crate) fn new(transaction: Transaction) -> Self {
        Self {
            transaction: transaction,
            janus: "create".to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "janus")]
pub(crate) enum Response {
    Success(SuccessResponse),
    Timeout(SessionTimeoutNotification),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct SuccessResponse {
    transaction: Transaction,
    data: SessionResponseData,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionResponseData {
    id: u64,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct SessionTimeoutNotification {
    session_id: u64,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct Transaction {
    id: String,
    agent_id: transport::AgentId,
}

impl Transaction {
    pub(crate) fn new(id: &str, agent_id: transport::AgentId) -> Self {
        Self {
            id: id.to_owned(),
            agent_id: agent_id,
        }
    }
}

impl fmt::Display for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.id, self.agent_id)
    }
}

impl FromStr for Transaction {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(2, '.').collect();
        match parts[..] {
            [id, rest] => Ok(Self::new(&id, rest.parse::<transport::AgentId>()?)),
            _ => Err(err_msg(format!("Invalid value for transaction: {}", val))),
        }
    }
}

impl serde::Serialize for Transaction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct TransactionVisitor;

impl<'de> serde::de::Visitor<'de> for TransactionVisitor {
    type Value = Transaction;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a FQDN compatible value")
    }

    fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        val.parse::<Transaction>()
            .map_err(|_| E::invalid_value(serde::de::Unexpected::Str(val), &self))
    }
}

impl<'de> serde::Deserialize<'de> for Transaction {
    fn deserialize<D>(deserializer: D) -> Result<Transaction, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(TransactionVisitor)
    }
}
