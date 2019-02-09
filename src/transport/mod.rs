use crate::authn::Authenticable;
use failure::{format_err, Error};
use serde_derive::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait Addressable: Authenticable {
    fn agent_label(&self) -> &str;
}
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize, FromSqlRow, AsExpression)]
#[sql_type = "sql::Account_id"]
pub(crate) struct AccountId {
    label: String,
    audience: String,
}

impl AccountId {
    pub(crate) fn new(label: &str, audience: &str) -> Self {
        Self {
            label: label.to_owned(),
            audience: audience.to_owned(),
        }
    }

    pub(crate) fn label(&self) -> &str {
        &self.label
    }

    pub(crate) fn audience(&self) -> &str {
        &self.audience
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.label, self.audience)
    }
}

impl FromStr for AccountId {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(2, '.').collect();
        match parts[..] {
            [ref label, ref audience] => Ok(Self::new(label, audience)),
            _ => Err(format_err!(
                "invalid value for the application name: {}",
                val
            )),
        }
    }
}

impl Authenticable for AccountId {
    fn account_label(&self) -> &str {
        &self.label
    }

    fn audience(&self) -> &str {
        &self.audience
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize, FromSqlRow, AsExpression)]
#[sql_type = "sql::Agent_id"]
pub(crate) struct AgentId {
    label: String,
    account_id: AccountId,
}

impl AgentId {
    pub(crate) fn new(label: &str, account_id: AccountId) -> Self {
        Self {
            label: label.to_owned(),
            account_id,
        }
    }

    pub(crate) fn label(&self) -> &str {
        &self.label
    }

    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.label, self.account_id.label, self.account_id.audience,
        )
    }
}

impl FromStr for AgentId {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(3, '.').collect();
        match parts[..] {
            [ref agent_label, ref account_label, ref audience] => {
                let account_id = AccountId::new(account_label, audience);
                let agent_id = Self::new(agent_label, account_id);
                Ok(agent_id)
            }
            _ => Err(format_err!("invalid value for the agent id: {}", val)),
        }
    }
}

impl Authenticable for AgentId {
    fn account_label(&self) -> &str {
        &self.account_id.label
    }

    fn audience(&self) -> &str {
        &self.account_id.audience
    }
}

impl Addressable for AgentId {
    fn agent_label(&self) -> &str {
        &self.label
    }
}

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

////////////////////////////////////////////////////////////////////////////////

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

pub mod sql {

    use super::AccountId;
    use super::AgentId;

    use diesel::deserialize::{self, FromSql};
    use diesel::pg::Pg;
    use diesel::serialize::{self, Output, ToSql, WriteTuple};
    use diesel::sql_types::{Record, Text};
    use std::io::Write;

    #[derive(SqlType, QueryId)]
    #[postgres(type_name = "account_id")]
    #[allow(non_camel_case_types)]
    pub struct Account_id;

    impl ToSql<Account_id, Pg> for AccountId {
        fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
            WriteTuple::<(Text, Text)>::write_tuple(&(&self.label, &self.audience), out)
        }
    }

    impl FromSql<Account_id, Pg> for AccountId {
        fn from_sql(bytes: Option<&[u8]>) -> deserialize::Result<Self> {
            let (label, audience): (String, String) =
                FromSql::<Record<(Text, Text)>, Pg>::from_sql(bytes)?;
            Ok(AccountId::new(&label, &audience))
        }
    }

    #[derive(SqlType, QueryId)]
    #[postgres(type_name = "agent_id")]
    #[allow(non_camel_case_types)]
    pub struct Agent_id;

    impl ToSql<Agent_id, Pg> for AgentId {
        fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
            WriteTuple::<(Account_id, Text)>::write_tuple(&(&self.account_id, &self.label), out)
        }
    }

    impl FromSql<Agent_id, Pg> for AgentId {
        fn from_sql(bytes: Option<&[u8]>) -> deserialize::Result<Self> {
            let (account_id, label): (AccountId, String) =
                FromSql::<Record<(Account_id, Text)>, Pg>::from_sql(bytes)?;
            Ok(AgentId::new(&label, account_id))
        }
    }

}

////////////////////////////////////////////////////////////////////////////////

pub(crate) mod correlation_data;
pub(crate) mod mqtt;
