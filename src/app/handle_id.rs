use std::fmt;
use std::str::FromStr;

use svc_agent::AgentId;

#[derive(Debug, Clone)]
pub(crate) struct HandleId {
    janus_handle_id: i64,
    janus_session_id: i64,
    backend_id: AgentId,
}

impl HandleId {
    pub(crate) fn janus_handle_id(&self) -> i64 {
        self.janus_handle_id
    }

    pub(crate) fn janus_session_id(&self) -> i64 {
        self.janus_session_id
    }

    pub(crate) fn backend_id(&self) -> &AgentId {
        &self.backend_id
    }
}

impl HandleId {
    pub(crate) fn new(janus_handle_id: i64, janus_session_id: i64, backend_id: AgentId) -> Self {
        Self {
            janus_handle_id,
            janus_session_id,
            backend_id,
        }
    }
}

impl fmt::Display for HandleId {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}.{}.{}",
            self.janus_handle_id, self.janus_session_id, self.backend_id
        )
    }
}

impl FromStr for HandleId {
    type Err = anyhow::Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(3, '.').collect();
        match parts[..] {
            [ref janus_handle_id, ref janus_session_id, ref rest] => Ok(Self::new(
                janus_handle_id.parse::<i64>()?,
                janus_session_id.parse::<i64>()?,
                rest.parse::<AgentId>()?,
            )),
            _ => Err(anyhow!("Invalid handle id: {}", val)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

mod serde {
    use serde::{de, ser};
    use std::fmt;

    use super::HandleId;

    impl ser::Serialize for HandleId {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: ser::Serializer,
        {
            serializer.serialize_str(&self.to_string())
        }
    }

    impl<'de> de::Deserialize<'de> for HandleId {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            struct AgentIdVisitor;

            impl<'de> de::Visitor<'de> for AgentIdVisitor {
                type Value = HandleId;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("struct HandleId")
                }

                fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    use std::str::FromStr;

                    HandleId::from_str(v)
                        .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(v), &self))
                }
            }

            deserializer.deserialize_str(AgentIdVisitor)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::prelude::*;

    #[test]
    fn dump_and_parse_handle_id() {
        let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
        let handle_id = HandleId::new(1, 2, agent.agent_id().to_owned());
        let dump = serde_json::to_string(&handle_id).expect("Failed to dump handle_id");
        let parsed = serde_json::from_str::<HandleId>(&dump).expect("Failed to parse handle id");
        assert_eq!(parsed.janus_handle_id(), 1);
        assert_eq!(parsed.janus_session_id(), 2);
        assert_eq!(parsed.backend_id(), agent.agent_id());
    }
}
