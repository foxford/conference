use std::{fmt, str::FromStr};

use anyhow::anyhow;
use svc_agent::AgentId;

use crate::db;

#[derive(Debug, Clone)]
pub struct HandleId {
    rtc_stream_id: db::janus_rtc_stream::Id,
    rtc_id: db::rtc::Id,
    janus_handle_id: crate::backend::janus::client::HandleId,
    backend_id: AgentId,
}

impl HandleId {
    pub fn rtc_stream_id(&self) -> db::janus_rtc_stream::Id {
        self.rtc_stream_id
    }

    pub fn rtc_id(&self) -> db::rtc::Id {
        self.rtc_id
    }

    pub fn janus_handle_id(&self) -> crate::backend::janus::client::HandleId {
        self.janus_handle_id
    }

    pub fn backend_id(&self) -> &AgentId {
        &self.backend_id
    }
}

impl HandleId {
    pub fn new(
        rtc_stream_id: db::janus_rtc_stream::Id,
        rtc_id: db::rtc::Id,
        janus_handle_id: crate::backend::janus::client::HandleId,
        backend_id: AgentId,
    ) -> Self {
        Self {
            rtc_stream_id,
            rtc_id,
            janus_handle_id,
            backend_id,
        }
    }
}

impl fmt::Display for HandleId {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}.{}.{}.{}",
            self.rtc_stream_id, self.rtc_id, self.janus_handle_id, self.backend_id
        )
    }
}

impl FromStr for HandleId {
    type Err = anyhow::Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(5, '.').collect();
        match parts[..] {
            [rtc_stream_id, rtc_id, janus_handle_id, _janus_session_id, rest] => Ok(Self::new(
                rtc_stream_id.parse()?,
                rtc_id.parse()?,
                janus_handle_id.parse()?,
                rest.parse()?,
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
