use std::fmt;
use std::str::FromStr;

use svc_agent::AgentId;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct HandleId {
    rtc_stream_id: Uuid,
    rtc_id: Uuid,
    janus_handle_id: crate::backend::janus::http::HandleId,
    janus_session_id: crate::backend::janus::http::SessionId,
    backend_id: AgentId,
}

impl HandleId {
    pub(crate) fn rtc_stream_id(&self) -> Uuid {
        self.rtc_stream_id
    }

    pub(crate) fn rtc_id(&self) -> Uuid {
        self.rtc_id
    }

    pub(crate) fn janus_handle_id(&self) -> crate::backend::janus::http::HandleId {
        self.janus_handle_id
    }

    pub(crate) fn janus_session_id(&self) -> crate::backend::janus::http::SessionId {
        self.janus_session_id
    }

    pub(crate) fn backend_id(&self) -> &AgentId {
        &self.backend_id
    }
}

impl HandleId {
    pub(crate) fn new(
        rtc_stream_id: Uuid,
        rtc_id: Uuid,
        janus_handle_id: crate::backend::janus::http::HandleId,
        janus_session_id: crate::backend::janus::http::SessionId,
        backend_id: AgentId,
    ) -> Self {
        Self {
            rtc_stream_id,
            rtc_id,
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
            "{}.{}.{}.{}.{}",
            self.rtc_stream_id,
            self.rtc_id,
            self.janus_handle_id,
            self.janus_session_id,
            self.backend_id
        )
    }
}

impl FromStr for HandleId {
    type Err = anyhow::Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(5, '.').collect();
        match parts[..] {
            [ref rtc_stream_id, ref rtc_id, ref janus_handle_id, ref janus_session_id, ref rest] => {
                Ok(Self::new(
                    Uuid::from_str(rtc_stream_id)?,
                    Uuid::from_str(rtc_id)?,
                    janus_handle_id.parse()?,
                    janus_session_id.parse()?,
                    rest.parse()?,
                ))
            }
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
