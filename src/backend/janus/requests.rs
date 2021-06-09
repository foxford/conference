use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::AgentId;
use uuid::Uuid;

use super::STREAM_UPLOAD_METHOD;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct CreateSessionRequest {
    transaction: String,
    janus: &'static str,
}

impl CreateSessionRequest {
    pub(crate) fn new(transaction: &str) -> Self {
        Self {
            transaction: transaction.to_owned(),
            janus: "create",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct CreateHandleRequest {
    transaction: String,
    session_id: i64,
    plugin: String,
    janus: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    opaque_id: Option<String>,
}

impl CreateHandleRequest {
    pub(crate) fn new(
        transaction: &str,
        session_id: i64,
        plugin: &str,
        opaque_id: Option<&str>,
    ) -> Self {
        Self {
            transaction: transaction.to_owned(),
            session_id,
            plugin: plugin.to_owned(),
            janus: "attach",
            opaque_id: opaque_id.map(|val| val.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub struct MessageRequest {
    transaction: String,
    janus: &'static str,
    session_id: i64,
    handle_id: i64,
    body: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    jsep: Option<JsonValue>,
}

impl MessageRequest {
    pub(crate) fn new(
        transaction: &str,
        session_id: i64,
        handle_id: i64,
        body: JsonValue,
        jsep: Option<JsonValue>,
    ) -> Self {
        Self {
            transaction: transaction.to_owned(),
            janus: "message",
            session_id,
            handle_id,
            body,
            jsep,
        }
    }

    /// Get a reference to the message request's session id.
    pub(crate) fn session_id(&self) -> i64 {
        self.session_id
    }

    /// Get a reference to the message request's handle id.
    pub(crate) fn handle_id(&self) -> i64 {
        self.handle_id
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateStreamRequestBody {
    method: &'static str,
    id: Uuid,
    agent_id: AgentId,
}

impl CreateStreamRequestBody {
    pub(crate) fn new(id: Uuid, agent_id: AgentId) -> Self {
        Self {
            method: "stream.create",
            id,
            agent_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ReadStreamRequestBody {
    method: &'static str,
    id: Uuid,
    agent_id: AgentId,
}

impl ReadStreamRequestBody {
    pub(crate) fn new(id: Uuid, agent_id: AgentId) -> Self {
        Self {
            method: "stream.read",
            id,
            agent_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UploadStreamRequestBody {
    method: &'static str,
    id: Uuid,
    backend: String,
    bucket: String,
    object: String,
}

impl UploadStreamRequestBody {
    pub(crate) fn new(id: Uuid, backend: &str, bucket: &str, object: &str) -> Self {
        Self {
            method: STREAM_UPLOAD_METHOD,
            id,
            backend: backend.to_owned(),
            bucket: bucket.to_owned(),
            object: object.to_owned(),
        }
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct TrickleRequest {
    transaction: String,
    janus: &'static str,
    session_id: i64,
    handle_id: i64,
    candidate: JsonValue,
}

impl TrickleRequest {
    pub(crate) fn new(
        transaction: &str,
        session_id: i64,
        handle_id: i64,
        candidate: JsonValue,
    ) -> Self {
        Self {
            transaction: transaction.to_owned(),
            janus: "trickle",
            session_id,
            handle_id,
            candidate,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct AgentLeaveRequestBody {
    method: &'static str,
    agent_id: AgentId,
}

impl AgentLeaveRequestBody {
    pub(crate) fn new(agent_id: AgentId) -> Self {
        Self {
            method: "agent.leave",
            agent_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UpdateReaderConfigRequestBody {
    method: String,
    configs: Vec<UpdateReaderConfigRequestBodyConfigItem>,
}

#[cfg(test)]
impl UpdateReaderConfigRequestBody {
    pub(crate) fn method(&self) -> &str {
        &self.method
    }

    pub(crate) fn configs(&self) -> &[UpdateReaderConfigRequestBodyConfigItem] {
        &self.configs
    }
}

impl UpdateReaderConfigRequestBody {
    pub(crate) fn new(configs: Vec<UpdateReaderConfigRequestBodyConfigItem>) -> Self {
        Self {
            method: String::from("reader_config.update"),
            configs,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UpdateReaderConfigRequestBodyConfigItem {
    reader_id: AgentId,
    stream_id: Uuid,
    receive_video: bool,
    receive_audio: bool,
}

impl UpdateReaderConfigRequestBodyConfigItem {
    pub(crate) fn new(
        reader_id: AgentId,
        stream_id: Uuid,
        receive_video: bool,
        receive_audio: bool,
    ) -> Self {
        Self {
            reader_id,
            stream_id,
            receive_video,
            receive_audio,
        }
    }

    #[cfg(test)]
    pub(crate) fn reader_id(&self) -> &AgentId {
        &self.reader_id
    }

    #[cfg(test)]
    pub(crate) fn stream_id(&self) -> Uuid {
        self.stream_id
    }

    #[cfg(test)]
    pub(crate) fn receive_video(&self) -> bool {
        self.receive_video
    }

    #[cfg(test)]
    pub(crate) fn receive_audio(&self) -> bool {
        self.receive_audio
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UpdateWriterConfigRequestBody {
    method: String,
    configs: Vec<UpdateWriterConfigRequestBodyConfigItem>,
}

impl UpdateWriterConfigRequestBody {
    pub(crate) fn new(configs: Vec<UpdateWriterConfigRequestBodyConfigItem>) -> Self {
        Self {
            method: String::from("writer_config.update"),
            configs,
        }
    }
}

#[cfg(test)]
impl UpdateWriterConfigRequestBody {
    pub(crate) fn method(&self) -> &str {
        &self.method
    }

    pub(crate) fn configs(&self) -> &[UpdateWriterConfigRequestBodyConfigItem] {
        &self.configs
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UpdateWriterConfigRequestBodyConfigItem {
    stream_id: Uuid,
    send_video: bool,
    send_audio: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    video_remb: Option<u32>,
}

impl UpdateWriterConfigRequestBodyConfigItem {
    pub(crate) fn new(stream_id: Uuid, send_video: bool, send_audio: bool) -> Self {
        Self {
            stream_id,
            send_video,
            send_audio,
            video_remb: None,
        }
    }

    pub(crate) fn set_video_remb(&mut self, video_remb: u32) -> &mut Self {
        self.video_remb = Some(video_remb);
        self
    }

    #[cfg(test)]
    pub(crate) fn stream_id(&self) -> Uuid {
        self.stream_id
    }

    #[cfg(test)]
    pub(crate) fn send_video(&self) -> bool {
        self.send_video
    }

    #[cfg(test)]
    pub(crate) fn send_audio(&self) -> bool {
        self.send_audio
    }

    #[cfg(test)]
    pub(crate) fn video_remb(&self) -> Option<u32> {
        self.video_remb
    }
}
