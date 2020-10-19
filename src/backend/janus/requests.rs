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
pub(crate) struct MessageRequest {
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
