use serde::{de, Deserialize};
use serde_json::Value;

use crate::{backend::janus::OpaqueId, util::from_base64};

use super::{transactions::Transaction, Sender, SessionId};

fn deserialize_json_string<'de, D>(deserializer: D) -> Result<Transaction, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: &str = de::Deserialize::deserialize(deserializer)?;
    from_base64(s).map_err(de::Error::custom)
}

// A response on a request sent to a plugin handle.
#[derive(Debug, Deserialize)]
pub struct EventResponse {
    #[serde(deserialize_with = "deserialize_json_string")]
    pub transaction: Transaction,
    pub session_id: SessionId,
    pub plugindata: EventResponsePluginData,
    pub jsep: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct EventResponsePluginData {
    pub data: Option<Value>,
    pub plugin: String,
}

// Stream started or a viewer started to receive it.
#[derive(Debug, Deserialize)]
pub struct WebRtcUpEvent {
    pub session_id: SessionId,
    pub sender: Sender,
    pub opaque_id: String,
}

impl OpaqueId for WebRtcUpEvent {
    fn opaque_id(&self) -> &str {
        &self.opaque_id
    }
}

// A RTCPeerConnection closed for a DTLS alert (normal shutdown).
// With Firefox it's not being sent. There's only `DetachedEvent`.
#[derive(Debug, Deserialize)]
pub struct HangUpEvent {
    pub session_id: SessionId,
    pub sender: Sender,
    pub opaque_id: String,
    pub reason: String,
}

impl OpaqueId for HangUpEvent {
    fn opaque_id(&self) -> &str {
        &self.opaque_id
    }
}

// Audio or video bytes being received by a plugin handle.
#[derive(Debug, Deserialize)]
pub struct MediaEvent {
    pub session_id: SessionId,
    pub sender: Sender,
    pub opaque_id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub receiving: bool,
}

// A session was torn down by the server because of timeout: 60 seconds (by default).
#[derive(Debug, Deserialize)]
pub struct TimeoutEvent {
    pub session_id: SessionId,
}

// Janus reporting problems sending media to a user
// (user sent many NACKs in the last second; uplink=true is from Janus' perspective).
#[derive(Debug, Deserialize)]
pub struct SlowLinkEvent {
    pub session_id: SessionId,
    pub sender: Sender,
    pub opaque_id: String,
    pub uplink: bool,
}

// Janus handle detached.
// This is being sent in case of abnormal shutdown or after `HangUpEvent` in Chrome.
#[derive(Debug, Deserialize)]
pub struct DetachedEvent {
    pub session_id: SessionId,
    pub sender: Sender,
    pub opaque_id: String,
}

impl OpaqueId for DetachedEvent {
    fn opaque_id(&self) -> &str {
        &self.opaque_id
    }
}
