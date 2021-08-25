use serde::Deserialize;
use serde_json::Value;

use super::{create_handle::OpaqueId, transactions::Transaction, HandleId, SessionId};

// A response on a request sent to a plugin handle.
#[derive(Debug, Deserialize)]
pub struct EventResponse {
    #[serde(with = "super::serialize_as_base64")]
    pub transaction: Transaction,
    pub session_id: SessionId,
    #[serde(with = "super::serialize_as_base64")]
    pub opaque_id: Option<OpaqueId>,
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
    pub sender: HandleId,
    #[serde(with = "super::serialize_as_base64")]
    pub opaque_id: OpaqueId,
}

// A RTCPeerConnection closed for a DTLS alert (normal shutdown).
// With Firefox it's not being sent. There's only `DetachedEvent`.
#[derive(Debug, Deserialize)]
pub struct HangUpEvent {
    pub session_id: SessionId,
    pub sender: HandleId,
    #[serde(with = "super::serialize_as_base64")]
    pub opaque_id: OpaqueId,
    pub reason: String,
}

// Audio or video bytes being received by a plugin handle.
#[derive(Debug, Deserialize)]
pub struct MediaEvent {
    pub session_id: SessionId,
    pub sender: HandleId,
    #[serde(with = "super::serialize_as_base64")]
    pub opaque_id: OpaqueId,
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
    pub sender: HandleId,
    pub opaque_id: OpaqueId,
    pub uplink: bool,
}

// Janus handle detached.
// This is being sent in case of abnormal shutdown or after `HangUpEvent` in Chrome.
#[derive(Debug, Deserialize)]
pub struct DetachedEvent {
    pub session_id: SessionId,
    pub sender: HandleId,
    #[serde(with = "super::serialize_as_base64")]
    pub opaque_id: OpaqueId,
}
