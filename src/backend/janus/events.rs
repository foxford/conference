use serde_derive::Deserialize;

use super::{responses::EventResponse, OpaqueId};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "janus")]
pub enum IncomingEvent {
    WebRtcUp(WebRtcUpEvent),
    Media(MediaEvent),
    Timeout(TimeoutEvent),
    HangUp(HangUpEvent),
    SlowLink(SlowLinkEvent),
    Detached(DetachedEvent),
    Event(EventResponse),
}

// Stream started or a viewer started to receive it.
#[derive(Debug, Deserialize)]
pub struct WebRtcUpEvent {
    session_id: i64,
    sender: i64,
    opaque_id: String,
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
    session_id: i64,
    sender: i64,
    opaque_id: String,
    reason: String,
}

impl OpaqueId for HangUpEvent {
    fn opaque_id(&self) -> &str {
        &self.opaque_id
    }
}

// Audio or video bytes being received by a plugin handle.
#[derive(Debug, Deserialize)]
pub struct MediaEvent {
    session_id: i64,
    sender: i64,
    opaque_id: String,
    #[serde(rename = "type")]
    kind: String,
    receiving: bool,
}

// A session was torn down by the server because of timeout: 60 seconds (by default).
#[derive(Debug, Deserialize)]
pub struct TimeoutEvent {
    session_id: i64,
}

// Janus reporting problems sending media to a user
// (user sent many NACKs in the last second; uplink=true is from Janus' perspective).
#[derive(Debug, Deserialize)]
pub struct SlowLinkEvent {
    session_id: i64,
    sender: i64,
    opaque_id: String,
    uplink: bool,
}

// Janus handle detached.
// This is being sent in case of abnormal shutdown or after `HangUpEvent` in Chrome.
#[derive(Debug, Deserialize)]
pub struct DetachedEvent {
    session_id: i64,
    sender: i64,
    opaque_id: String,
}

impl OpaqueId for DetachedEvent {
    fn opaque_id(&self) -> &str {
        &self.opaque_id
    }
}

// Janus Gateway online/offline status.
#[derive(Debug, Deserialize)]
pub struct StatusEvent {
    online: bool,
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
}

impl StatusEvent {
    pub(crate) fn online(&self) -> bool {
        self.online
    }

    pub(crate) fn capacity(&self) -> Option<i32> {
        self.capacity
    }

    pub(crate) fn balancer_capacity(&self) -> Option<i32> {
        self.balancer_capacity
    }
}
