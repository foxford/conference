use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub(crate) const JANUS_API_VERSION: &str = "v1";

////////////////////////////////////////////////////////////////////////////////

// Creating a session

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

// TODO: implement InfoRequest
// Getting generic info from the Janus Gateway instance

//    {
//        "janus" : "detach",
//        "transaction" : "<random string>"
//    }

////////////////////////////////////////////////////////////////////////////////

// Destroying the session

//#[derive(Debug, Serialize)]
//pub(crate) struct DestroySessionRequest {
//    transaction: String,
//    janus: &'static str,
//}
//
//impl DestroySessionRequest {
//    pub(crate) fn new(transaction: &str) -> Self {
//        Self {
//            transaction: transaction.to_owned(),
//            janus: "destroy",
//        }
//    }
//}

////////////////////////////////////////////////////////////////////////////////

// Attaching to a plugin

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

// TODO: implement HandleDetachRequest
// Destroying the plugin handle

//    {
//        "janus" : "detach",
//        "transaction" : "<random string>"
//    }

////////////////////////////////////////////////////////////////////////////////

// TODO: implement HandleHangupRequest
// Hanging up the associated PeerConnection, keeping the plugin handle alive

//    {
//        "janus" : "hangup",
//        "transaction" : "<random string>"
//    }

////////////////////////////////////////////////////////////////////////////////

// Sending a message to a plugin

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

// TODO: implement KeepaliveRequest
// A Janus session is kept alive as long as there's no inactivity
// for 60 seconds (dy default): if no messages have been received in that time frame,
// the session is torn down by the server.

//    {
//        "janus" : "keepalive",
//        "session_id" : <the session identifier>,
//        "transaction" : "sBJNyUhH6Vc6"
//    }

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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "janus")]
pub(crate) enum IncomingResponse {
    Error(ErrorResponse),
    Ack(AckResponse),
    Event(EventResponse),
    Success(SuccessResponse),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "janus")]
pub(crate) enum IncomingEvent {
    WebRtcUp(WebRtcUpEvent),
    Media(MediaEvent),
    Timeout(TimeoutEvent),
    HangUp(HangUpEvent),
    SlowLink(SlowLinkEvent),
    Detached(DetachedEvent),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum ErrorResponse {
    Handle(HandleErrorResponse),
    Session(SessionErrorResponse),
}

#[derive(Debug, Deserialize)]
pub(crate) struct HandleErrorResponse {
    transaction: String,
    session_id: i64,
    error: ErrorResponseData,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionErrorResponse {
    transaction: String,
    error: ErrorResponseData,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ErrorResponseData {
    code: i32,
    reason: String,
}

////////////////////////////////////////////////////////////////////////////////

// A request to a plugin handler was received

#[derive(Debug, Deserialize)]
pub(crate) struct AckResponse {
    transaction: String,
    session_id: i64,
}

impl AckResponse {
    pub(crate) fn transaction(&self) -> &str {
        &self.transaction
    }
}

////////////////////////////////////////////////////////////////////////////////

// A response on a request sent to a plugin handler

#[derive(Debug, Deserialize)]
pub(crate) struct EventResponse {
    transaction: String,
    session_id: i64,
    sender: i64,
    plugindata: EventResponsePluginData,
    jsep: Option<JsonValue>,
}

impl EventResponse {
    pub(crate) fn transaction(&self) -> &str {
        &self.transaction
    }

    pub(crate) fn jsep(&self) -> Option<&JsonValue> {
        self.jsep.as_ref()
    }

    pub(crate) fn plugin(&self) -> &EventResponsePluginData {
        &self.plugindata
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct EventResponsePluginData {
    data: JsonValue,
    plugin: String,
}

impl EventResponsePluginData {
    pub(crate) fn data(&self) -> &JsonValue {
        &self.data
    }
}

////////////////////////////////////////////////////////////////////////////////

// A success response on request sent to a plugin handler

#[derive(Debug, Deserialize)]
pub(crate) struct SuccessResponse {
    transaction: String,
    data: SuccessResponseData,
}

impl SuccessResponse {
    pub(crate) fn transaction(&self) -> &str {
        &self.transaction
    }

    pub(crate) fn data(&self) -> &SuccessResponseData {
        &self.data
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct SuccessResponseData {
    id: i64,
}

impl SuccessResponseData {
    pub(crate) fn id(&self) -> i64 {
        self.id
    }
}

////////////////////////////////////////////////////////////////////////////////

// A RTCPeerConnection becoming ready

#[derive(Debug, Deserialize)]
pub(crate) struct WebRtcUpEvent {
    session_id: i64,
    sender: i64,
    opaque_id: String,
}

impl WebRtcUpEvent {
    pub(crate) fn opaque_id(&self) -> &str {
        &self.opaque_id
    }
}

////////////////////////////////////////////////////////////////////////////////

// A RTCPeerConnection closed for a DTLS alert (normal shutdown)

#[derive(Debug, Deserialize)]
pub(crate) struct HangUpEvent {
    session_id: i64,
    sender: i64,
    opaque_id: String,
    reason: String,
}

////////////////////////////////////////////////////////////////////////////////

// Audio or video bytes being received by plugin handle

#[derive(Debug, Deserialize)]
pub(crate) struct MediaEvent {
    session_id: i64,
    sender: i64,
    opaque_id: String,
    #[serde(rename = "type")]
    kind: String,
    receiving: bool,
}

////////////////////////////////////////////////////////////////////////////////

// A session was torn down by the server because of timeout: 60 seconds (by default)

#[derive(Debug, Deserialize)]
pub(crate) struct TimeoutEvent {
    session_id: i64,
}

////////////////////////////////////////////////////////////////////////////////

// Janus reporting problems sending media to a user
// (user sent many NACKs in the last second; uplink=true is from Janus' perspective)

#[derive(Debug, Deserialize)]
pub(crate) struct SlowLinkEvent {
    session_id: i64,
    sender: i64,
    opaque_id: String,
    uplink: bool,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct DetachedEvent {
    session_id: i64,
    sender: i64,
    opaque_id: String,
}

impl DetachedEvent {
    pub(crate) fn opaque_id(&self) -> &str {
        &self.opaque_id
    }
}

////////////////////////////////////////////////////////////////////////////////

// Janus Gateway actual status

#[derive(Debug, Deserialize)]
pub(crate) struct StatusEvent {
    online: bool,
}

impl StatusEvent {
    pub(crate) fn online(&self) -> bool {
        self.online
    }
}
