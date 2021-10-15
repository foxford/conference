use self::{
    create_handle::{CreateHandleRequest, CreateHandleResponse, OpaqueId},
    create_stream::{CreateStreamRequest, CreateStreamResponse},
    events::{DetachedEvent, EventResponse, HangUpEvent, WebRtcUpEvent},
    read_stream::{ReadStreamRequest, ReadStreamResponse},
    trickle::TrickleRequest,
    update_agent_reader_config::UpdateReaderConfigRequest,
    update_agent_writer_config::UpdateWriterConfigRequest,
    upload_stream::{UploadResponse, UploadStreamRequest},
};
use anyhow::Context;
use diesel_derive_newtype::DieselNewType;

use http::HeaderMap;
use rand::Rng;
use reqwest::{Client, StatusCode, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use derive_more::{Display, FromStr};
use uuid::Uuid;

pub mod create_handle;
pub mod create_stream;
pub mod events;
pub mod read_stream;
pub mod trickle;
pub mod update_agent_reader_config;
pub mod update_agent_writer_config;
pub mod upload_stream;

#[derive(Debug, Clone)]
pub struct JanusClient {
    http: Client,
    janus_url: Url,
}

impl JanusClient {
    pub fn new(janus_url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            http: Client::builder()
                .default_headers({
                    let mut header_map = HeaderMap::new();
                    header_map.insert("Content-type", "application/json".parse()?);
                    header_map
                })
                .build()?,
            janus_url: janus_url.parse()?,
        })
    }

    pub async fn poll(&self) -> anyhow::Result<PollResult> {
        let response = self
            .http
            .get(dbg!(format!("{}poll?max_events=5", self.janus_url)))
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(PollResult::SessionNotFound);
        }
        let body = response.text().await?;
        let body: Vec<Value> = dbg!(serde_json::from_str(&body).context(body)?);
        Ok(PollResult::Events(body))
    }

    pub async fn upload_stream(
        &self,
        request: UploadStreamRequest,
    ) -> anyhow::Result<UploadResponse> {
        let response: UploadResponse = self.send_request("stream-upload", request).await?;
        Ok(response)
    }

    pub async fn reader_update(&self, request: UpdateReaderConfigRequest) -> anyhow::Result<()> {
        let _response: Value = self.send_request("reader-config-update", request).await?;
        Ok(())
    }

    pub async fn writer_update(&self, request: UpdateWriterConfigRequest) -> anyhow::Result<()> {
        self.send_request("writer-config-update", request).await?;
        Ok(())
    }

    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> anyhow::Result<CreateStreamResponse> {
        self.send_request("proxy", request).await
    }

    pub async fn read_stream(
        &self,
        request: ReadStreamRequest,
    ) -> anyhow::Result<ReadStreamResponse> {
        self.send_request("proxy", request).await
    }

    pub async fn trickle_request(&self, request: TrickleRequest) -> anyhow::Result<()> {
        let _response: Value = self.send_request("proxy", request).await?;
        Ok(())
    }

    pub async fn create_handle(
        &self,
        request: CreateHandleRequest,
    ) -> anyhow::Result<CreateHandleResponse> {
        // let _timer = METRICS.create_handle_time.start_timer();
        let response: JanusResponse<CreateHandleResponse> = self
            .send_request("create-handle", create_handle(request))
            .await?;
        Ok(response.data)
    }

    async fn send_request<R: DeserializeOwned>(
        &self,
        method: &str,
        body: impl Serialize,
    ) -> anyhow::Result<R> {
        let body = serde_json::to_vec(&body)?;
        let response = self
            .http
            .post(format!("{}{}", self.janus_url, method))
            .body(body)
            .send()
            .await?
            .text()
            .await?;
        Ok(serde_json::from_str(&response).context(response)?)
    }
}

#[derive(Debug)]
pub enum PollResult {
    SessionNotFound,
    Events(Vec<Value>),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Jsep {
    // '{"type": "offer", "sdp": _}' or '{"type": "answer", "sdp": _}'
    OfferOrAnswer {
        #[serde(rename = "type")]
        kind: JsepType,
        sdp: String,
    },
    IceCandidate(IceCandidateSdp),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JsepType {
    Offer,
    Answer,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IceCandidate {
    #[serde(rename = "sdpMid")]
    _sdp_mid: String,
    #[serde(rename = "sdpMLineIndex")]
    _sdp_m_line_index: u16,
    #[serde(rename = "candidate")]
    _candidate: String,
    #[serde(rename = "usernameFragment")]
    _username_fragment: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum IceCandidateSdpItem {
    IceCandidate(IceCandidate),
    // {"completed": true}
    Completed {
        #[serde(rename = "completed")]
        _completed: bool,
    },
    // null
    Null(Option<usize>),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum IceCandidateSdp {
    // {"sdpMid": _, "sdpMLineIndex": _, "candidate": _}
    Single(IceCandidateSdpItem),
    // [{"sdpMid": _, "sdpMLineIndex": _, "candidate": _}, …, {"completed": true}]
    List(Vec<IceCandidateSdpItem>),
}

#[derive(
    Debug, Deserialize, Serialize, Display, Copy, Clone, DieselNewType, Hash, PartialEq, Eq, FromStr,
)]
pub struct HandleId(i64);

impl HandleId {
    pub fn stub_id() -> Self {
        Self(123)
    }

    pub fn random() -> Self {
        Self(rand::thread_rng().gen())
    }
}

/////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "janus")]
#[allow(clippy::large_enum_variant)]
pub enum IncomingEvent {
    WebRtcUp(WebRtcUpEvent),
    HangUp(HangUpEvent),
    Detached(DetachedEvent),
    Event(EventResponse),
}

impl IncomingEvent {
    pub fn event_kind(&self) -> &'static str {
        match self {
            IncomingEvent::WebRtcUp(_) => "WebRtcUp",
            IncomingEvent::HangUp(_) => "HangUp",
            IncomingEvent::Detached(_) => "Detached",
            IncomingEvent::Event(_) => "AgentSpeaking",
        }
    }

    pub fn opaque_id(&self) -> Option<&OpaqueId> {
        match self {
            IncomingEvent::WebRtcUp(x) => Some(&x.opaque_id),
            IncomingEvent::HangUp(x) => Some(&x.opaque_id),
            IncomingEvent::Detached(x) => Some(&x.opaque_id),
            IncomingEvent::Event(x) => Some(&x.opaque_id),
        }
    }
}

#[derive(Deserialize, Debug)]
enum Ack {
    #[serde(rename = "ack")]
    Ack,
}

#[derive(Deserialize, Debug)]
struct AckResponse {
    janus: Ack,
}

#[derive(Deserialize, Debug)]
enum Success {
    #[serde(rename = "success")]
    Success,
}

#[derive(Deserialize, Debug)]
struct JanusResponse<T> {
    data: T,
    janus: Success,
}

#[derive(Serialize, Debug)]
struct JanusRequest<T> {
    transaction: Uuid,
    janus: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin: Option<&'static str>,
    #[serde(flatten)]
    data: T,
}

fn create_handle(request: CreateHandleRequest) -> JanusRequest<CreateHandleRequest> {
    JanusRequest {
        transaction: Uuid::new_v4(),
        janus: "attach",
        plugin: Some("janus.plugin.conference"),
        data: request,
    }
}

mod serialize_as_base64 {
    use serde::{de, ser};

    use crate::util::{from_base64, to_base64};

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: de::Deserializer<'de>,
        T: serde::de::DeserializeOwned,
    {
        let s: String = de::Deserialize::deserialize(deserializer)?;
        from_base64(&s).map_err(de::Error::custom)
    }

    pub fn serialize<S, T>(obj: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
        T: serde::Serialize,
    {
        let s = to_base64(obj).map_err(ser::Error::custom)?;
        serializer.serialize_str(&s)
    }
}
