use std::sync::Arc;

use crate::util::to_base64;

use self::{
    agent_leave::AgentLeaveRequest,
    create_handle::{CreateHandleRequest, CreateHandleResponse},
    create_session::CreateSessionResponse,
    create_stream::{CreateStreamRequest, CreateStreamTransaction},
    events::{
        DetachedEvent, EventResponse, HangUpEvent, MediaEvent, SlowLinkEvent, TimeoutEvent,
        WebRtcUpEvent,
    },
    read_stream::{ReadStreamRequest, ReadStreamTransaction},
    transactions::Transaction,
    trickle::TrickleRequest,
    update_agent_reader_config::UpdateReaderConfigRequest,
    update_agent_writer_config::UpdateWriterConfigRequest,
    upload_stream::{UploadStreamRequest, UploadStreamTransaction},
};
use anyhow::Context;
use diesel_derive_newtype::DieselNewType;
use isahc::{
    http::{StatusCode, Uri},
    AsyncReadResponseExt, HttpClient, Request,
};
use rand::Rng;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

use derive_more::{Display, FromStr};

pub mod agent_leave;
pub mod create_handle;
pub mod create_session;
pub mod create_stream;
pub mod events;
pub mod read_stream;
pub mod transactions;
pub mod trickle;
pub mod update_agent_reader_config;
pub mod update_agent_writer_config;
pub mod upload_stream;

#[derive(Debug, Clone)]
pub struct JanusClient {
    http: Arc<HttpClient>,
    janus_url: Uri,
}

impl JanusClient {
    pub fn new(janus_url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            http: Arc::new(HttpClient::new()?),
            janus_url: janus_url.parse()?,
        })
    }

    pub async fn poll(&self, session_id: SessionId) -> anyhow::Result<PollResult> {
        let mut response = self
            .http
            .get_async(format!("{}/{}?maxev=5", self.janus_url, session_id))
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(PollResult::SessionNotFound);
        }
        let body = response.text().await?;
        let body: Vec<IncomingEvent> = serde_json::from_str(&body).context(body)?;
        Ok(PollResult::Events(body))
    }

    pub async fn upload_stream(
        &self,
        request: UploadStreamRequest,
        transaction: UploadStreamTransaction,
    ) -> anyhow::Result<()> {
        let _response: AckResponse = self
            .send_request(upload_stream(request, transaction)?)
            .await?;
        Ok(())
    }

    pub async fn agent_leave(&self, request: AgentLeaveRequest) -> anyhow::Result<()> {
        let _response: AckResponse = self.send_request(agent_leave(request)?).await?;
        Ok(())
    }

    pub async fn reader_update(&self, request: UpdateReaderConfigRequest) -> anyhow::Result<()> {
        let _response: AckResponse = self.send_request(update_reader(request)?).await?;
        Ok(())
    }

    pub async fn writer_update(&self, request: UpdateWriterConfigRequest) -> anyhow::Result<()> {
        let _response: AckResponse = self.send_request(update_writer(request)?).await?;
        Ok(())
    }

    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
        transaction: CreateStreamTransaction,
    ) -> anyhow::Result<()> {
        let _response: AckResponse = self
            .send_request(create_stream(dbg!(request), transaction)?)
            .await?;
        Ok(())
    }

    pub async fn read_stream(
        &self,
        request: ReadStreamRequest,
        transaction: ReadStreamTransaction,
    ) -> anyhow::Result<()> {
        let _response: AckResponse = self
            .send_request(read_stream(request, transaction)?)
            .await?;
        Ok(())
    }

    pub async fn trickle_request(&self, request: TrickleRequest) -> anyhow::Result<()> {
        let _response: AckResponse = self.send_request(trickle(request)).await?;
        Ok(())
    }

    pub async fn create_handle(
        &self,
        request: CreateHandleRequest,
    ) -> anyhow::Result<CreateHandleResponse> {
        // let _timer = METRICS.create_handle_time.start_timer();
        let response: JanusResponse<CreateHandleResponse> =
            self.send_request(create_handle(request)).await?;
        Ok(response.data)
    }

    pub async fn create_session(&self) -> anyhow::Result<CreateSessionResponse> {
        let response: JanusResponse<CreateSessionResponse> =
            self.send_request(create_session()).await?;
        Ok(response.data)
    }

    async fn send_request<R: DeserializeOwned>(&self, body: impl Serialize) -> anyhow::Result<R> {
        let body = serde_json::to_vec(&body)?;
        let request = Request::post(self.janus_url.clone()).body(body)?;
        let response = self.http.send_async(request).await?.text().await?;
        Ok(serde_json::from_str(&response).context(response)?)
    }
}

pub enum PollResult {
    SessionNotFound,
    Events(Vec<IncomingEvent>),
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

#[derive(
    Debug, Deserialize, Serialize, Display, Copy, Clone, DieselNewType, Hash, PartialEq, Eq, FromStr,
)]
pub struct SessionId(i64);

impl SessionId {
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
    Media(MediaEvent),
    Timeout(TimeoutEvent),
    HangUp(HangUpEvent),
    SlowLink(SlowLinkEvent),
    Detached(DetachedEvent),
    Event(EventResponse),
    KeepAlive,
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
    transaction: String,
    janus: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin: Option<&'static str>,
    #[serde(flatten)]
    data: T,
}

fn create_session() -> JanusRequest<()> {
    JanusRequest {
        transaction: Uuid::new_v4().to_string(),
        plugin: None,
        janus: "create",
        data: (),
    }
}

fn create_handle(request: CreateHandleRequest) -> JanusRequest<CreateHandleRequest> {
    JanusRequest {
        transaction: Uuid::new_v4().to_string(),
        janus: "attach",
        plugin: Some("janus.plugin.conference"),
        data: request,
    }
}

fn trickle(request: TrickleRequest) -> JanusRequest<TrickleRequest> {
    JanusRequest {
        transaction: Uuid::new_v4().to_string(),
        janus: "trickle",
        plugin: None,
        data: request,
    }
}

fn read_stream(
    request: ReadStreamRequest,
    transaction: ReadStreamTransaction,
) -> anyhow::Result<JanusRequest<ReadStreamRequest>> {
    Ok(JanusRequest {
        transaction: to_base64(&Transaction::ReadStream(transaction))?,
        janus: "message",
        plugin: None,
        data: request,
    })
}

fn create_stream(
    request: CreateStreamRequest,
    transaction: CreateStreamTransaction,
) -> anyhow::Result<JanusRequest<CreateStreamRequest>> {
    Ok(JanusRequest {
        transaction: to_base64(&Transaction::CreateStream(transaction))?,
        janus: "message",
        plugin: None,
        data: request,
    })
}

fn update_reader(
    request: UpdateReaderConfigRequest,
) -> anyhow::Result<JanusRequest<UpdateReaderConfigRequest>> {
    Ok(JanusRequest {
        transaction: to_base64(&Transaction::UpdateReaderConfig)?,
        janus: "message",
        plugin: None,
        data: request,
    })
}

fn update_writer(
    request: UpdateWriterConfigRequest,
) -> anyhow::Result<JanusRequest<UpdateWriterConfigRequest>> {
    Ok(JanusRequest {
        transaction: to_base64(&Transaction::UpdateWriterConfig)?,
        janus: "message",
        plugin: None,
        data: request,
    })
}

fn agent_leave(request: AgentLeaveRequest) -> anyhow::Result<JanusRequest<AgentLeaveRequest>> {
    Ok(JanusRequest {
        transaction: to_base64(&Transaction::AgentLeave)?,
        janus: "message",
        plugin: None,
        data: request,
    })
}

fn upload_stream(
    request: UploadStreamRequest,
    transaction: UploadStreamTransaction,
) -> anyhow::Result<JanusRequest<UploadStreamRequest>> {
    Ok(JanusRequest {
        transaction: to_base64(&Transaction::UploadStream(transaction))?,
        janus: "message",
        plugin: None,
        data: request,
    })
}

#[test]
fn test() {
    // dbg!(serde_json::to_string_pretty(&create_session()));
}
