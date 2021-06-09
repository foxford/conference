use std::sync::Arc;

use self::{
    create_handle::{CreateHandleRequest, CreateHandleResponse},
    create_stream::{CreateStreamRequest, CreateStreamResponse},
    read_stream::{ReadStreamRequest, ReadStreamResponse},
};
use anyhow::anyhow;
use isahc::{http::Uri, AsyncReadResponseExt, HttpClient, Request};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use slog::warn;
use uuid::Uuid;

pub mod create_handle;
pub mod create_stream;
pub mod read_stream;

#[derive(Debug)]
pub struct JanusClient {
    http: Arc<HttpClient>,
    janus_url: Uri,
}

impl JanusClient {
    pub fn new(janus_url: Uri) -> anyhow::Result<Self> {
        Ok(Self {
            http: Arc::new(HttpClient::new()?),
            janus_url,
        })
    }

    pub async fn create_stream(&self, request: &CreateStreamRequest) -> anyhow::Result<()> {
        let request = Request::post(format!(
            "{}/{}/{}",
            self.janus_url,
            request.session_id(),
            request.handle_id()
        ))
        .body(serde_json::to_vec(&request)?)?;
        let create_handle_response: String = self.http.send_async(request).await?.text().await?;
        warn!(crate::LOG, "resp: {}", create_handle_response);
        Ok(())
        // serde_json::from_str(&create_handle_response).map_err(|err| {
        //     let err = anyhow!("Err: {:?}, raw: {}", err, create_handle_response);
        //     warn!(crate::LOG, "raw {}", err);
        //     err
        // })
    }

    pub async fn read_stream(
        &self,
        request: &ReadStreamRequest,
    ) -> anyhow::Result<ReadStreamResponse> {
        let request = Request::post(format!(
            "{}/{}/{}",
            self.janus_url,
            request.session_id(),
            request.handle_id()
        ))
        .body(serde_json::to_vec(&request)?)?;
        let create_handle_response: CreateStreamResponse =
            self.http.send_async(request).await?.json().await?;
        Ok(create_handle_response)
    }

    pub async fn create_handle(
        &self,
        request: &CreateHandleRequest,
    ) -> anyhow::Result<CreateHandleResponse> {
        let body = serde_json::to_vec(&json!( {
            "janus": "attach",
            "transaction": Uuid::new_v4().to_string(),
            "session_id": request.session_id,
            "plugin": "janus.plugin.conference",
            "opaque_id": request.opaque_id,
        }))?;
        let request =
            Request::post(format!("{}/{}", self.janus_url, request.session_id)).body(body)?;
        let create_handle_response: JanusResponse<CreateHandleResponse> =
            self.http.send_async(request).await?.json().await?;

        Ok(create_handle_response.data)
    }
}

#[derive(Deserialize)]
struct JanusResponse<T> {
    data: T,
}

#[derive(Serialize)]
struct JanusRequest<T> {
    transaction: String,
    janus: &'static str,
    #[serde(flatten)]
    data: T,
}
