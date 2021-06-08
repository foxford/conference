use std::sync::Arc;

use self::create_handle::{CreateHandleRequest, CreateHandleResponse};
use anyhow::anyhow;
use isahc::{http::Uri, AsyncReadResponseExt, HttpClient, Request};
use serde_json::{json, Value};
use uuid::Uuid;
pub mod create_handle;

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
        let create_handle_response: Value = self.http.send_async(request).await?.json().await?;
        let id = create_handle_response
            .get("data")
            .ok_or_else(|| anyhow!("Missing data"))?
            .get("id")
            .ok_or_else(|| anyhow!("Missing id"))?
            .as_i64()
            .ok_or_else(|| anyhow!("id not i64"))?;
        Ok(CreateHandleResponse { id })
    }
}
