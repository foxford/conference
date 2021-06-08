use std::sync::Arc;

use isahc::{http::Uri, AsyncReadResponseExt, HttpClient, Request};
use uuid::Uuid;

use self::create_handle::{CreateHandleRequest, CreateHandleResponse};
use serde_json::json;
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
        }))?;
        let request =
            Request::post(format!("{}/{}", self.janus_url, request.session_id)).body(body)?;
        Ok(self.http.send_async(request).await?.json().await?)
    }
}
