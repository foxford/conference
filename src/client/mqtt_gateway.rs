use async_trait::async_trait;

use reqwest::{Client, StatusCode, Url};
use svc_agent::AgentId;

use crate::app::API_VERSION;

#[derive(Debug)]
pub enum Error {
    Http(reqwest::Error),
    UnexpectedResponse(StatusCode),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::Http(err)
    }
}

#[async_trait]
pub trait MqttGatewayClient: Sync + Send {
    async fn create_subscription(&self, subject: AgentId, object: &[&str]) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct MqttGatewayHttpClient {
    http: Client,
    token: String,
    mqtt_api_host_uri: Url,
}

impl MqttGatewayHttpClient {
    pub fn new(token: String, mqtt_api_host_uri: Url) -> Self {
        Self {
            http: Client::new(),
            token,
            mqtt_api_host_uri,
        }
    }
}

#[async_trait]
impl MqttGatewayClient for MqttGatewayHttpClient {
    async fn create_subscription(&self, subject: AgentId, object: &[&str]) -> Result<(), Error> {
        let mut uri = self.mqtt_api_host_uri.clone();
        uri.set_path("/api/v1/subscriptions");

        let r = self
            .http
            .post(uri)
            .json(&serde_json::json!({
                "object": object,
                "subject": subject,
                "version": API_VERSION
            }))
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?;

        match r.status() {
            StatusCode::OK => Ok(()),
            otherwise => Err(Error::UnexpectedResponse(otherwise)),
        }
    }
}
