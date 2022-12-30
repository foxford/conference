use std::net::IpAddr;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};

use crate::{
    app::{endpoint::rtc_signal::CreateResponseData, error},
    backend::janus::online_handler::StreamCallback,
};

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
pub trait ConferenceClient: Sync + Send {
    async fn stream_callback(
        &self,
        replica_addr: IpAddr,
        response: Result<CreateResponseData, error::Error>,
        id: usize,
    ) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct ConferenceHttpClient {
    http: Client,
    token: String,
}

impl ConferenceHttpClient {
    pub fn new(token: String) -> Self {
        Self {
            http: Client::new(),
            token,
        }
    }
}

const INTERNAL_API_PORT: usize = 8081;

#[async_trait]
impl ConferenceClient for ConferenceHttpClient {
    async fn stream_callback(
        &self,
        replica_addr: IpAddr,
        response: Result<CreateResponseData, error::Error>,
        id: usize,
    ) -> Result<(), Error> {
        let uri = format!("http://{replica_addr}:{INTERNAL_API_PORT}/callbacks/stream",);

        let r = self
            .http
            .post(uri)
            .json(&StreamCallback::new(response, id))
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?;

        match r.status() {
            StatusCode::OK => Ok(()),
            otherwise => Err(Error::UnexpectedResponse(otherwise)),
        }
    }
}
