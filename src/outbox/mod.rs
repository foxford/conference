use svc_nats_client::EventId;

pub mod config;
pub mod db;
pub mod pipeline;
pub mod util;

pub type ErrorCode = u16;
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub struct StageError {
    code: ErrorCode,
    error: BoxError,
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stage error, code: {}, error: {}", self.code, self.error)
    }
}

impl StageError {
    pub fn new(code: ErrorCode, error: BoxError) -> Self {
        Self { code, error }
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn error(&self) -> &BoxError {
        &self.error
    }
}

#[async_trait::async_trait]
pub trait StageHandle
where
    Self: Sized + Clone + Send + Sync + 'static,
{
    type Context;
    type Stage;

    async fn handle(
        &self,
        ctx: &Self::Context,
        id: &EventId,
    ) -> Result<Option<Self::Stage>, StageError>;
}
