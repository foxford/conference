use crate::outbox::{BoxError, StageError, StageHandle};
use serde::de::DeserializeOwned;
use serde::Serialize;
use svc_nats_client::EventId;

pub mod diesel;

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum ErrorKind {
    DbConnAcquisitionFailed,
    DbQueryFailed,
    LoadStagesFailed,
    SerializationFailed,
    DeserializationFailed,
    DeleteStageFailed,
    UpdateStageFailed,
    InsertStageFailed,
    RunningStageFailed,
}

pub trait PipelineErrorExt<T> {
    fn error(self, kind: ErrorKind) -> Result<T, Error>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> PipelineErrorExt<T> for Result<T, E> {
    fn error(self, kind: ErrorKind) -> Result<T, Error> {
        self.map_err(|source| PipelineError::new(kind, Box::new(source)).into())
    }
}

#[derive(Debug, thiserror::Error)]
pub struct PipelineError {
    kind: ErrorKind,
    error: BoxError,
}

impl PipelineError {
    pub fn new(kind: ErrorKind, error: BoxError) -> Self {
        Self { kind, error }
    }
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pipeline error, kind: {:?}, error: {:?}",
            self.kind, self.error
        )
    }
}

#[derive(Debug, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[error(transparent)]
    PipelineError(#[from] PipelineError),
    #[error(transparent)]
    StageError(#[from] StageError),
}

#[async_trait::async_trait]
pub trait Pipeline {
    async fn run_single_stage<T, C>(&self, ctx: C, id: EventId) -> Result<(), Error>
    where
        T: StageHandle<Context = C, Stage = T>,
        T: Clone + Serialize + DeserializeOwned,
        C: Clone + Send + Sync + 'static;

    async fn run_multiple_stages<T, C>(&self, ctx: C, records_per_try: i64) -> Result<(), Error>
    where
        T: StageHandle<Context = C, Stage = T>,
        T: Clone + Serialize + DeserializeOwned,
        C: Clone + Send + Sync + 'static;
}
