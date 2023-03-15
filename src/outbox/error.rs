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
    fn error(self, kind: ErrorKind) -> Result<T, PipelineError>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> PipelineErrorExt<T> for Result<T, E> {
    fn error(self, kind: ErrorKind) -> Result<T, PipelineError> {
        self.map_err(|source| PipelineError::new(kind, Box::new(source)))
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
