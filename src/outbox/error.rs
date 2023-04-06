use std::vec::IntoIter;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub struct StageError {
    kind: String,
    error: BoxError,
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stage error, kind: {}, error: {}", self.kind, self.error)
    }
}

impl StageError {
    pub fn new(kind: String, error: BoxError) -> Self {
        Self { kind, error }
    }

    pub fn kind(&self) -> &str {
        &self.kind
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
    StageError(String),
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
    pub kind: ErrorKind,
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

pub struct PipelineErrors(Vec<PipelineError>);

impl PipelineErrors {
    pub fn new() -> Self {
        PipelineErrors(vec![])
    }

    pub fn add(&mut self, error: PipelineError) {
        self.0.push(error);
    }
}

impl From<PipelineError> for PipelineErrors {
    fn from(error: PipelineError) -> Self {
        PipelineErrors(vec![error])
    }
}

impl IntoIterator for PipelineErrors {
    type Item = PipelineError;
    type IntoIter = IntoIter<PipelineError>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<StageError> for PipelineError {
    fn from(error: StageError) -> Self {
        PipelineError {
            kind: ErrorKind::StageError(error.kind),
            error: error.error,
        }
    }
}

impl From<diesel::result::Error> for PipelineErrors {
    fn from(source: diesel::result::Error) -> Self {
        let error = PipelineError::new(ErrorKind::DbQueryFailed, Box::new(source));
        PipelineErrors(vec![error])
    }
}
