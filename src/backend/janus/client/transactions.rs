use crate::trace_id::TraceId;

use super::{
    create_stream::CreateStreamTransaction, read_stream::ReadStreamTransaction,
    upload_stream::UploadStreamTransaction,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Transaction {
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: Option<TraceId>,
    #[serde(with = "super::serialize_as_base64")]
    pub kind: Option<TransactionKind>,
}

impl Transaction {
    pub fn new(kind: TransactionKind) -> Self {
        Self {
            trace_id: TraceId::get(),
            kind: Some(kind),
        }
    }

    pub fn only_id() -> Self {
        Self {
            trace_id: TraceId::get(),
            kind: None,
        }
    }

    pub fn trace_id(&self) -> Option<&TraceId> {
        self.trace_id.as_ref()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum TransactionKind {
    AgentLeave,
    CreateStream(CreateStreamTransaction),
    ReadStream(ReadStreamTransaction),
    UpdateReaderConfig,
    UpdateWriterConfig,
    UploadStream(UploadStreamTransaction),
    AgentSpeaking,
    ServicePing,
}
