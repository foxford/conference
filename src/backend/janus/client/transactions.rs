use super::{
    create_stream::CreateStreamTransaction, read_stream::ReadStreamTransaction,
    upload_stream::UploadStreamTransaction,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum Transaction {
    AgentLeave,
    CreateStream(CreateStreamTransaction),
    ReadStream(ReadStreamTransaction),
    UpdateReaderConfig,
    UpdateWriterConfig,
    UploadStream(UploadStreamTransaction),
    SpeakingNotification,
}
