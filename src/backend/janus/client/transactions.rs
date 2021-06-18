use super::{
    create_stream::CreateStreamTransaction, read_stream::ReadStreamTransaction,
    upload_stream::UploadStreamTransaction,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum Transaction {
    AgentLeave,
    CreateStream(CreateStreamTransaction),
    // CreateRtcHandle(create_rtc_handle::TransactionData),
    ReadStream(ReadStreamTransaction),
    // Trickle(trickle::TransactionData),
    UpdateReaderConfig,
    UpdateWriterConfig,
    UploadStream(UploadStreamTransaction),
}
