use serde_derive::{Deserialize, Serialize};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Transaction {
    AgentLeave(agent_leave::TransactionData),
    CreateControlHandle(create_control_handle::TransactionData),
    CreatePoolHandle(create_pool_handle::TransactionData),
    CreateSession(create_session::TransactionData),
    CreateStream(create_stream::TransactionData),
    ReadStream(read_stream::TransactionData),
    Trickle(trickle::TransactionData),
    UpdateReaderConfig,
    UpdateWriterConfig,
    UploadStream(upload_stream::TransactionData),
}

mod agent_leave;
mod create_control_handle;
mod create_pool_handle;
mod create_session;
mod create_stream;
mod read_stream;
mod trickle;
mod update_agent_reader_config;
mod update_agent_writer_config;
mod upload_stream;
