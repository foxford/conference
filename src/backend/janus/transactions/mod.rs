use serde_derive::{Deserialize, Serialize};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Transaction {
    AgentLeave(agent_leave::TransactionData),
    CreateAgentHandle(create_agent_handle::TransactionData),
    CreateServiceHandle(create_service_handle::TransactionData),
    CreateSession(create_session::TransactionData),
    CreateSignal(create_signal::TransactionData),
    UpdateSignal(update_signal::TransactionData),
    CreateStream(create_stream::TransactionData),
    ReadStream(read_stream::TransactionData),
    Trickle(trickle::TransactionData),
    UploadStream(upload_stream::TransactionData),
}

mod agent_leave;
mod create_agent_handle;
mod create_service_handle;
mod create_session;
mod create_signal;
mod create_stream;
mod read_stream;
mod trickle;
mod update_signal;
mod upload_stream;
