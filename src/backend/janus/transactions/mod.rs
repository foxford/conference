use serde_derive::{Deserialize, Serialize};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Transaction {
    AgentLeave(agent_leave::TransactionData),
    CreateHandle(create_handle::TransactionData),
    CreateSession(create_session::TransactionData),
    CreateStream(create_stream::TransactionData),
    CreateRtcHandle(create_rtc_handle::TransactionData),
    ReadStream(read_stream::TransactionData),
    Trickle(trickle::TransactionData),
    UploadStream(upload_stream::TransactionData),
}

mod agent_leave;
mod create_handle;
mod create_rtc_handle;
mod create_session;
mod create_stream;
mod read_stream;
mod trickle;
mod upload_stream;
