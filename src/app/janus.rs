use super::Agent;
use crate::backend;
use crate::transport::correlation_data::to_base64;
use failure::{err_msg, Error};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Transaction {
    CreateSession(CreateSessionTransaction),
    CreateHandle(CreateHandleTransaction),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateSessionTransaction {
    room_id: Uuid,
    rtc_id: Uuid,
}

impl CreateSessionTransaction {
    pub(crate) fn new(room_id: Uuid, rtc_id: Uuid) -> Self {
        Self { room_id, rtc_id }
    }
}

pub(crate) fn create_session_request(
    room_id: Uuid,
    rtc_id: Uuid,
) -> Result<backend::janus::CreateSessionRequest, Error> {
    let transaction = Transaction::CreateSession(CreateSessionTransaction::new(room_id, rtc_id));
    let message = backend::janus::CreateSessionRequest::new(&to_base64(&transaction)?);
    Ok(message)
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateHandleTransaction {
    previous: CreateSessionTransaction,
    session_id: u64,
}

impl CreateHandleTransaction {
    pub(crate) fn new(previous: CreateSessionTransaction, session_id: u64) -> Self {
        Self {
            previous,
            session_id,
        }
    }
}

pub(crate) fn create_handle_request(
    previous: CreateSessionTransaction,
    session_id: u64,
) -> Result<backend::janus::CreateHandleRequest, Error> {
    let transaction = Transaction::CreateHandle(CreateHandleTransaction::new(previous, session_id));
    let message = backend::janus::CreateHandleRequest::new(
        &to_base64(&transaction)?,
        session_id,
        "janus.plugin.conference",
    );
    Ok(message)
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn handle_message(tx: &mut Agent, data: &[u8]) -> Result<(), Error> {
    use crate::backend::janus::{ErrorResponse, Response};
    use crate::transport::compat::{from_envelope, Envelope};
    use crate::transport::correlation_data::from_base64;
    use crate::transport::AgentId;

    let envelope = serde_json::from_slice::<Envelope>(data)?;
    let (payload, properties) = from_envelope::<Response>(envelope)?;
    match payload {
        Response::Success(ref resp) => {
            match from_base64::<Transaction>(&resp.transaction)? {
                // Session created
                Transaction::CreateSession(tn) => {
                    let req = create_handle_request(tn, resp.data.id)?;
                    tx.publish(&tx.backend_input_topic(&AgentId::from(&properties)), &req)
                }
                // Handle created
                Transaction::CreateHandle(_tn) => Ok(()),
            }
        }
        Response::Error(ErrorResponse::Session(ref resp)) => {
            Err(err_msg(format!("on-session-error: {:?}", resp)))
        }
        Response::Error(ErrorResponse::Handle(ref resp)) => {
            Err(err_msg(format!("on-handle-error: {:?}", resp)))
        }
        Response::Timeout(ref event) => Err(err_msg(format!("on-timeout-event: {:?}", event))),
    }
}
