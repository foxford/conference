use crate::app::model::{janus_handle_shadow, janus_session_shadow, rtc};
use crate::app::rtc::CreateRequest as CreateRtcRequest;
use crate::backend::janus::{CreateHandleRequest, CreateSessionRequest, ErrorResponse, Response};
use crate::transport::correlation_data::{from_base64, to_base64};
use crate::transport::mqtt::compat;
use crate::transport::mqtt::{
    Agent, LocalMessage, LocalMessageProperties, LocalRequestMessageProperties,
    LocalResponseMessageStatus, Publish,
};
use crate::transport::{AgentId, Destination};
use failure::{format_err, Error};
use serde_derive::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Transaction {
    CreateSession(CreateSessionTransaction),
    CreateHandle(CreateHandleTransaction),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateSessionTransaction {
    rtc: rtc::Record,
    req: CreateRtcRequest,
}

impl CreateSessionTransaction {
    pub(crate) fn new(rtc: rtc::Record, req: CreateRtcRequest) -> Self {
        Self { rtc, req }
    }
}

pub(crate) fn create_session_request(
    rtc: rtc::Record,
    req: CreateRtcRequest,
    to: AgentId,
) -> Result<LocalMessage<CreateSessionRequest>, Error> {
    let transaction = Transaction::CreateSession(CreateSessionTransaction::new(rtc, req));
    let payload = CreateSessionRequest::new(&to_base64(&transaction)?);
    let method = LocalRequestMessageProperties::new("janus_session.create");
    let props = LocalMessageProperties::Request(method);
    let message = LocalMessage::new(payload, props, Destination::Unicast(to));
    Ok(message)
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateHandleTransaction {
    previous: CreateSessionTransaction,
    session_id: i64,
}

impl CreateHandleTransaction {
    pub(crate) fn new(previous: CreateSessionTransaction, session_id: i64) -> Self {
        Self {
            previous,
            session_id,
        }
    }
}

pub(crate) fn create_handle_request(
    previous: CreateSessionTransaction,
    session_id: i64,
    to: AgentId,
) -> Result<LocalMessage<CreateHandleRequest>, Error> {
    let transaction = Transaction::CreateHandle(CreateHandleTransaction::new(previous, session_id));
    let method = LocalRequestMessageProperties::new("janus_handle.create");
    let props = LocalMessageProperties::Request(method);
    let payload = CreateHandleRequest::new(
        &to_base64(&transaction)?,
        session_id,
        "janus.plugin.conference",
    );
    let message = LocalMessage::new(payload, props, Destination::Unicast(to));
    Ok(message)
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn handle_message(tx: &mut Agent, bytes: &[u8]) -> Result<(), Error> {
    let envelope = serde_json::from_slice::<compat::Envelope>(bytes)?;
    let message = compat::into_message::<Response>(envelope)?;
    match message.payload() {
        Response::Success(ref resp) => {
            match from_base64::<Transaction>(&resp.transaction)? {
                // Session created
                Transaction::CreateSession(tn) => {
                    // Creating a shadow of Janus Session
                    let rtc_id = tn.rtc.id();
                    let session_id = resp.data.id;
                    let location_id = message.agent_id();
                    let _ =
                        janus_session_shadow::InsertQuery::new(rtc_id, session_id, &location_id)
                            .execute()?;

                    let req = create_handle_request(tn, session_id, location_id)?;
                    req.publish(tx)
                }
                // Handle created
                Transaction::CreateHandle(tn) => {
                    // Creating a shadow of Janus Session
                    let handle_id = resp.data.id;
                    let rtc = tn.previous.rtc;
                    let req = tn.previous.req;
                    let _ =
                        janus_handle_shadow::InsertQuery::new(handle_id, rtc.id(), &req.agent_id())
                            .execute()?;

                    let status = LocalResponseMessageStatus::Success;
                    let resp = req
                        .to_response(rtc, status)
                        .expect("Error converting request to response");
                    resp.publish(tx)
                }
            }
        }
        Response::Error(ErrorResponse::Session(ref resp)) => {
            Err(format_err!("on-session-error: {:?}", resp))
        }
        Response::Error(ErrorResponse::Handle(ref resp)) => {
            Err(format_err!("on-handle-error: {:?}", resp))
        }
        Response::Timeout(ref event) => Err(format_err!("on-timeout-event: {:?}", event)),
    }
}
