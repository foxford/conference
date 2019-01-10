use crate::app::rtc::CreateRequest as CreateRtcRequest;
use crate::authn::{AgentId, Authenticable};
use crate::backend::janus::{CreateHandleRequest, CreateSessionRequest, ErrorResponse, Response};
use crate::db::{janus_handle_shadow, janus_session_shadow, rtc, ConnectionPool};
use crate::transport::correlation_data::{from_base64, to_base64};
use crate::transport::mqtt::compat;
use crate::transport::mqtt::{
    Agent, OutgoingRequest, OutgoingRequestProperties, OutgoingResponseStatus, Publish,
};
use crate::transport::Destination;
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
) -> Result<OutgoingRequest<CreateSessionRequest>, Error> {
    let transaction = Transaction::CreateSession(CreateSessionTransaction::new(rtc, req));
    let payload = CreateSessionRequest::new(&to_base64(&transaction)?);
    let props = OutgoingRequestProperties::new("janus_session.create");
    Ok(OutgoingRequest::new(
        payload,
        props,
        Destination::Unicast(to),
    ))
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
) -> Result<OutgoingRequest<CreateHandleRequest>, Error> {
    let transaction = Transaction::CreateHandle(CreateHandleTransaction::new(previous, session_id));
    let payload = CreateHandleRequest::new(
        &to_base64(&transaction)?,
        session_id,
        "janus.plugin.conference",
    );
    let props = OutgoingRequestProperties::new("janus_handle.create");
    Ok(OutgoingRequest::new(
        payload,
        props,
        Destination::Unicast(to),
    ))
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(db: ConnectionPool) -> Self {
        Self { db }
    }
}

pub(crate) fn handle_message(tx: &mut Agent, bytes: &[u8], janus: &State) -> Result<(), Error> {
    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(bytes)?;
    let message = compat::into_event::<Response>(envelope)?;
    match message.payload() {
        Response::Success(ref resp) => {
            match from_base64::<Transaction>(&resp.transaction)? {
                // Session created
                Transaction::CreateSession(tn) => {
                    // Creating a shadow of Janus Session
                    let rtc_id = tn.rtc.id();
                    let session_id = resp.data.id;
                    let location_id = message.properties().agent_id();
                    let conn = janus.db.get()?;
                    let _ =
                        janus_session_shadow::InsertQuery::new(rtc_id, session_id, &location_id)
                            .execute(&conn)?;

                    let req = create_handle_request(tn, session_id, location_id)?;
                    req.publish(tx)
                }
                // Handle created
                Transaction::CreateHandle(tn) => {
                    // Creating a shadow of Janus Session
                    let handle_id = resp.data.id;
                    let rtc = tn.previous.rtc;
                    let req = tn.previous.req;
                    let owner_id = req.properties().agent_id();
                    let conn = janus.db.get()?;
                    let _ = janus_handle_shadow::InsertQuery::new(handle_id, rtc.id(), &owner_id)
                        .execute(&conn)?;

                    let status = OutgoingResponseStatus::Success;
                    let resp = req.to_response(rtc, status);
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
