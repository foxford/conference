use crate::authn::{AgentId, Authenticable};
use crate::backend::janus::{
    CreateHandleRequest, CreateSessionRequest, ErrorResponse, IncomingMessage, MessageRequest,
    TrickleRequest,
};
use crate::db::{janus_handle_shadow, janus_session_shadow, rtc, ConnectionPool};
use crate::transport::correlation_data::{from_base64, to_base64};
use crate::transport::mqtt::compat::{into_event, IncomingEnvelope, IntoEnvelope};
use crate::transport::mqtt::{
    Agent, IncomingRequestProperties, OutgoingRequest, OutgoingRequestProperties,
    OutgoingResponseStatus, Publish,
};
use crate::transport::Destination;
use failure::{format_err, Error};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Transaction {
    CreateSession(CreateSessionTransaction),
    CreateHandle(CreateHandleTransaction),
    CreateStream(CreateStreamTransaction),
    ReadStream(ReadStreamTransaction),
    Trickle(TrickleTransaction),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateSessionTransaction {
    reqp: IncomingRequestProperties,
    rtc_id: Uuid,
}

impl CreateSessionTransaction {
    pub(crate) fn new(reqp: IncomingRequestProperties, rtc_id: Uuid) -> Self {
        Self { reqp, rtc_id }
    }
}

pub(crate) fn create_session_request(
    reqp: IncomingRequestProperties,
    rtc_id: Uuid,
    to: AgentId,
) -> Result<OutgoingRequest<CreateSessionRequest>, Error> {
    let transaction = Transaction::CreateSession(CreateSessionTransaction::new(reqp, rtc_id));
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
    reqp: IncomingRequestProperties,
    rtc_id: Uuid,
    session_id: i64,
}

impl CreateHandleTransaction {
    pub(crate) fn new(reqp: IncomingRequestProperties, rtc_id: Uuid, session_id: i64) -> Self {
        Self {
            reqp,
            rtc_id,
            session_id,
        }
    }
}

pub(crate) fn create_handle_request(
    reqp: IncomingRequestProperties,
    rtc_id: Uuid,
    session_id: i64,
    to: AgentId,
) -> Result<OutgoingRequest<CreateHandleRequest>, Error> {
    let transaction =
        Transaction::CreateHandle(CreateHandleTransaction::new(reqp, rtc_id, session_id));
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

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateStreamTransaction {
    reqp: IncomingRequestProperties,
    rtc_id: Uuid,
}

impl CreateStreamTransaction {
    pub(crate) fn new(reqp: IncomingRequestProperties, rtc_id: Uuid) -> Self {
        Self { reqp, rtc_id }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateStreamRequestBody {
    method: &'static str,
    id: Uuid,
}

impl CreateStreamRequestBody {
    pub(crate) fn new(id: Uuid) -> Self {
        Self {
            method: "stream.create",
            id,
        }
    }
}

pub(crate) fn create_stream_request(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    rtc_id: Uuid,
    jsep: JsonValue,
    to: AgentId,
) -> Result<OutgoingRequest<MessageRequest>, Error> {
    let transaction = Transaction::CreateStream(CreateStreamTransaction::new(reqp, rtc_id));
    let body = CreateStreamRequestBody::new(rtc_id);
    let payload = MessageRequest::new(
        &to_base64(&transaction)?,
        session_id,
        handle_id,
        serde_json::to_value(&body)?,
        Some(jsep),
    );
    let props = OutgoingRequestProperties::new("janus_conference_stream.create");
    Ok(OutgoingRequest::new(
        payload,
        props,
        Destination::Unicast(to),
    ))
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ReadStreamTransaction {
    reqp: IncomingRequestProperties,
}

impl ReadStreamTransaction {
    pub(crate) fn new(reqp: IncomingRequestProperties) -> Self {
        Self { reqp }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ReadStreamRequestBody {
    method: &'static str,
    id: Uuid,
}

impl ReadStreamRequestBody {
    pub(crate) fn new(id: Uuid) -> Self {
        Self {
            method: "stream.read",
            id,
        }
    }
}

pub(crate) fn read_stream_request(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    rtc_id: Uuid,
    jsep: JsonValue,
    to: AgentId,
) -> Result<OutgoingRequest<MessageRequest>, Error> {
    let transaction = Transaction::ReadStream(ReadStreamTransaction::new(reqp));
    let body = ReadStreamRequestBody::new(rtc_id);
    let payload = MessageRequest::new(
        &to_base64(&transaction)?,
        session_id,
        handle_id,
        serde_json::to_value(&body)?,
        Some(jsep),
    );
    let props = OutgoingRequestProperties::new("janus_conference_stream.create");
    Ok(OutgoingRequest::new(
        payload,
        props,
        Destination::Unicast(to),
    ))
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct TrickleTransaction {
    reqp: IncomingRequestProperties,
}

impl TrickleTransaction {
    pub(crate) fn new(reqp: IncomingRequestProperties) -> Self {
        Self { reqp }
    }
}

pub(crate) fn trickle_request(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    jsep: JsonValue,
    to: AgentId,
) -> Result<OutgoingRequest<TrickleRequest>, Error> {
    let transaction = Transaction::Trickle(TrickleTransaction::new(reqp));
    let payload = TrickleRequest::new(&to_base64(&transaction)?, session_id, handle_id, jsep);
    let props = OutgoingRequestProperties::new("janus_trickle.create");
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
    let envelope = serde_json::from_slice::<IncomingEnvelope>(bytes)?;
    let message = into_event::<IncomingMessage>(envelope)?;
    match message.payload() {
        IncomingMessage::Success(ref inresp) => {
            match from_base64::<Transaction>(&inresp.transaction())? {
                // Session has been created
                Transaction::CreateSession(tn) => {
                    // Creating a shadow of Janus Session
                    let session_id = inresp.data().id();
                    let location_id = message.properties().agent_id();
                    let conn = janus.db.get()?;
                    let _ = janus_session_shadow::InsertQuery::new(
                        &tn.rtc_id,
                        session_id,
                        &location_id,
                    )
                    .execute(&conn)?;

                    let req = create_handle_request(tn.reqp, tn.rtc_id, session_id, location_id)?;
                    req.into_envelope()?.publish(tx)
                }
                // Handle has been created
                Transaction::CreateHandle(tn) => {
                    let reqp = tn.reqp;
                    let rtc_id = tn.rtc_id;
                    let id = inresp.data().id();

                    // Creating a shadow of Janus Session
                    let conn = janus.db.get()?;
                    let _ = janus_handle_shadow::InsertQuery::new(id, &rtc_id, &reqp.agent_id())
                        .execute(&conn)?;

                    // Returning Real-Time connection
                    let record = rtc::FindQuery::new(&rtc_id).execute(&conn)?;
                    let resp = crate::app::rtc::RecordResponse::new(
                        record,
                        reqp.to_response(&OutgoingResponseStatus::OK),
                        Destination::Unicast(reqp.agent_id()),
                    );

                    resp.into_envelope()?.publish(tx)
                }
                // An unsupported incoming Success message has been received
                _ => Err(format_err!("on-success: {:?}", inresp)),
            }
        }
        IncomingMessage::Ack(ref inresp) => {
            match from_base64::<Transaction>(&inresp.transaction())? {
                // Conference Stream is being created
                Transaction::CreateStream(_tn) => Err(format_err!("on-ack-stream: {:?}", inresp)),
                // Trickle message has been received by Janus Gateway
                Transaction::Trickle(tn) => {
                    let reqp = tn.reqp;
                    let resp = crate::app::signal::CreateResponse::new(
                        crate::app::signal::CreateResponseData::new(None),
                        reqp.to_response(&OutgoingResponseStatus::OK),
                        Destination::Unicast(reqp.agent_id()),
                    );

                    resp.into_envelope()?.publish(tx)
                }
                // An unsupported incoming Ack message has been received
                _ => Err(format_err!("on-ack: {:?}", inresp)),
            }
        }
        IncomingMessage::Event(ref inresp) => {
            match from_base64::<Transaction>(&inresp.transaction())? {
                // Conference Stream has been created
                Transaction::CreateStream(tn) => {
                    let plugin_data = inresp.plugin().data();
                    let reqp = tn.reqp;

                    // TODO: improve error handling
                    let status = plugin_data.get("status").ok_or_else(|| {
                        format_err!("missing status in a response on {}", reqp.method())
                    })?;
                    if status != 200 {
                        return Err(format_err!("error received on {}", reqp.method()));
                    }

                    // Getting answer (as JSEP)
                    let jsep = inresp.jsep().ok_or_else(|| {
                        format_err!("missing jsep in a response on {}", reqp.method())
                    })?;
                    // Updating Rtc w/ offer (as JSEP) for listeners
                    let _ = {
                        let conn = janus.db.get()?;
                        let offer = plugin_data.get("offer").ok_or_else(|| {
                            format_err!("missing offer in a response on {}", reqp.method())
                        })?;
                        rtc::UpdateQuery::new(&tn.rtc_id)
                            .jsep(offer)
                            .execute(&conn)?
                    };

                    let resp = crate::app::signal::CreateResponse::new(
                        crate::app::signal::CreateResponseData::new(Some(jsep.clone())),
                        reqp.to_response(&OutgoingResponseStatus::OK),
                        Destination::Unicast(reqp.agent_id()),
                    );

                    resp.into_envelope()?.publish(tx)
                }
                Transaction::ReadStream(tn) => {
                    let plugin_data = inresp.plugin().data();
                    let reqp = tn.reqp;

                    // TODO: improve error handling
                    let status = plugin_data.get("status").ok_or_else(|| {
                        format_err!("missing status in a response on {}", reqp.method())
                    })?;
                    if status != 200 {
                        return Err(format_err!("error received on {}", reqp.method()));
                    }

                    let resp = crate::app::signal::CreateResponse::new(
                        crate::app::signal::CreateResponseData::new(None),
                        reqp.to_response(&OutgoingResponseStatus::OK),
                        Destination::Unicast(reqp.agent_id()),
                    );

                    resp.into_envelope()?.publish(tx)
                }
                // An unsupported incoming Event message has been received
                _ => Err(format_err!("on-event: {:?}", inresp)),
            }
        }
        IncomingMessage::Error(ErrorResponse::Session(ref inresp)) => {
            Err(format_err!("on-session-error: {:?}", inresp))
        }
        IncomingMessage::Error(ErrorResponse::Handle(ref inresp)) => {
            Err(format_err!("on-handle-error: {:?}", inresp))
        }
        IncomingMessage::Timeout(ref inev) => Err(format_err!("on-timeout-event: {:?}", inev)),
        IncomingMessage::WebRtcUp(ref inev) => Err(format_err!("on-webrtc-up: {:?}", inev)),
        IncomingMessage::Media(ref inev) => Err(format_err!("on-media: {:?}", inev)),
        IncomingMessage::HangUp(ref inev) => Err(format_err!("on-hangup-event: {:?}", inev)),
        IncomingMessage::SlowLink(ref inev) => Err(format_err!("on-slowlink-event: {:?}", inev)),
    }
}
