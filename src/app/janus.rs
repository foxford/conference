use std::ops::Bound;

use chrono::{DateTime, NaiveDateTime, Utc};
use failure::{format_err, Error};
use log::info;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::compat::{into_event, IncomingEnvelope, IntoEnvelope};
use svc_agent::mqtt::{
    Agent, IncomingRequestProperties, OutgoingRequest, OutgoingRequestProperties,
    OutgoingResponseStatus, Publish,
};
use svc_agent::{Addressable, AgentId, Authenticable};
use uuid::Uuid;

use crate::backend::janus::{
    CreateHandleRequest, CreateSessionRequest, ErrorResponse, IncomingMessage, MessageRequest,
    TrickleRequest,
};
use crate::db::{
    janus_handle_shadow, janus_session_shadow, location, recording, room, rtc, ConnectionPool,
};
use crate::util::{from_base64, to_base64};

////////////////////////////////////////////////////////////////////////////////

const IGNORE: &str = "ignore";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Transaction {
    CreateSession(CreateSessionTransaction),
    CreateHandle(CreateHandleTransaction),
    CreateSystemSession(CreateSystemSessionTransaction),
    CreateSystemHandle(CreateSystemHandleTransaction),
    CreateStream(CreateStreamTransaction),
    ReadStream(ReadStreamTransaction),
    UploadStream(UploadStreamTransaction),
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

pub(crate) fn create_session_request<A>(
    reqp: IncomingRequestProperties,
    rtc_id: Uuid,
    to: &A,
) -> Result<OutgoingRequest<CreateSessionRequest>, Error>
where
    A: Addressable,
{
    let transaction = Transaction::CreateSession(CreateSessionTransaction::new(reqp, rtc_id));
    let payload = CreateSessionRequest::new(&to_base64(&transaction)?);
    let props = OutgoingRequestProperties::new("janus_session.create", IGNORE, IGNORE);
    Ok(OutgoingRequest::unicast(payload, props, to))
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateSystemSessionTransaction {}

impl CreateSystemSessionTransaction {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

pub(crate) fn create_system_session_request<A>(
    to: &A,
) -> Result<OutgoingRequest<CreateSessionRequest>, Error>
where
    A: Authenticable,
{
    let transaction = Transaction::CreateSystemSession(CreateSystemSessionTransaction::new());
    let payload = CreateSessionRequest::new(&to_base64(&transaction)?);
    let props = OutgoingRequestProperties::new("janus_session.create", IGNORE, IGNORE);
    Ok(OutgoingRequest::multicast(payload, props, to))
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

pub(crate) fn create_handle_request<A>(
    reqp: IncomingRequestProperties,
    rtc_id: Uuid,
    session_id: i64,
    to: &A,
) -> Result<OutgoingRequest<CreateHandleRequest>, Error>
where
    A: Addressable,
{
    let transaction =
        Transaction::CreateHandle(CreateHandleTransaction::new(reqp, rtc_id, session_id));
    let payload = CreateHandleRequest::new(
        &to_base64(&transaction)?,
        session_id,
        "janus.plugin.conference",
    );
    let props = OutgoingRequestProperties::new("janus_handle.create", IGNORE, IGNORE);
    Ok(OutgoingRequest::unicast(payload, props, to))
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateSystemHandleTransaction {
    session_id: i64,
}

impl CreateSystemHandleTransaction {
    pub(crate) fn new(session_id: i64) -> Self {
        Self { session_id }
    }
}

pub(crate) fn create_system_handle_request<A>(
    session_id: i64,
    to: &A,
) -> Result<OutgoingRequest<CreateHandleRequest>, Error>
where
    A: Addressable,
{
    let transaction =
        Transaction::CreateSystemHandle(CreateSystemHandleTransaction::new(session_id));
    let payload = CreateHandleRequest::new(
        &to_base64(&transaction)?,
        session_id,
        "janus.plugin.conference",
    );
    let props = OutgoingRequestProperties::new("janus_handle.create", IGNORE, IGNORE);
    Ok(OutgoingRequest::unicast(payload, props, to))
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateStreamTransaction {
    reqp: IncomingRequestProperties,
}

impl CreateStreamTransaction {
    pub(crate) fn new(reqp: IncomingRequestProperties) -> Self {
        Self { reqp }
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

pub(crate) fn create_stream_request<A>(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    rtc_id: Uuid,
    jsep: JsonValue,
    to: &A,
) -> Result<OutgoingRequest<MessageRequest>, Error>
where
    A: Addressable,
{
    let transaction = Transaction::CreateStream(CreateStreamTransaction::new(reqp));
    let body = CreateStreamRequestBody::new(rtc_id);
    let payload = MessageRequest::new(
        &to_base64(&transaction)?,
        session_id,
        handle_id,
        serde_json::to_value(&body)?,
        Some(jsep),
    );
    let props = OutgoingRequestProperties::new("janus_conference_stream.create", IGNORE, IGNORE);
    Ok(OutgoingRequest::unicast(payload, props, to))
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

pub(crate) fn read_stream_request<A>(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    rtc_id: Uuid,
    jsep: JsonValue,
    to: &A,
) -> Result<OutgoingRequest<MessageRequest>, Error>
where
    A: Addressable,
{
    let transaction = Transaction::ReadStream(ReadStreamTransaction::new(reqp));
    let body = ReadStreamRequestBody::new(rtc_id);
    let payload = MessageRequest::new(
        &to_base64(&transaction)?,
        session_id,
        handle_id,
        serde_json::to_value(&body)?,
        Some(jsep),
    );
    let props = OutgoingRequestProperties::new("janus_conference_stream.create", IGNORE, IGNORE);
    Ok(OutgoingRequest::unicast(payload, props, to))
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UploadStreamTransaction {
    reqp: IncomingRequestProperties,
}

impl UploadStreamTransaction {
    pub(crate) fn new(reqp: IncomingRequestProperties) -> Self {
        Self { reqp }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UploadStreamRequestBody {
    method: &'static str,
    id: Uuid,
    bucket: String,
    object: String,
}

impl UploadStreamRequestBody {
    pub(crate) fn new(id: Uuid, bucket: &str, object: &str) -> Self {
        Self {
            method: "stream.upload",
            id,
            bucket: bucket.to_owned(),
            object: object.to_owned(),
        }
    }
}

pub(crate) fn upload_stream_request(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    body: UploadStreamRequestBody,
    to: &AgentId,
) -> Result<OutgoingRequest<MessageRequest>, Error> {
    let transaction = Transaction::UploadStream(UploadStreamTransaction::new(reqp));
    let payload = MessageRequest::new(
        &to_base64(&transaction)?,
        session_id,
        handle_id,
        serde_json::to_value(&body)?,
        None,
    );
    let props = OutgoingRequestProperties::new("janus_conference_stream.upload", IGNORE, IGNORE);
    Ok(OutgoingRequest::unicast(payload, props, to))
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

pub(crate) fn trickle_request<A>(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    jsep: JsonValue,
    to: &A,
) -> Result<OutgoingRequest<TrickleRequest>, Error>
where
    A: Addressable,
{
    let transaction = Transaction::Trickle(TrickleTransaction::new(reqp));
    let payload = TrickleRequest::new(&to_base64(&transaction)?, session_id, handle_id, jsep);
    let props = OutgoingRequestProperties::new("janus_trickle.create", IGNORE, IGNORE);
    Ok(OutgoingRequest::unicast(payload, props, to))
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

pub(crate) fn handle_message(
    tx: &mut Agent,
    bytes: &[u8],
    janus: &State,
    system: &mut super::system::State,
) -> Result<(), Error> {
    let envelope = serde_json::from_slice::<IncomingEnvelope>(bytes)?;
    let message = into_event::<IncomingMessage>(envelope)?;
    match message.payload() {
        IncomingMessage::Success(ref inresp) => {
            match from_base64::<Transaction>(&inresp.transaction())? {
                // Session has been created
                Transaction::CreateSession(tn) => {
                    // Creating a shadow of Janus Gateway Session
                    let location_id = message.properties().as_agent_id();
                    let session_id = inresp.data().id();
                    let conn = janus.db.get()?;
                    let _ =
                        janus_session_shadow::InsertQuery::new(tn.rtc_id, session_id, location_id)
                            .execute(&conn)?;

                    let req = create_handle_request(tn.reqp, tn.rtc_id, session_id, location_id)?;
                    req.into_envelope()?.publish(tx)
                }
                // Handle has been created
                Transaction::CreateHandle(tn) => {
                    let reqp = tn.reqp;
                    let agent_id = reqp.as_agent_id();
                    let rtc_id = tn.rtc_id;
                    let id = inresp.data().id();

                    // Creating a shadow of Janus Gateway Session
                    let conn = janus.db.get()?;
                    let _ = janus_handle_shadow::InsertQuery::new(id, rtc_id, &agent_id)
                        .execute(&conn)?;

                    // Returning Real-Time connection
                    let object = rtc::FindQuery::new()
                        .id(rtc_id)
                        .execute(&conn)?
                        .ok_or_else(|| format_err!("the rtc = '{}' is not found", &rtc_id))?;
                    let props = reqp.to_response(OutgoingResponseStatus::OK);
                    let resp = crate::app::rtc::ObjectResponse::unicast(object, props, agent_id);

                    resp.into_envelope()?.publish(tx)
                }
                // System session has been created
                Transaction::CreateSystemSession(_tn) => {
                    let location_id = message.properties().as_agent_id();
                    let session_id = inresp.data().id();

                    system.set_session_id(session_id);

                    let req = create_system_handle_request(session_id, location_id)?;
                    req.into_envelope()?.publish(tx)
                }
                // System handle has been created
                Transaction::CreateSystemHandle(_tn) => {
                    let handle_id = inresp.data().id();

                    system.set_handle_id(handle_id);

                    Ok(())
                }
                // An unsupported incoming Success message has been received
                _ => Err(format_err!(
                    "received an unexpected Success message: {:?}",
                    inresp,
                )),
            }
        }
        IncomingMessage::Ack(ref inresp) => {
            match from_base64::<Transaction>(&inresp.transaction())? {
                // Conference Stream is being created
                Transaction::CreateStream(_tn) => Err(format_err!(
                    "received an unexpected Ack message (stream.create): {:?}",
                    inresp,
                )),
                // Trickle message has been received by Janus Gateway
                Transaction::Trickle(tn) => {
                    let resp = crate::app::signal::CreateResponse::unicast(
                        crate::app::signal::CreateResponseData::new(None),
                        tn.reqp.to_response(OutgoingResponseStatus::OK),
                        tn.reqp.as_agent_id(),
                    );

                    resp.into_envelope()?.publish(tx)
                }
                // An unsupported incoming Ack message has been received
                _ => Err(format_err!(
                    "received an unexpected Ack message: {:?}",
                    inresp,
                )),
            }
        }
        IncomingMessage::Event(ref inresp) => {
            match from_base64::<Transaction>(&inresp.transaction())? {
                // Conference Stream has been created (an answer received)
                Transaction::CreateStream(tn) => {
                    // TODO: improve error handling
                    let plugin_data = inresp.plugin().data();
                    let status = plugin_data.get("status").ok_or_else(|| {
                        format_err!(
                            "missing status in a response on method = {}, transaction = {}",
                            tn.reqp.method(),
                            inresp.transaction()
                        )
                    })?;
                    if status != 200 {
                        return Err(format_err!(
                            "error received on method = {}, transaction = {}",
                            tn.reqp.method(),
                            inresp.transaction()
                        ));
                    }

                    // Getting answer (as JSEP)
                    let jsep = inresp.jsep().ok_or_else(|| {
                        format_err!(
                            "missing jsep in a response on method = {}, transaction = {}",
                            tn.reqp.method(),
                            inresp.transaction()
                        )
                    })?;

                    let resp = crate::app::signal::CreateResponse::unicast(
                        crate::app::signal::CreateResponseData::new(Some(jsep.clone())),
                        tn.reqp.to_response(OutgoingResponseStatus::OK),
                        tn.reqp.as_agent_id(),
                    );

                    resp.into_envelope()?.publish(tx)
                }
                // Conference Stream has been read (an answer received)
                Transaction::ReadStream(tn) => {
                    // TODO: improve error handling
                    let plugin_data = inresp.plugin().data();
                    let status = plugin_data.get("status").ok_or_else(|| {
                        format_err!(
                            "missing status in a response on method = {}, transaction = {}",
                            tn.reqp.method(),
                            inresp.transaction()
                        )
                    })?;
                    if status != 200 {
                        return Err(format_err!(
                            "error received on method = {}, transaction = {}",
                            tn.reqp.method(),
                            inresp.transaction()
                        ));
                    }

                    // Getting answer (as JSEP)
                    let jsep = inresp.jsep().ok_or_else(|| {
                        format_err!(
                            "missing jsep in a response on method = {}, transaction = {}",
                            tn.reqp.method(),
                            inresp.transaction()
                        )
                    })?;

                    let resp = crate::app::signal::CreateResponse::unicast(
                        crate::app::signal::CreateResponseData::new(Some(jsep.clone())),
                        tn.reqp.to_response(OutgoingResponseStatus::OK),
                        tn.reqp.as_agent_id(),
                    );

                    resp.into_envelope()?.publish(tx)
                }
                Transaction::UploadStream(tn) => {
                    let reqp = tn.reqp;

                    let response = inresp.plugin().data();
                    let status = response.get("status").ok_or_else(|| {
                        format_err!(
                            "missing status in a response on method = {}, transaction = {}",
                            reqp.method(),
                            inresp.transaction(),
                        )
                    })?;
                    if status != 200 {
                        return Err(format_err!(
                            "error received on method = {}, transaction = {}",
                            reqp.method(),
                            inresp.transaction()
                        ));
                    }

                    // TODO: deserialize response into struct
                    let rtc_id = response
                        .get("id")
                        .ok_or_else(|| {
                            format_err!(
                                "missing rtc id in a response on method = {}, transaction = {}",
                                reqp.method(),
                                inresp.transaction()
                            )
                        })?
                        .as_str()
                        .ok_or_else(|| {
                            format_err!(
                                "rtc_id is not a string on method = {}, transaction = {}",
                                reqp.method(),
                                inresp.transaction()
                            )
                        })?;
                    let rtc_id = uuid::Uuid::parse_str(rtc_id)?;

                    let conn = janus.db.get()?;

                    let rtc = rtc::FindQuery::new()
                        .id(rtc_id)
                        .execute(&conn)?
                        .ok_or_else(|| format_err!("the rtc = '{}' is not found", &rtc_id))?;

                    let room = room::FindQuery::new()
                        .id(rtc.room_id())
                        .execute(&conn)?
                        .ok_or_else(|| {
                            format_err!("a room for rtc = '{}' is not found", &rtc.id())
                        })?;

                    let raw_value = response
                        .get("time")
                        .ok_or_else(|| {
                            format_err!(
                                "missing time in a response on method = {}, transaction = {}",
                                reqp.method(),
                                inresp.transaction()
                            )
                        })?
                        .clone();
                    let mut start_stop_timestamps: Vec<(u64, u64)> =
                        serde_json::from_value(raw_value)?;

                    let start_stop_timestamps = start_stop_timestamps
                        .into_iter()
                        .map(|(start, end)| {
                            let start_secs = start as i64 / 1000;
                            let start_nanos = ((start % 1000) * 1_000_000) as u32;
                            let start = Bound::Included(DateTime::<Utc>::from_utc(
                                NaiveDateTime::from_timestamp(start_secs, start_nanos),
                                Utc,
                            ));

                            let end_secs = end as i64 / 1000;
                            let end_nanos = ((end % 1000) * 1_000_000) as u32;
                            let end = Bound::Included(DateTime::<Utc>::from_utc(
                                NaiveDateTime::from_timestamp(end_secs, end_nanos),
                                Utc,
                            ));

                            (start, end)
                        })
                        .collect();

                    recording::InsertQuery::new(rtc_id, start_stop_timestamps).execute(&conn)?;

                    use diesel::prelude::*;

                    let rtcs: Vec<rtc::Object> = rtc::Object::belonging_to(&room).load(&conn)?;
                    let recordings: Vec<recording::Object> =
                        recording::Object::belonging_to(&rtcs).load(&conn)?;
                    let recordings = recordings.grouped_by(&rtcs);

                    if recordings.iter().any(|r| r.is_empty()) {
                        info!(
                            "Some rtcs is not uploaded for room with Id = {} yet, so not sending 'room.upload' event",
                            room.id()
                        );
                        return Ok(());
                    }

                    let rtcs_and_recordings = rtcs.into_iter().zip(recordings);
                    let store_event = super::system::upload_event(room, rtcs_and_recordings)?;
                    store_event.into_envelope()?.publish(tx)
                }
                // An unsupported incoming Event message has been received
                _ => Err(format_err!(
                    "received an unexpected Event message: {:?}",
                    inresp,
                )),
            }
        }
        IncomingMessage::Error(ErrorResponse::Session(ref inresp)) => Err(format_err!(
            "received an unexpected Error message (session): {:?}",
            inresp,
        )),
        IncomingMessage::Error(ErrorResponse::Handle(ref inresp)) => Err(format_err!(
            "received an unexpected Error message (handle): {:?}",
            inresp,
        )),
        IncomingMessage::WebRtcUp(ref inev) => {
            let conn = janus.db.get()?;

            // Updating Rtc State
            // TODO: replace with one query
            // Could've implemented in one query using '.single_value()'
            // for the first select statement. The problem is that its
            // return value is always 'Nullable' when the 'rtc_id' value
            // for the following statement can't be null.
            let session_id = inev.session_id();
            let location_id = message.properties().as_agent_id();
            let location = location::FindQuery::new()
                .handle_id(inev.sender())
                .location_id(&location_id)
                .execute(&conn)?
                .ok_or_else(|| {
                    format_err!(
                        "session = '{}' within location = '{}' is not found",
                        session_id,
                        &location_id,
                    )
                })?;

            match rtc::update_state(location.rtc_id(), location.reply_to(), &conn) {
                // webrtcup came from publisher, so send event.
                Ok(rtc) => {
                    let event = crate::app::rtc::update_event(rtc);
                    event.into_envelope()?.publish(tx)
                }
                // webrtcup came from subscriber, so ignore.
                Err(_) => Ok(()),
            }
        }
        IncomingMessage::HangUp(ref inev) => {
            let conn = janus.db.get()?;

            // Deleting Rtc State
            // TODO: replace with one query
            // Could've implemented in one query using '.single_value()'
            // for the first select statement. The problem is that its
            // return value is always 'Nullable' when the 'rtc_id' value
            // for the following statement can't be null.
            let session_id = inev.session_id();
            let agent_id = message.properties().as_agent_id();
            let location = location::FindQuery::new()
                .handle_id(inev.sender())
                .session_id(session_id)
                .execute(&conn)?
                .ok_or_else(|| {
                    format_err!(
                        "session = '{}' within location = '{}' is not found",
                        session_id,
                        agent_id,
                    )
                })?;

            match rtc::delete_state(location.rtc_id(), location.reply_to(), &conn) {
                // Hangup came from publisher, so send update event.
                Ok(rtc) => {
                    let event = crate::app::rtc::update_event(rtc);
                    event.into_envelope()?.publish(tx)
                }
                // Hangup came from subscriber, so ignore.
                Err(_) => Ok(()),
            }
        }
        IncomingMessage::Media(ref inev) => Err(format_err!(
            "received an unexpected Media message: {:?}",
            inev,
        )),
        IncomingMessage::Timeout(ref inev) => Err(format_err!(
            "received an unexpected Timeout message: {:?}",
            inev,
        )),
        IncomingMessage::SlowLink(ref inev) => Err(format_err!(
            "received an unexpected SlowLink message: {:?}",
            inev,
        )),
    }
}
