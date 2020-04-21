use std::str::FromStr;

use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use failure::{err_msg, format_err, Error};
use log::{info, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::ops::Bound;
use std::sync::Arc;
use svc_agent::mqtt::{
    compat::{into_event, into_response, IncomingEnvelope},
    IncomingEventProperties, IncomingRequestProperties, IncomingResponseProperties,
    IntoPublishableDump, OutgoingRequest, OutgoingRequestProperties, ResponseStatus,
    ShortTermTimingProperties, SubscriptionTopic, TrackingProperties,
};
use svc_agent::{Addressable, AgentId, Subscription};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
use crate::backend::janus::{
    CreateHandleRequest, CreateSessionRequest, ErrorResponse, IncomingEvent, IncomingResponse,
    MessageRequest, StatusEvent, TrickleRequest, JANUS_API_VERSION,
};
use crate::db::{janus_backend, janus_rtc_stream, recording, room, rtc, ConnectionPool};
use crate::util::{from_base64, generate_correlation_data, to_base64};

////////////////////////////////////////////////////////////////////////////////

const STREAM_UPLOAD_METHOD: &str = "stream.upload";

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Transaction {
    CreateSession(CreateSessionTransaction),
    CreateHandle(CreateHandleTransaction),
    CreateRtcHandle(CreateRtcHandleTransaction),
    CreateStream(CreateStreamTransaction),
    ReadStream(ReadStreamTransaction),
    UploadStream(UploadStreamTransaction),
    Trickle(TrickleTransaction),
    AgentLeave(AgentLeaveTransaction),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateSessionTransaction {}

impl CreateSessionTransaction {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

pub(crate) fn create_session_request<M>(
    evp: &IncomingEventProperties,
    me: &M,
    start_timestamp: DateTime<Utc>,
) -> Result<OutgoingRequest<CreateSessionRequest>, Error>
where
    M: Addressable,
{
    let to = evp.as_agent_id();
    let transaction = Transaction::CreateSession(CreateSessionTransaction::new());
    let payload = CreateSessionRequest::new(&to_base64(&transaction)?);

    let mut props = OutgoingRequestProperties::new(
        "janus_session.create",
        &response_topic(to, me)?,
        &generate_correlation_data(),
        ShortTermTimingProperties::until_now(start_timestamp),
    );

    props.set_tracking(evp.tracking().to_owned());
    Ok(OutgoingRequest::unicast(
        payload,
        props,
        to,
        JANUS_API_VERSION,
    ))
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateHandleTransaction {
    session_id: i64,
}

impl CreateHandleTransaction {
    pub(crate) fn new(session_id: i64) -> Self {
        Self { session_id }
    }
}

pub(crate) fn create_handle_request<M>(
    respp: &IncomingResponseProperties,
    session_id: i64,
    me: &M,
    start_timestamp: DateTime<Utc>,
) -> Result<OutgoingRequest<CreateHandleRequest>, Error>
where
    M: Addressable,
{
    let to = respp.as_agent_id();
    let transaction = Transaction::CreateHandle(CreateHandleTransaction::new(session_id));

    let payload = CreateHandleRequest::new(
        &to_base64(&transaction)?,
        session_id,
        "janus.plugin.conference",
        None,
    );

    let mut props = OutgoingRequestProperties::new(
        "janus_handle.create",
        &response_topic(to, me)?,
        &generate_correlation_data(),
        ShortTermTimingProperties::until_now(start_timestamp),
    );

    props.set_tracking(respp.tracking().to_owned());
    Ok(OutgoingRequest::unicast(
        payload,
        props,
        to,
        JANUS_API_VERSION,
    ))
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateRtcHandleTransaction {
    reqp: IncomingRequestProperties,
    rtc_stream_id: Uuid,
    rtc_id: Uuid,
    session_id: i64,
}

impl CreateRtcHandleTransaction {
    pub(crate) fn new(
        reqp: IncomingRequestProperties,
        rtc_stream_id: Uuid,
        rtc_id: Uuid,
        session_id: i64,
    ) -> Self {
        Self {
            reqp,
            rtc_stream_id,
            rtc_id,
            session_id,
        }
    }
}

pub(crate) fn create_rtc_handle_request<A, M>(
    reqp: IncomingRequestProperties,
    rtc_handle_id: Uuid,
    rtc_id: Uuid,
    session_id: i64,
    to: &A,
    me: &M,
    start_timestamp: DateTime<Utc>,
    authz_time: Duration,
) -> Result<OutgoingRequest<CreateHandleRequest>, Error>
where
    A: Addressable,
    M: Addressable,
{
    let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    short_term_timing.set_authorization_time(authz_time);

    let props = reqp.to_request(
        "janus_handle.create",
        &response_topic(to, me)?,
        &generate_correlation_data(),
        short_term_timing,
    );

    let transaction = Transaction::CreateRtcHandle(CreateRtcHandleTransaction::new(
        reqp,
        rtc_handle_id,
        rtc_id,
        session_id,
    ));

    let payload = CreateHandleRequest::new(
        &to_base64(&transaction)?,
        session_id,
        "janus.plugin.conference",
        Some(&rtc_handle_id.to_string()),
    );

    Ok(OutgoingRequest::unicast(
        payload,
        props,
        to,
        JANUS_API_VERSION,
    ))
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
    agent_id: AgentId,
}

impl CreateStreamRequestBody {
    pub(crate) fn new(id: Uuid, agent_id: AgentId) -> Self {
        Self {
            method: "stream.create",
            id,
            agent_id,
        }
    }
}

pub(crate) fn create_stream_request<A, M>(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    rtc_id: Uuid,
    jsep: JsonValue,
    to: &A,
    me: &M,
    start_timestamp: DateTime<Utc>,
    authz_time: Duration,
) -> Result<OutgoingRequest<MessageRequest>, Error>
where
    A: Addressable,
    M: Addressable,
{
    let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    short_term_timing.set_authorization_time(authz_time);

    let props = reqp.to_request(
        "janus_conference_stream.create",
        &response_topic(to, me)?,
        &generate_correlation_data(),
        short_term_timing,
    );

    let agent_id = reqp.as_agent_id().to_owned();
    let body = CreateStreamRequestBody::new(rtc_id, agent_id);
    let transaction = Transaction::CreateStream(CreateStreamTransaction::new(reqp));

    let payload = MessageRequest::new(
        &to_base64(&transaction)?,
        session_id,
        handle_id,
        serde_json::to_value(&body)?,
        Some(jsep),
    );

    Ok(OutgoingRequest::unicast(
        payload,
        props,
        to,
        JANUS_API_VERSION,
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
    agent_id: AgentId,
}

impl ReadStreamRequestBody {
    pub(crate) fn new(id: Uuid, agent_id: AgentId) -> Self {
        Self {
            method: "stream.read",
            id,
            agent_id,
        }
    }
}

pub(crate) fn read_stream_request<A, M>(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    rtc_id: Uuid,
    jsep: JsonValue,
    to: &A,
    me: &M,
    start_timestamp: DateTime<Utc>,
    authz_time: Duration,
) -> Result<OutgoingRequest<MessageRequest>, Error>
where
    A: Addressable,
    M: Addressable,
{
    let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    short_term_timing.set_authorization_time(authz_time);

    let props = reqp.to_request(
        "janus_conference_stream.create",
        &response_topic(to, me)?,
        &generate_correlation_data(),
        short_term_timing,
    );

    let agent_id = reqp.as_agent_id().to_owned();
    let body = ReadStreamRequestBody::new(rtc_id, agent_id);
    let transaction = Transaction::ReadStream(ReadStreamTransaction::new(reqp));

    let payload = MessageRequest::new(
        &to_base64(&transaction)?,
        session_id,
        handle_id,
        serde_json::to_value(&body)?,
        Some(jsep),
    );

    Ok(OutgoingRequest::unicast(
        payload,
        props,
        to,
        JANUS_API_VERSION,
    ))
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UploadStreamTransaction {
    rtc_id: Uuid,
}

impl UploadStreamTransaction {
    pub(crate) fn new(rtc_id: Uuid) -> Self {
        Self { rtc_id }
    }

    pub(crate) fn method(&self) -> &str {
        STREAM_UPLOAD_METHOD
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
            method: STREAM_UPLOAD_METHOD,
            id,
            bucket: bucket.to_owned(),
            object: object.to_owned(),
        }
    }
}

pub(crate) fn upload_stream_request<A, M>(
    reqp: &IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    body: UploadStreamRequestBody,
    to: &A,
    me: &M,
    start_timestamp: DateTime<Utc>,
) -> Result<OutgoingRequest<MessageRequest>, Error>
where
    A: Addressable,
    M: Addressable,
{
    let transaction = Transaction::UploadStream(UploadStreamTransaction::new(body.id));

    let payload = MessageRequest::new(
        &to_base64(&transaction)?,
        session_id,
        handle_id,
        serde_json::to_value(&body)?,
        None,
    );

    let mut props = OutgoingRequestProperties::new(
        "janus_conference_stream.upload",
        &response_topic(to, me)?,
        &generate_correlation_data(),
        ShortTermTimingProperties::until_now(start_timestamp),
    );

    props.set_tracking(reqp.tracking().to_owned());

    Ok(OutgoingRequest::unicast(
        payload,
        props,
        to,
        JANUS_API_VERSION,
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

pub(crate) fn trickle_request<A, M>(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    jsep: JsonValue,
    to: &A,
    me: &M,
    start_timestamp: DateTime<Utc>,
    authz_time: Duration,
) -> Result<OutgoingRequest<TrickleRequest>, Error>
where
    A: Addressable,
    M: Addressable,
{
    let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    short_term_timing.set_authorization_time(authz_time);

    let props = reqp.to_request(
        "janus_trickle.create",
        &response_topic(to, me)?,
        &generate_correlation_data(),
        short_term_timing,
    );

    let transaction = Transaction::Trickle(TrickleTransaction::new(reqp));
    let payload = TrickleRequest::new(&to_base64(&transaction)?, session_id, handle_id, jsep);
    Ok(OutgoingRequest::unicast(
        payload,
        props,
        to,
        JANUS_API_VERSION,
    ))
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct AgentLeaveTransaction {
    evp: IncomingEventProperties,
}

impl AgentLeaveTransaction {
    pub(crate) fn new(evp: IncomingEventProperties) -> Self {
        Self { evp }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct AgentLeaveRequestBody {
    method: &'static str,
    agent_id: AgentId,
}

impl AgentLeaveRequestBody {
    pub(crate) fn new(agent_id: AgentId) -> Self {
        Self {
            method: "agent.leave",
            agent_id,
        }
    }
}

pub(crate) fn agent_leave_request<M>(
    evp: IncomingEventProperties,
    session_id: i64,
    handle_id: i64,
    agent_id: &AgentId,
    me: &M,
    tracking: &TrackingProperties,
) -> Result<OutgoingRequest<MessageRequest>, Error>
where
    M: Addressable,
{
    let to = evp.as_agent_id().to_owned();

    let mut props = OutgoingRequestProperties::new(
        "janus_conference_agent.leave",
        &response_topic(&to, me)?,
        &generate_correlation_data(),
        ShortTermTimingProperties::new(Utc::now()),
    );

    props.set_tracking(tracking.to_owned());

    let transaction = Transaction::AgentLeave(AgentLeaveTransaction::new(evp));
    let body = AgentLeaveRequestBody::new(agent_id.to_owned());

    let payload = MessageRequest::new(
        &to_base64(&transaction)?,
        session_id,
        handle_id,
        serde_json::to_value(&body)?,
        None,
    );

    Ok(OutgoingRequest::unicast(
        payload,
        props,
        &to,
        JANUS_API_VERSION,
    ))
}

fn response_topic<T, M>(to: &T, me: &M) -> Result<String, Error>
where
    T: Addressable,
    M: Addressable,
{
    Subscription::unicast_responses_from(to)
        .subscription_topic(me, JANUS_API_VERSION)
        .map_err(|err| format_err!("Failed to build subscription topic: {}", err))
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

pub(crate) async fn handle_response<M>(
    payload: Arc<Vec<u8>>,
    janus: Arc<State>,
    start_timestamp: DateTime<Utc>,
    me: M,
) -> Result<Vec<Box<dyn IntoPublishableDump>>, Error>
where
    M: Addressable,
{
    use endpoint::handle_error;

    let envelope = serde_json::from_slice::<IncomingEnvelope>(payload.as_slice())?;
    let message = into_response::<IncomingResponse>(envelope)?;
    match message.payload() {
        IncomingResponse::Success(ref inresp) => {
            match from_base64::<Transaction>(&inresp.transaction())? {
                // Session has been created
                Transaction::CreateSession(_tn) => {
                    // Creating Handle
                    let backreq = create_handle_request(
                        message.properties(),
                        inresp.data().id(),
                        &me,
                        start_timestamp,
                    )?;

                    Ok(vec![Box::new(backreq) as Box<dyn IntoPublishableDump>])
                }
                // Handle has been created
                Transaction::CreateHandle(tn) => {
                    let backend_id = message.properties().as_agent_id();
                    let handle_id = inresp.data().id();

                    let conn = janus.db.get()?;
                    let _ = janus_backend::UpdateQuery::new(backend_id, handle_id, tn.session_id)
                        .execute(&conn)?;

                    Ok(vec![])
                }
                // Rtc Handle has been created
                Transaction::CreateRtcHandle(tn) => {
                    let agent_id = message.properties().as_agent_id();
                    let reqp = tn.reqp;

                    // Returning Real-Time connection handle
                    let resp = endpoint::rtc::ConnectResponse::unicast(
                        endpoint::rtc::ConnectResponseData::new(
                            endpoint::rtc_signal::HandleId::new(
                                tn.rtc_stream_id,
                                tn.rtc_id,
                                inresp.data().id(),
                                tn.session_id,
                                agent_id.clone(),
                            ),
                        ),
                        reqp.to_response(
                            ResponseStatus::OK,
                            ShortTermTimingProperties::until_now(start_timestamp),
                        ),
                        &reqp,
                        JANUS_API_VERSION,
                    );

                    Ok(vec![Box::new(resp) as Box<dyn IntoPublishableDump>])
                }
                // An unsupported incoming Success message has been received
                _ => Err(format_err!(
                    "received an unexpected Success message: {:?}",
                    inresp,
                )),
            }
        }
        IncomingResponse::Ack(ref inresp) => {
            match from_base64::<Transaction>(&inresp.transaction())? {
                // Conference Stream is being created
                Transaction::CreateStream(_tn) => Err(format_err!(
                    "received an unexpected Ack message (stream.create): {:?}",
                    inresp,
                )),
                // Trickle message has been received by Janus Gateway
                Transaction::Trickle(tn) => {
                    let resp = endpoint::rtc_signal::CreateResponse::unicast(
                        endpoint::rtc_signal::CreateResponseData::new(None),
                        tn.reqp.to_response(
                            ResponseStatus::OK,
                            ShortTermTimingProperties::until_now(start_timestamp),
                        ),
                        tn.reqp.as_agent_id(),
                        JANUS_API_VERSION,
                    );

                    Ok(vec![Box::new(resp) as Box<dyn IntoPublishableDump>])
                }
                // An unsupported incoming Ack message has been received
                _ => Err(format_err!(
                    "received an unexpected Ack message: {:?}",
                    inresp,
                )),
            }
        }
        IncomingResponse::Event(ref inresp) => {
            match from_base64::<Transaction>(&inresp.transaction())? {
                // Conference Stream has been created (an answer received)
                Transaction::CreateStream(ref tn) => {
                    // TODO: improve error handling
                    let plugin_data = inresp.plugin().data();
                    let next = plugin_data
                        .get("status")
                        .ok_or_else(|| {
                            // We fail if response doesn't contain a status
                            SvcError::builder()
                                .status(ResponseStatus::FAILED_DEPENDENCY)
                                .detail(&format!(
                                    "missing 'status' in a response on method = '{}', transaction = '{}'",
                                    tn.reqp.method(),
                                    inresp.transaction()
                                ))
                                .build()
                        })
                        .and_then(|status| match status {
                            // We fail if the status isn't equal to 200
                            val if val == "200" => Ok(()),
                            _ => Err(SvcError::builder()
                                .status(ResponseStatus::FAILED_DEPENDENCY)
                                .detail(&format!(
                                    "error received on method = '{}', transaction = '{}'",
                                    tn.reqp.method(),
                                    inresp.transaction()
                                ))
                                .build()),
                        })
                        .and_then(|_| {
                            // Getting answer (as JSEP)
                            let jsep = inresp.jsep().ok_or_else(|| {
                                SvcError::builder()
                                    .status(ResponseStatus::FAILED_DEPENDENCY)
                                    .detail(&format!(
                                        "missing 'jsep' in a response on method = '{}', transaction = '{}'",
                                        tn.reqp.method(),
                                        inresp.transaction(),
                                    ))
                                    .build()
                            })?;

                            let resp = endpoint::rtc_signal::CreateResponse::unicast(
                                endpoint::rtc_signal::CreateResponseData::new(Some(jsep.clone())),
                                tn.reqp.to_response(ResponseStatus::OK, ShortTermTimingProperties::until_now(start_timestamp)),
                                tn.reqp.as_agent_id(),
                                JANUS_API_VERSION,
                            );

                            Ok(vec![Box::new(resp) as Box<dyn IntoPublishableDump>])
                        });

                    next.or_else(|err| {
                        handle_error(
                            "error:janus_stream.create",
                            "Error creating a Janus Conference Stream",
                            &tn.reqp,
                            err,
                            start_timestamp,
                        )
                    })
                }
                // Conference Stream has been read (an answer received)
                Transaction::ReadStream(ref tn) => {
                    // TODO: improve error handling
                    let plugin_data = inresp.plugin().data();
                    let next = plugin_data
                        .get("status")
                        .ok_or_else(|| {
                            // We fail if response doesn't contain a status
                            SvcError::builder()
                                .status(ResponseStatus::FAILED_DEPENDENCY)
                                .detail(&format!(
                                    "missing 'status' in a response on method = '{}', transaction = '{}'",
                                    tn.reqp.method(),
                                    inresp.transaction()
                                ))
                                .build()
                        })
                        // We fail if the status isn't equal to 200
                        .and_then(|status| match status {
                            val if val == "200" => Ok(()),
                            _ => Err(SvcError::builder()
                                .status(ResponseStatus::FAILED_DEPENDENCY)
                                .detail(&format!(
                                    "error received on method = '{}', transaction = '{}'",
                                    tn.reqp.method(),
                                    inresp.transaction()
                                ))
                                .build()),
                        })
                        .and_then(|_| {
                            // Getting answer (as JSEP)
                            let jsep = inresp.jsep().ok_or_else(|| {
                                SvcError::builder()
                                    .status(ResponseStatus::FAILED_DEPENDENCY)
                                    .detail(&format!(
                                        "missing 'jsep' in a response on method = '{}', transaction = '{}'",
                                        tn.reqp.method(),
                                        inresp.transaction(),
                                    ))
                                    .build()
                            })?;

                            let resp = endpoint::rtc_signal::CreateResponse::unicast(
                                endpoint::rtc_signal::CreateResponseData::new(Some(jsep.clone())),
                                tn.reqp.to_response(ResponseStatus::OK, ShortTermTimingProperties::until_now(start_timestamp)),
                                tn.reqp.as_agent_id(),
                                JANUS_API_VERSION,
                            );

                            Ok(vec![Box::new(resp) as Box<dyn IntoPublishableDump>])
                        });

                    next.or_else(|err| {
                        handle_error(
                            "error:janus_stream.read",
                            "Error reading a Janus Conference Stream",
                            &tn.reqp,
                            err,
                            start_timestamp,
                        )
                    })
                }
                // Conference Stream has been uploaded to a storage backend (a confirmation)
                Transaction::UploadStream(ref tn) => {
                    // TODO: improve error handling
                    let plugin_data = inresp.plugin().data();

                    plugin_data
                        .get("status")
                        .ok_or_else(|| {
                            // We fail if response doesn't contain a status
                            format_err!(
                                "missing 'status' in a response on method = '{}', transaction = '{}'",
                                tn.method(),
                                inresp.transaction(),
                            )
                        })
                        // We fail if the status isn't equal to 200
                        .and_then(|status| match status {
                            val if val == "200" => Ok(()),
                            val if val == "404" => {
                                let conn = janus.db.get()?;

                                recording::InsertQuery::new(tn.rtc_id, recording::Status::Missing)
                                    .execute(&conn)?;

                                Err(format_err!(
                                    "janus is missing recording for rtc = '{}', method = '{}', transaction = '{}'",
                                    tn.rtc_id,
                                    tn.method(),
                                    inresp.transaction(),
                                ))
                            }
                            _ => Err(format_err!(
                                "error with status code = '{}' received in a response on method = '{}', transaction = '{}'",
                                status,
                                tn.method(),
                                inresp.transaction(),
                            ))
                        })
                        .and_then(|_| {
                            let rtc_id = plugin_data
                                .get("id")
                                .ok_or_else(|| {
                                    format_err!(
                                        "Missing 'id' in response on method = '{}', transaction = '{}'",
                                        tn.method(),
                                        inresp.transaction()
                                    )
                                })
                                .and_then(|val| {
                                    serde_json::from_value::<Uuid>(val.clone())
                                        .map_err(|_| err_msg("invalid value for 'id'"))
                                })?;

                            let started_at = plugin_data
                                .get("started_at")
                                .ok_or_else(|| {
                                    format_err!(
                                        "Missing 'started_at' in response on method = '{}', transaction = '{}'",
                                        tn.method(),
                                        inresp.transaction()
                                    )
                                })
                                .and_then(|val| {
                                    let unix_ts = serde_json::from_value::<u64>(val.clone())
                                        .map_err(|_| err_msg("invalid value for 'started_at'"))?;

                                    let naive_datetime = NaiveDateTime::from_timestamp(
                                        unix_ts as i64 / 1000,
                                        ((unix_ts % 1000) * 1_000_000) as u32,
                                    );

                                    Ok(DateTime::<Utc>::from_utc(naive_datetime, Utc))
                                })?;

                            let segments = plugin_data
                                .get("time")
                                .ok_or_else(|| {
                                    format_err!(
                                        "missing 'time' in a response on method = '{}', transaction = '{}'",
                                        tn.method(),
                                        inresp.transaction(),
                                    )
                                })
                                .and_then(|segments| {
                                    Ok(serde_json::from_value::<Vec<(i64, i64)>>(segments.clone())
                                        .map_err(|_| err_msg("invalid value for 'time'"))?
                                        .into_iter()
                                        .map(|(start, end)| {
                                            (Bound::Included(start), Bound::Excluded(end))
                                        })
                                        .collect())
                                })?;

                            let (room, rtcs, recs) = {
                                let conn = janus.db.get()?;

                                recording::InsertQuery::new(rtc_id, recording::Status::Ready)
                                    .started_at(started_at)
                                    .segments(segments)
                                    .execute(&conn)?;

                                let rtc = rtc::FindQuery::new()
                                    .id(rtc_id)
                                    .execute(&conn)?
                                    .ok_or_else(|| {
                                        format_err!(
                                            "the rtc = '{}' is not found on method = '{}', transaction = '{}'",
                                            rtc_id,
                                            tn.method(),
                                            inresp.transaction(),
                                        )
                                    })?;

                                let room = room::FindQuery::new()
                                    .id(rtc.room_id())
                                    .execute(&conn)?
                                    .ok_or_else(|| {
                                        format_err!(
                                            "the room = '{}' is not found on method = '{}', transaction = '{}'",
                                            rtc.room_id(),
                                            tn.method(),
                                            inresp.transaction(),
                                        )
                                    })?;

                                // TODO: move to db module
                                use diesel::prelude::*;
                                let rtcs = rtc::Object::belonging_to(&room).load(&conn)?;
                                let recs = recording::Object::belonging_to(&rtcs).load(&conn)?.grouped_by(&rtcs);

                                (room, rtcs, recs)
                            };

                            let maybe_rtcs_and_recordings: Option<Vec<(rtc::Object, recording::Object)>> = rtcs
                                .into_iter()
                                .zip(recs)
                                .map(|(rtc, rtc_recs)| {
                                    if rtc_recs.len() > 1 {
                                        warn!(
                                            "there must be at most 1 recording for an rtc, got {} for the room = '{}', rtc = '{}'; using the first one, ignoring the rest",
                                            rtc_recs.len(),
                                            room.id(),
                                            rtc.id(),
                                        );
                                    }

                                    rtc_recs.into_iter().next().map(|rec| (rtc, rec))
                                })
                                .collect();

                            match maybe_rtcs_and_recordings {
                                Some(rtcs_and_recordings) => {
                                    let event = endpoint::system::upload_event(
                                        &room,
                                        rtcs_and_recordings.into_iter(),
                                        start_timestamp,
                                        message.properties().tracking(),
                                    ).map_err(|e| {
                                        format_err!("error creating a system event, {}", e)
                                    })?;

                                    Ok(vec![Box::new(event) as Box<dyn IntoPublishableDump>])
                                }
                                None => {
                                    // Waiting for all the room's rtc being uploaded
                                    info!(
                                        "postpone 'room.upload' event because still waiting for rtcs being uploaded for the room = '{}'",
                                        room.id(),
                                    );

                                    Ok(vec![])
                                }
                            }
                        })
                }
                // An unsupported incoming Event message has been received
                _ => Err(format_err!(
                    "received an unexpected Event message: {:?}",
                    inresp,
                )),
            }
        }
        IncomingResponse::Error(ErrorResponse::Session(ref inresp)) => Err(format_err!(
            "received an unexpected Error message (session): {:?}",
            inresp,
        )),
        IncomingResponse::Error(ErrorResponse::Handle(ref inresp)) => Err(format_err!(
            "received an unexpected Error message (handle): {:?}",
            inresp,
        )),
    }
}

pub(crate) async fn handle_event(
    payload: Arc<Vec<u8>>,
    janus: Arc<State>,
    start_timestamp: DateTime<Utc>,
) -> Result<Vec<Box<dyn IntoPublishableDump>>, Error> {
    let envelope = serde_json::from_slice::<IncomingEnvelope>(payload.as_slice())?;
    let message = into_event::<IncomingEvent>(envelope)?;

    match message.payload() {
        IncomingEvent::WebRtcUp(ref inev) => {
            let rtc_stream_id = Uuid::from_str(inev.opaque_id())?;
            let conn = janus.db.get()?;

            // If the event relates to a publisher's handle,
            // we will find the corresponding stream and send event w/ updated stream object
            // to the room's topic.
            if let Some(rtc_stream) = janus_rtc_stream::start(rtc_stream_id, &conn)? {
                let rtc_id = rtc_stream.rtc_id();

                let room = room::FindQuery::new()
                    .time(room::now())
                    .rtc_id(rtc_id)
                    .execute(&conn)?
                    .ok_or_else(|| format_err!("a room for rtc = '{}' is not found", &rtc_id))?;

                let event = endpoint::rtc_stream::update_event(
                    room.id(),
                    rtc_stream,
                    start_timestamp,
                    message.properties().tracking(),
                )?;

                Ok(vec![Box::new(event) as Box<dyn IntoPublishableDump>])
            } else {
                Ok(vec![])
            }
        }
        IncomingEvent::Detached(ref inev) => {
            let rtc_stream_id = Uuid::from_str(inev.opaque_id())?;
            let conn = janus.db.get()?;

            // If the event relates to a publisher's handle,
            // we will find the corresponding stream and send event w/ updated stream object
            // to the room's topic.
            if let Some(rtc_stream) = janus_rtc_stream::stop(rtc_stream_id, &conn)? {
                let rtc_id = rtc_stream.rtc_id();

                let room = room::FindQuery::new()
                    .time(room::now())
                    .rtc_id(rtc_id)
                    .execute(&conn)?
                    .ok_or_else(|| format_err!("a room for rtc = '{}' is not found", &rtc_id))?;

                // Publish the update event if only stream object has been changed
                // (if there weren't any actual media stream, the object won't contain its start time)
                if let Some(_) = rtc_stream.time() {
                    let event = endpoint::rtc_stream::update_event(
                        room.id(),
                        rtc_stream,
                        start_timestamp,
                        message.properties().tracking(),
                    )?;

                    return Ok(vec![Box::new(event) as Box<dyn IntoPublishableDump>]);
                }
            }

            Ok(vec![])
        }
        IncomingEvent::HangUp(_)
        | IncomingEvent::Media(_)
        | IncomingEvent::Timeout(_)
        | IncomingEvent::SlowLink(_) => {
            // Ignore these kinds of events.
            Ok(vec![])
        }
    }
}

pub(crate) async fn handle_status<M>(
    payload: Arc<Vec<u8>>,
    janus: Arc<State>,
    start_timestamp: DateTime<Utc>,
    me: M,
) -> Result<Vec<Box<dyn IntoPublishableDump>>, Error>
where
    M: Addressable,
{
    let envelope = serde_json::from_slice::<IncomingEnvelope>(payload.as_slice())?;
    let inev = into_event::<StatusEvent>(envelope)?;

    if let true = inev.payload().online() {
        let event = create_session_request(inev.properties(), &me, start_timestamp)?;
        Ok(vec![Box::new(event) as Box<dyn IntoPublishableDump>])
    } else {
        let conn = janus.db.get()?;
        let agent_id = inev.properties().as_agent_id();
        let _ = janus_backend::DeleteQuery::new(agent_id).execute(&conn)?;
        Ok(vec![])
    }
}
