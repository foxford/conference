use std::str::FromStr;

use anyhow::{Context as AnyhowContext, Result};
use async_std::stream;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use log::{error, info, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::ops::Bound;
use svc_agent::mqtt::{
    IncomingEvent as MQTTIncomingEvent, IncomingEventProperties, IncomingRequestProperties,
    IncomingResponse as MQTTIncomingResponse, IncomingResponseProperties, IntoPublishableMessage,
    OutgoingMessage, OutgoingRequest, OutgoingRequestProperties, OutgoingResponse, ResponseStatus,
    ShortTermTimingProperties, SubscriptionTopic, TrackingProperties,
};
use svc_agent::{Addressable, AgentId, Subscription};
use svc_error::{extension::sentry, Error as SvcError};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint;
use crate::app::handle_id::HandleId;
use crate::app::message_handler::{MessageStream, SvcErrorSugar};
use crate::app::API_VERSION;
use crate::backend::janus::{
    CreateHandleRequest, CreateSessionRequest, ErrorResponse, IncomingEvent, IncomingResponse,
    MessageRequest, OpaqueId, StatusEvent, TrickleRequest, JANUS_API_VERSION,
};
use crate::db::{janus_backend, janus_rtc_stream, recording, room, rtc};
use crate::util::{from_base64, generate_correlation_data, to_base64};

////////////////////////////////////////////////////////////////////////////////

const STREAM_UPLOAD_METHOD: &str = "stream.upload";

////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::large_enum_variant)]
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
pub(crate) struct CreateSessionTransaction {
    capacity: Option<i32>,
}

impl CreateSessionTransaction {
    pub(crate) fn new() -> Self {
        Self { capacity: None }
    }

    pub(crate) fn capacity(&self) -> Option<i32> {
        self.capacity
    }

    pub(crate) fn set_capacity(&mut self, capacity: i32) -> &mut Self {
        self.capacity = Some(capacity);
        self
    }
}

pub(crate) fn create_session_request<M>(
    payload: &StatusEvent,
    evp: &IncomingEventProperties,
    me: &M,
    start_timestamp: DateTime<Utc>,
) -> Result<OutgoingMessage<CreateSessionRequest>>
where
    M: Addressable,
{
    let to = evp.as_agent_id();
    let mut tn_data = CreateSessionTransaction::new();

    if let Some(capacity) = payload.capacity() {
        tn_data.set_capacity(capacity);
    }

    let transaction = Transaction::CreateSession(tn_data);
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
    capacity: Option<i32>,
}

impl CreateHandleTransaction {
    pub(crate) fn new(session_id: i64) -> Self {
        Self {
            session_id,
            capacity: None,
        }
    }

    pub(crate) fn capacity(&self) -> Option<i32> {
        self.capacity
    }

    pub(crate) fn set_capacity(&mut self, capacity: i32) -> &mut Self {
        self.capacity = Some(capacity);
        self
    }
}

pub(crate) fn create_handle_request<M>(
    respp: &IncomingResponseProperties,
    session_id: i64,
    capacity: Option<i32>,
    me: &M,
    start_timestamp: DateTime<Utc>,
) -> Result<OutgoingMessage<CreateHandleRequest>>
where
    M: Addressable,
{
    let to = respp.as_agent_id();
    let mut tn_data = CreateHandleTransaction::new(session_id);

    if let Some(capacity) = capacity {
        tn_data.set_capacity(capacity);
    }

    let transaction = Transaction::CreateHandle(tn_data);

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

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_rtc_handle_request<A, M>(
    reqp: IncomingRequestProperties,
    rtc_stream_id: Uuid,
    rtc_id: Uuid,
    session_id: i64,
    to: &A,
    me: &M,
    start_timestamp: DateTime<Utc>,
    authz_time: Duration,
) -> Result<OutgoingMessage<CreateHandleRequest>>
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
        rtc_stream_id,
        rtc_id,
        session_id,
    ));

    let payload = CreateHandleRequest::new(
        &to_base64(&transaction)?,
        session_id,
        "janus.plugin.conference",
        Some(&rtc_stream_id.to_string()),
    );

    Ok(OutgoingRequest::unicast(
        payload,
        props,
        to,
        JANUS_API_VERSION,
    ))
}

// ////////////////////////////////////////////////////////////////////////////////

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

#[allow(clippy::too_many_arguments)]
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
) -> Result<OutgoingMessage<MessageRequest>>
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

#[allow(clippy::too_many_arguments)]
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
) -> Result<OutgoingMessage<MessageRequest>>
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
) -> Result<OutgoingMessage<MessageRequest>>
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn trickle_request<A, M>(
    reqp: IncomingRequestProperties,
    session_id: i64,
    handle_id: i64,
    jsep: JsonValue,
    to: &A,
    me: &M,
    start_timestamp: DateTime<Utc>,
    authz_time: Duration,
) -> Result<OutgoingMessage<TrickleRequest>>
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

pub(crate) fn agent_leave_request<T, M>(
    evp: IncomingEventProperties,
    session_id: i64,
    handle_id: i64,
    agent_id: &AgentId,
    to: &T,
    me: &M,
    tracking: &TrackingProperties,
) -> Result<OutgoingMessage<MessageRequest>>
where
    T: Addressable,
    M: Addressable,
{
    let mut props = OutgoingRequestProperties::new(
        "janus_conference_agent.leave",
        &response_topic(to, me)?,
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
        to,
        JANUS_API_VERSION,
    ))
}

fn response_topic<T, M>(to: &T, me: &M) -> Result<String>
where
    T: Addressable,
    M: Addressable,
{
    Subscription::unicast_responses_from(to)
        .subscription_topic(me, JANUS_API_VERSION)
        .context("Failed to build subscription topic")
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn handle_response<C: Context>(
    context: &C,
    resp: &MQTTIncomingResponse<String>,
    start_timestamp: DateTime<Utc>,
) -> MessageStream {
    handle_response_impl(context, resp, start_timestamp)
        .await
        .unwrap_or_else(|err| {
            error!("Failed to handle a response from janus: {}", err);
            sentry::send(err).unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
            Box::new(stream::empty())
        })
}

async fn handle_response_impl<C: Context>(
    context: &C,
    resp: &MQTTIncomingResponse<String>,
    start_timestamp: DateTime<Utc>,
) -> Result<MessageStream, SvcError> {
    let payload = MQTTIncomingResponse::convert_payload::<IncomingResponse>(&resp)
        .map_err(|err| format!("failed to parse response: {}", err))
        .status(ResponseStatus::BAD_REQUEST)?;

    let respp = resp.properties();

    match payload {
        IncomingResponse::Success(ref inresp) => {
            let txn = from_base64::<Transaction>(&inresp.transaction())
                .map_err(|err| format!("failed to parse transaction: {}", err))
                .status(ResponseStatus::BAD_REQUEST)?;

            match txn {
                // Session has been created
                Transaction::CreateSession(tn) => {
                    // Creating Handle
                    let backreq = create_handle_request(
                        respp,
                        inresp.data().id(),
                        tn.capacity(),
                        context.agent_id(),
                        start_timestamp,
                    )
                    .map_err(|err| err.to_string())
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

                    let boxed_backreq = Box::new(backreq) as Box<dyn IntoPublishableMessage + Send>;
                    Ok(Box::new(stream::once(boxed_backreq)))
                }
                // Handle has been created
                Transaction::CreateHandle(tn) => {
                    let backend_id = respp.as_agent_id();
                    let handle_id = inresp.data().id();
                    let conn = context.db().get()?;

                    let mut q =
                        janus_backend::UpsertQuery::new(backend_id, handle_id, tn.session_id);

                    if let Some(capacity) = tn.capacity() {
                        q = q.capacity(capacity);
                    }

                    q.execute(&conn)?;
                    Ok(Box::new(stream::empty()))
                }
                // Rtc Handle has been created
                Transaction::CreateRtcHandle(tn) => {
                    let agent_id = respp.as_agent_id();
                    let reqp = tn.reqp;

                    // Returning Real-Time connection handle
                    let resp = endpoint::rtc::ConnectResponse::unicast(
                        endpoint::rtc::ConnectResponseData::new(HandleId::new(
                            tn.rtc_stream_id,
                            tn.rtc_id,
                            inresp.data().id(),
                            tn.session_id,
                            agent_id.clone(),
                        )),
                        reqp.to_response(
                            ResponseStatus::OK,
                            ShortTermTimingProperties::until_now(start_timestamp),
                        ),
                        &reqp,
                        JANUS_API_VERSION,
                    );

                    let boxed_resp = Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
                    Ok(Box::new(stream::once(boxed_resp)))
                }
                // An unsupported incoming Success message has been received
                _ => Ok(Box::new(stream::empty())),
            }
        }
        IncomingResponse::Ack(ref inresp) => {
            let txn = from_base64::<Transaction>(&inresp.transaction())
                .map_err(|err| format!("failed to parse transaction: {}", err))
                .status(ResponseStatus::BAD_REQUEST)?;

            match txn {
                // Conference Stream is being created
                Transaction::CreateStream(_tn) => Ok(Box::new(stream::empty())),
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

                    let boxed_resp = Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
                    Ok(Box::new(stream::once(boxed_resp)))
                }
                // An unsupported incoming Ack message has been received
                _ => Ok(Box::new(stream::empty())),
            }
        }
        IncomingResponse::Event(ref inresp) => {
            let txn = from_base64::<Transaction>(&inresp.transaction())
                .map_err(|err| format!("failed to parse transaction: {}", err))
                .status(ResponseStatus::BAD_REQUEST)?;

            match txn {
                // Conference Stream has been created (an answer received)
                Transaction::CreateStream(ref tn) => inresp
                    .plugin()
                    .data()
                    .get("status")
                    .ok_or_else(|| {
                        // We fail if response doesn't contain a status
                        format!(
                            "missing 'status' in a response on method = '{}', transaction = '{}'",
                            tn.reqp.method(),
                            inresp.transaction()
                        )
                    })
                    .status(ResponseStatus::FAILED_DEPENDENCY)
                    .and_then(|status| match status {
                        // We fail if the status isn't equal to 200
                        val if val == "200" => Ok(()),
                        status => {
                            let err = format!(
                                "error received on method = '{}', transaction = '{}', status = '{}'",
                                tn.reqp.method(),
                                inresp.transaction(),
                                status,
                            );

                            Err(err).status(ResponseStatus::FAILED_DEPENDENCY)
                        }
                    })
                    .and_then(|_| {
                        // Getting answer (as JSEP)
                        let jsep = inresp
                            .jsep()
                            .ok_or_else(|| format!(
                                "missing 'jsep' in a response on method = '{}', transaction = '{}'",
                                tn.reqp.method(),
                                inresp.transaction(),
                            ))
                            .status(ResponseStatus::FAILED_DEPENDENCY)?;

                        let resp = endpoint::rtc_signal::CreateResponse::unicast(
                            endpoint::rtc_signal::CreateResponseData::new(Some(jsep.clone())),
                            tn.reqp.to_response(ResponseStatus::OK, ShortTermTimingProperties::until_now(start_timestamp)),
                            tn.reqp.as_agent_id(),
                            JANUS_API_VERSION,
                        );

                        let boxed_resp = Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
                        Ok(Box::new(stream::once(boxed_resp)) as MessageStream)
                    })
                    .or_else(|err| {
                        Ok(handle_response_error(
                            "janus_stream.create",
                            "Error creating a Janus Conference Stream",
                            &tn.reqp,
                            err,
                            start_timestamp,
                        ))
                    }),
                // Conference Stream has been read (an answer received)
                Transaction::ReadStream(ref tn) => inresp
                    .plugin()
                    .data()
                    .get("status")
                    .ok_or_else(|| {
                        // We fail if response doesn't contain a status
                        format!(
                            "missing 'status' in a response on method = '{}', transaction = '{}'",
                            tn.reqp.method(),
                            inresp.transaction()
                        )
                    })
                    .status(ResponseStatus::FAILED_DEPENDENCY)
                    // We fail if the status isn't equal to 200
                    .and_then(|status| match status {
                        val if val == "200" => Ok(()),
                        _ => {
                            let err = format!(
                                "error received on method = '{}', transaction = '{}'",
                                tn.reqp.method(),
                                inresp.transaction()
                            );

                            Err(err).status(ResponseStatus::FAILED_DEPENDENCY)
                        }
                    })
                    .and_then(|_| {
                        // Getting answer (as JSEP)
                        let jsep = inresp
                            .jsep()
                            .ok_or_else(|| format!(
                                "missing 'jsep' in a response on method = '{}', transaction = '{}'",
                                tn.reqp.method(),
                                inresp.transaction()
                            ))
                            .status(ResponseStatus::FAILED_DEPENDENCY)?;

                        let resp = endpoint::rtc_signal::CreateResponse::unicast(
                            endpoint::rtc_signal::CreateResponseData::new(Some(jsep.clone())),
                            tn.reqp.to_response(ResponseStatus::OK, ShortTermTimingProperties::until_now(start_timestamp)),
                            tn.reqp.as_agent_id(),
                            JANUS_API_VERSION,
                        );

                        let boxed_resp = Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
                        Ok(Box::new(stream::once(boxed_resp)) as MessageStream)
                    })
                    .or_else(|err| {
                        Ok(handle_response_error(
                            "janus_stream.read",
                            "Error reading a Janus Conference Stream",
                            &tn.reqp,
                            err,
                            start_timestamp,
                        ))
                    }),
                // Conference Stream has been uploaded to a storage backend (a confirmation)
                Transaction::UploadStream(ref tn) => {
                    // TODO: improve error handling
                    let plugin_data = inresp.plugin().data();

                    plugin_data
                        .get("status")
                        .ok_or_else(|| {
                            // We fail if response doesn't contain a status
                            format!(
                                "missing 'status' in a response on method = '{}', transaction = '{}'",
                                tn.method(),
                                inresp.transaction(),
                            )
                        })
                        .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                        // We fail if the status isn't equal to 200
                        .and_then(|status| match status {
                            val if val == "200" => Ok(()),
                            val if val == "404" => {
                                let conn = context.db().get()?;

                                recording::InsertQuery::new(tn.rtc_id, recording::Status::Missing)
                                    .execute(&conn)?;

                                let err = format!(
                                    "janus is missing recording for rtc = '{}', method = '{}', transaction = '{}'",
                                    tn.rtc_id,
                                    tn.method(),
                                    inresp.transaction(),
                                );

                                Err(err).status(ResponseStatus::UNPROCESSABLE_ENTITY)
                            }
                            _ => {
                                let err = format!(
                                    "error with status code = '{}' received in a response on method = '{}', transaction = '{}'",
                                    status,
                                    tn.method(),
                                    inresp.transaction(),
                                );

                                Err(err).status(ResponseStatus::UNPROCESSABLE_ENTITY)
                            }
                        })
                        .and_then(|_| {
                            let rtc_id = plugin_data
                                .get("id")
                                .ok_or_else(|| format!(
                                    "Missing 'id' in response on method = '{}', transaction = '{}'",
                                    tn.method(),
                                    inresp.transaction()
                                ))
                                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                                .and_then(|val| {
                                    serde_json::from_value::<Uuid>(val.clone())
                                        .map_err(|_| String::from("invalid value for 'id'"))
                                        .status(ResponseStatus::BAD_REQUEST)
                                })?;

                            let started_at = plugin_data
                                .get("started_at")
                                .ok_or_else(|| format!(
                                    "Missing 'started_at' in response on method = '{}', transaction = '{}'",
                                    tn.method(),
                                    inresp.transaction()
                                ))
                                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                                .and_then(|val| {
                                    let unix_ts = serde_json::from_value::<u64>(val.clone())
                                        .map_err(|_| String::from("invalid value for 'started_at'"))
                                        .status(ResponseStatus::BAD_REQUEST)?;

                                    let naive_datetime = NaiveDateTime::from_timestamp(
                                        unix_ts as i64 / 1000,
                                        ((unix_ts % 1000) * 1_000_000) as u32,
                                    );

                                    Ok(DateTime::<Utc>::from_utc(naive_datetime, Utc))
                                })?;

                            let segments = plugin_data
                                .get("time")
                                .ok_or_else(|| format!(
                                    "missing 'time' in a response on method = '{}', transaction = '{}'",
                                    tn.method(),
                                    inresp.transaction(),
                                ))
                                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                                .and_then(|segments| {
                                    Ok(serde_json::from_value::<Vec<(i64, i64)>>(segments.clone())
                                        .map_err(|_| "invalid value for 'time'")
                                        .status(ResponseStatus::UNPROCESSABLE_ENTITY)?
                                        .into_iter()
                                        .map(|(start, end)| {
                                            (Bound::Included(start), Bound::Excluded(end))
                                        })
                                        .collect())
                                })?;

                            let (room, rtcs, recs) = {
                                let conn = context.db().get()?;

                                recording::InsertQuery::new(rtc_id, recording::Status::Ready)
                                    .started_at(started_at)
                                    .segments(segments)
                                    .execute(&conn)?;

                                let rtc = rtc::FindQuery::new()
                                    .id(rtc_id)
                                    .execute(&conn)?
                                    .ok_or_else(|| format!(
                                        "the rtc = '{}' is not found on method = '{}', transaction = '{}'",
                                        rtc_id,
                                        tn.method(),
                                        inresp.transaction(),
                                    ))
                                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

                                let room = room::FindQuery::new()
                                    .id(rtc.room_id())
                                    .execute(&conn)?
                                    .ok_or_else(|| format!(
                                        "the room = '{}' is not found on method = '{}', transaction = '{}'",
                                        rtc.room_id(),
                                        tn.method(),
                                        inresp.transaction(),
                                    ))
                                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

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
                                        respp.tracking(),
                                    )
                                    .map_err(|e| format!("error creating a system event, {}", e))
                                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

                                    let event_box = Box::new(event) as Box<dyn IntoPublishableMessage + Send>;
                                    Ok(Box::new(stream::once(event_box)) as MessageStream)
                                }
                                None => {
                                    // Waiting for all the room's rtc being uploaded
                                    info!(
                                        "postpone 'room.upload' event because still waiting for rtcs being uploaded for the room = '{}'",
                                        room.id(),
                                    );

                                    Ok(Box::new(stream::empty()) as MessageStream)
                                }
                            }
                        })
                }
                // An unsupported incoming Event message has been received
                _ => Ok(Box::new(stream::empty())),
            }
        }
        IncomingResponse::Error(ErrorResponse::Session(ref inresp)) => {
            let err = format!(
                "received an unexpected Error message (session): {:?}",
                inresp
            );
            Err(err).status(ResponseStatus::BAD_REQUEST)
        }
        IncomingResponse::Error(ErrorResponse::Handle(ref inresp)) => {
            let err = format!(
                "received an unexpected Error message (handle): {:?}",
                inresp
            );
            Err(err).status(ResponseStatus::BAD_REQUEST)
        }
    }
}

fn handle_response_error(
    kind: &str,
    title: &str,
    reqp: &IncomingRequestProperties,
    mut err: SvcError,
    start_timestamp: DateTime<Utc>,
) -> MessageStream {
    err.set_kind(kind, title);
    let status = err.status_code();

    error!(
        "Failed to handle a response from janus, kind = '{}': {}",
        kind, err
    );

    sentry::send(err.clone()).unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));

    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let resp = OutgoingResponse::unicast(err, reqp.to_response(status, timing), reqp, API_VERSION);
    let boxed_resp = Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
    Box::new(stream::once(boxed_resp))
}

pub(crate) async fn handle_event<C: Context>(
    context: &C,
    event: &MQTTIncomingEvent<String>,
    start_timestamp: DateTime<Utc>,
) -> MessageStream {
    handle_event_impl(context, event, start_timestamp)
        .await
        .unwrap_or_else(|err| {
            error!(
                "Failed to handle an event from janus, label = '{}': {}",
                event.properties().label().unwrap_or("none"),
                err,
            );

            sentry::send(err).unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
            Box::new(stream::empty())
        })
}

async fn handle_event_impl<C: Context>(
    context: &C,
    event: &MQTTIncomingEvent<String>,
    start_timestamp: DateTime<Utc>,
) -> Result<MessageStream, SvcError> {
    let payload = MQTTIncomingEvent::convert_payload::<IncomingEvent>(&event)
        .map_err(|err| format!("failed to parse event: {}", err))
        .status(ResponseStatus::BAD_REQUEST)?;

    let evp = event.properties();
    match payload {
        IncomingEvent::WebRtcUp(ref inev) => {
            let rtc_stream_id = Uuid::from_str(inev.opaque_id())
                .map_err(|err| format!("Failed to parse opaque id as uuid: {}", err))
                .status(ResponseStatus::BAD_REQUEST)?;

            let conn = context.db().get()?;

            // If the event relates to a publisher's handle,
            // we will find the corresponding stream and send event w/ updated stream object
            // to the room's topic.
            if let Some(rtc_stream) = janus_rtc_stream::start(rtc_stream_id, &conn)? {
                let rtc_id = rtc_stream.rtc_id();

                let room = room::FindQuery::new()
                    .time(room::now())
                    .rtc_id(rtc_id)
                    .execute(&conn)?
                    .ok_or_else(|| format!("a room for rtc = '{}' is not found", &rtc_id))
                    .status(ResponseStatus::NOT_FOUND)?;

                let event = endpoint::rtc_stream::update_event(
                    room.id(),
                    rtc_stream,
                    start_timestamp,
                    evp.tracking(),
                )?;

                Ok(Box::new(stream::once(
                    Box::new(event) as Box<dyn IntoPublishableMessage + Send>
                )))
            } else {
                Ok(Box::new(stream::empty()))
            }
        }
        IncomingEvent::HangUp(ref inev) => {
            handle_hangup_detach(context, start_timestamp, inev, evp)
        }
        IncomingEvent::Detached(ref inev) => {
            handle_hangup_detach(context, start_timestamp, inev, evp)
        }
        IncomingEvent::Media(_) | IncomingEvent::Timeout(_) | IncomingEvent::SlowLink(_) => {
            // Ignore these kinds of events.
            Ok(Box::new(stream::empty()))
        }
    }
}

fn handle_hangup_detach<C: Context, E: OpaqueId>(
    context: &C,
    start_timestamp: DateTime<Utc>,
    inev: &E,
    evp: &IncomingEventProperties,
) -> Result<MessageStream, SvcError> {
    let rtc_stream_id = Uuid::from_str(inev.opaque_id())
        .map_err(|err| format!("Failed to parse opaque id as uuid: {}", err))
        .status(ResponseStatus::BAD_REQUEST)?;

    let conn = context.db().get()?;

    // If the event relates to a publisher's handle,
    // we will find the corresponding stream and send event w/ updated stream object
    // to the room's topic.
    if let Some(rtc_stream) = janus_rtc_stream::stop(rtc_stream_id, &conn)? {
        let rtc_id = rtc_stream.rtc_id();

        let room = room::FindQuery::new()
            .time(room::now())
            .rtc_id(rtc_id)
            .execute(&conn)?
            .ok_or_else(|| format!("a room for rtc = '{}' is not found", &rtc_id))
            .status(ResponseStatus::NOT_FOUND)?;

        // Publish the update event if only stream object has been changed
        // (if there weren't any actual media stream, the object won't contain its start time)
        if rtc_stream.time().is_some() {
            let event = endpoint::rtc_stream::update_event(
                room.id(),
                rtc_stream,
                start_timestamp,
                evp.tracking(),
            )?;

            let boxed_event = Box::new(event) as Box<dyn IntoPublishableMessage + Send>;
            return Ok(Box::new(stream::once(boxed_event)));
        }
    }

    Ok(Box::new(stream::empty()))
}

pub(crate) async fn handle_status_event<C: Context>(
    context: &C,
    event: &MQTTIncomingEvent<String>,
    start_timestamp: DateTime<Utc>,
) -> MessageStream {
    handle_status_event_impl(context, event, start_timestamp)
        .await
        .unwrap_or_else(|err| {
            error!(
                "Failed to handle a status event from janus, label = '{}': {}",
                event.properties().label().unwrap_or("none"),
                err,
            );

            sentry::send(err).unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
            Box::new(stream::empty())
        })
}

async fn handle_status_event_impl<C: Context>(
    context: &C,
    event: &MQTTIncomingEvent<String>,
    start_timestamp: DateTime<Utc>,
) -> Result<MessageStream, SvcError> {
    let evp = event.properties();

    let payload = MQTTIncomingEvent::convert_payload::<StatusEvent>(&event)
        .map_err(|err| format!("failed to parse event: {}", err))
        .status(ResponseStatus::BAD_REQUEST)?;

    if payload.online() {
        let event = create_session_request(&payload, evp, context.agent_id(), start_timestamp)
            .map_err(|err| err.to_string())
            .status(ResponseStatus::UNPROCESSABLE_ENTITY)?;

        let boxed_event = Box::new(event) as Box<dyn IntoPublishableMessage + Send>;
        Ok(Box::new(stream::once(boxed_event)))
    } else {
        let conn = context.db().get()?;
        janus_backend::DeleteQuery::new(evp.as_agent_id()).execute(&conn)?;
        Ok(Box::new(stream::empty()))
    }
}
