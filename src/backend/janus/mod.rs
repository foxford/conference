use std::str::FromStr;

use anyhow::{Context as AnyhowContext, Result};
use async_std::stream;
use chrono::Utc;
use std::ops::Bound;
use svc_agent::mqtt::{
    IncomingEvent as MQTTIncomingEvent, IncomingRequestProperties, IntoPublishableMessage,
    OutgoingResponse, ResponseStatus, ShortTermTimingProperties,
};
use svc_agent::Addressable;
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
use crate::app::message_handler::MessageStream;
use crate::app::API_VERSION;
use crate::db::{agent_connection, janus_backend, janus_rtc_stream};
use crate::diesel::Connection;
use crate::{app::context::Context, backend::janus::http::create_handle::CreateHandleRequest};
use crate::{
    app::error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind},
    backend::janus::http::transactions::Transaction,
};

use serde::Deserialize;

use self::http::IncomingEvent;

////////////////////////////////////////////////////////////////////////////////

const STREAM_UPLOAD_METHOD: &str = "stream.upload";
pub(crate) const JANUS_API_VERSION: &str = "v1";

const ALREADY_RUNNING_STATE: &str = "already_running";

pub(crate) trait OpaqueId {
    fn opaque_id(&self) -> &str;
}

fn handle_response_error<C: Context>(
    context: &mut C,
    reqp: &IncomingRequestProperties,
    app_error: AppError,
) -> MessageStream {
    context.add_logger_tags(o!(
        "status" => app_error.status().as_u16(),
        "kind" => app_error.kind().to_owned(),
    ));

    error!(
        context.logger(),
        "Failed to handle a response from janus: {}",
        app_error.source(),
    );

    app_error.notify_sentry(context.logger());

    let svc_error: SvcError = app_error.to_svc_error();
    let timing = ShortTermTimingProperties::until_now(context.start_timestamp());
    let respp = reqp.to_response(svc_error.status_code(), timing);
    let resp = OutgoingResponse::unicast(svc_error, respp, reqp, API_VERSION);
    let boxed_resp = Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
    Box::new(stream::once(boxed_resp))
}

pub(crate) async fn handle_event<C: Context>(
    context: &mut C,
    event: IncomingEvent,
) -> MessageStream {
    handle_event_impl(context, event)
        .await
        .unwrap_or_else(|app_error| {
            error!(
                context.logger(),
                "Failed to handle an event from janus: {}", app_error
            );

            app_error.notify_sentry(context.logger());
            Box::new(stream::empty())
        })
}

async fn handle_event_impl<C: Context>(
    context: &mut C,
    payload: IncomingEvent,
) -> Result<MessageStream, AppError> {
    match payload {
        IncomingEvent::WebRtcUp(ref inev) => {
            context.add_logger_tags(o!("rtc_stream_id" => inev.opaque_id().to_string()));

            let rtc_stream_id = Uuid::from_str(inev.opaque_id())
                .map_err(|err| anyhow!("Failed to parse opaque id as UUID: {}", err))
                .error(AppErrorKind::MessageParsingFailed)?;

            let conn = context.get_conn()?;

            // If the event relates to a publisher's handle,
            // we will find the corresponding stream and send event w/ updated stream object
            // to the room's topic.
            if let Some(rtc_stream) = janus_rtc_stream::start(rtc_stream_id, &conn)? {
                let room = endpoint::helpers::find_room_by_rtc_id(
                    context,
                    rtc_stream.rtc_id(),
                    endpoint::helpers::RoomTimeRequirement::Open,
                    &conn,
                )?;

                let event = endpoint::rtc_stream::update_event(
                    room.id(),
                    rtc_stream,
                    context.start_timestamp(),
                )?;

                Ok(Box::new(stream::once(
                    Box::new(event) as Box<dyn IntoPublishableMessage + Send>
                )))
            } else {
                Ok(Box::new(stream::empty()))
            }
        }
        IncomingEvent::HangUp(ref inev) => handle_hangup_detach(context, inev),
        IncomingEvent::Detached(ref inev) => handle_hangup_detach(context, inev),
        IncomingEvent::Media(_) | IncomingEvent::Timeout(_) | IncomingEvent::SlowLink(_) => {
            // Ignore these kinds of events.
            Ok(Box::new(stream::empty()))
        }
        IncomingEvent::Event(resp) => {
            match resp.transaction {
                Transaction::AgentLeave => Ok(Box::new(stream::empty())),
                Transaction::CreateStream(tn) => {
                    context.add_logger_tags(o!("method" => tn.reqp.method().to_string()));
                    let jsep = resp.jsep;
                    resp.plugindata
                        .data
                        .as_ref()
                        .ok_or_else(|| anyhow!("Missing 'data' in the response"))
                        .error(AppErrorKind::MessageParsingFailed)?
                        .get("status")
                        .ok_or_else(|| anyhow!("Missing 'status' in the response"))
                        .error(AppErrorKind::MessageParsingFailed)
                        .and_then(|status| {
                            context.add_logger_tags(o!("status" => status.as_u64()));

                            if status == "200" {
                                Ok(())
                            } else {
                                Err(anyhow!("Received error status"))
                                    .error(AppErrorKind::BackendRequestFailed)
                            }
                        })
                        .and_then(|_| {
                            // Getting answer (as JSEP)
                            let jsep = jsep
                                .ok_or_else(|| anyhow!("Missing 'jsep' in the response"))
                                .error(AppErrorKind::MessageParsingFailed)?;

                            let timing =
                                ShortTermTimingProperties::until_now(context.start_timestamp());

                            let resp = endpoint::rtc_signal::CreateResponse::unicast(
                                endpoint::rtc_signal::CreateResponseData::new(Some(jsep)),
                                tn.reqp.to_response(ResponseStatus::OK, timing),
                                tn.reqp.as_agent_id(),
                                JANUS_API_VERSION,
                            );

                            let boxed_resp =
                                Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
                            Ok(Box::new(stream::once(boxed_resp)) as MessageStream)
                        })
                        .or_else(|err| Ok(handle_response_error(context, &tn.reqp, err)))
                }
                Transaction::ReadStream(tn) => {
                    context.add_logger_tags(o!("method" => tn.reqp.method().to_string()));
                    let jsep = resp.jsep;
                    resp.plugindata
                        .data
                        .as_ref()
                        .ok_or_else(|| anyhow!("Missing 'data' in the response"))
                        .error(AppErrorKind::MessageParsingFailed)?
                        .get("status")
                        .ok_or_else(|| anyhow!("Missing 'status' in the response"))
                        .error(AppErrorKind::MessageParsingFailed)
                        // We fail if the status isn't equal to 200
                        .and_then(|status| {
                            context.add_logger_tags(o!("status" => status.as_u64()));

                            if status == "200" {
                                Ok(())
                            } else {
                                Err(anyhow!("Received error status"))
                                    .error(AppErrorKind::BackendRequestFailed)
                            }
                        })
                        .and_then(|_| {
                            // Getting answer (as JSEP)
                            let jsep = jsep
                                .ok_or_else(|| anyhow!("Missing 'jsep' in the response"))
                                .error(AppErrorKind::MessageParsingFailed)?;

                            let timing =
                                ShortTermTimingProperties::until_now(context.start_timestamp());

                            let resp = endpoint::rtc_signal::CreateResponse::unicast(
                                endpoint::rtc_signal::CreateResponseData::new(Some(jsep)),
                                tn.reqp.to_response(ResponseStatus::OK, timing),
                                tn.reqp.as_agent_id(),
                                JANUS_API_VERSION,
                            );

                            let boxed_resp =
                                Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
                            Ok(Box::new(stream::once(boxed_resp)) as MessageStream)
                        })
                        .or_else(|err| Ok(handle_response_error(context, &tn.reqp, err)))
                }
                Transaction::UpdateReaderConfig => Ok(Box::new(stream::empty())),
                Transaction::UpdateWriterConfig => Ok(Box::new(stream::empty())),
                Transaction::UploadStream(transaction) => {
                    //todo handle after merge
                    Ok(Box::new(stream::empty()))
                }
            }
        }
        IncomingEvent::KeepAlive => Ok(Box::new(stream::empty())),
    }
}

fn handle_hangup_detach<C: Context, E: OpaqueId>(
    context: &mut C,
    inev: &E,
) -> Result<MessageStream, AppError> {
    context.add_logger_tags(o!("rtc_stream_id" => inev.opaque_id().to_owned()));

    let rtc_stream_id = Uuid::from_str(inev.opaque_id())
        .map_err(|err| anyhow!("Failed to parse opaque id as UUID: {}", err))
        .error(AppErrorKind::MessageParsingFailed)?;

    let conn = context.get_conn()?;

    // If the event relates to the publisher's handle,
    // we will find the corresponding stream and send an event w/ updated stream object
    // to the room's topic.
    if let Some(rtc_stream) = janus_rtc_stream::stop(rtc_stream_id, &conn)? {
        let room = endpoint::helpers::find_room_by_rtc_id(
            context,
            rtc_stream.rtc_id(),
            endpoint::helpers::RoomTimeRequirement::Open,
            &conn,
        )?;

        // Publish the update event only if the stream object has been changed.
        // If there's no actual media stream, the object wouldn't contain its start time.
        if rtc_stream.time().is_some() {
            // Disconnect agents.
            agent_connection::BulkDisconnectByRoomQuery::new(room.id()).execute(&conn)?;

            // Send rtc_stream.update event.
            let event = endpoint::rtc_stream::update_event(
                room.id(),
                rtc_stream,
                context.start_timestamp(),
                // evp.tracking(),
            )?;

            let boxed_event = Box::new(event) as Box<dyn IntoPublishableMessage + Send>;
            return Ok(Box::new(stream::once(boxed_event)));
        }
    }

    Ok(Box::new(stream::empty()))
}

pub(crate) async fn handle_status_event<C: Context>(
    context: &mut C,
    event: &MQTTIncomingEvent<String>,
) -> MessageStream {
    handle_status_event_impl(context, event)
        .await
        .unwrap_or_else(|app_error| {
            error!(
                context.logger(),
                "Failed to handle a status event from janus: {}", app_error
            );

            app_error.notify_sentry(context.logger());
            Box::new(stream::empty())
        })
}

// Janus Gateway online/offline status.
#[derive(Debug, Deserialize)]
pub struct StatusEvent {
    pub online: bool,
    pub capacity: Option<i32>,
    pub balancer_capacity: Option<i32>,
}

async fn handle_status_event_impl<C: Context>(
    context: &mut C,
    event: &MQTTIncomingEvent<String>,
) -> Result<MessageStream, AppError> {
    let evp = event.properties();
    context.add_logger_tags(o!("label" => evp.label().unwrap_or("").to_string()));

    let payload = MQTTIncomingEvent::convert_payload::<StatusEvent>(&event)
        .map_err(|err| anyhow!("Failed to parse event: {}", err))
        .error(AppErrorKind::MessageParsingFailed)?;

    if payload.online {
        let client = context.janus_http_client();
        let session = client
            .create_session()
            .await
            .context("CreateSession")
            .error(AppErrorKind::BackendRequestFailed)?;
        let handle = client
            .create_handle(CreateHandleRequest {
                session_id: session.id,
                opaque_id: Uuid::new_v4().to_string(),
            })
            .await
            .context("Create first handle")
            .error(AppErrorKind::BackendRequestFailed)?;
        let backend_id = evp.as_agent_id();
        let conn = context.get_conn()?;

        let mut q = janus_backend::UpsertQuery::new(backend_id, handle.id, session.id);

        if let Some(capacity) = payload.capacity {
            q = q.capacity(capacity);
        }

        if let Some(balancer_capacity) = payload.balancer_capacity {
            q = q.balancer_capacity(balancer_capacity);
        }

        q.execute(&conn)?;
        // start_polling(context.janus_http_client(), session.id).await;
        Ok(Box::new(stream::empty()))
    } else {
        let conn = context.get_conn()?;

        let streams_with_rtc = conn.transaction::<_, AppError, _>(|| {
            let streams_with_rtc = janus_rtc_stream::ListWithRtcQuery::new()
                .active(true)
                .backend_id(evp.as_agent_id())
                .execute(&conn)?;

            agent_connection::BulkDisconnectByBackendQuery::new(evp.as_agent_id())
                .execute(&conn)?;

            janus_backend::DeleteQuery::new(evp.as_agent_id()).execute(&conn)?;
            Ok(streams_with_rtc)
        })?;

        let now = Utc::now();
        let mut events = Vec::with_capacity(streams_with_rtc.len());

        for (mut stream, rtc) in streams_with_rtc {
            stream.set_time(stream.time().map(|t| (t.0, Bound::Excluded(now))));

            let event = endpoint::rtc_stream::update_event(
                rtc.room_id(),
                stream,
                context.start_timestamp(),
                // evp.tracking(),
            )?;

            events.push(Box::new(event) as Box<dyn IntoPublishableMessage + Send>);
        }

        Ok(Box::new(stream::from_iter(events)))
    }
}

////////////////////////////////////////////////////////////////////////////////
pub mod http;
pub mod poller;
