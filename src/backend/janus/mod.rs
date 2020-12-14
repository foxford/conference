use std::ops::Bound;

use anyhow::Result;
use async_std::stream;
use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::pg::PgConnection;
use svc_agent::mqtt::{
    IncomingEvent as MQTTIncomingEvent, IncomingEventProperties, IncomingRequestProperties,
    IncomingResponse as MQTTIncomingResponse, IntoPublishableMessage, OutgoingResponse,
    ResponseStatus, ShortTermTimingProperties,
};
use svc_agent::Addressable;
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint;
use crate::app::error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind};
use crate::app::handle_id::HandleId;
use crate::app::message_handler::MessageStream;
use crate::app::API_VERSION;
use crate::db::{agent, janus_backend, janus_rtc_stream, recording, room, rtc};
use crate::diesel::Connection;
use crate::util::from_base64;

use self::events::{DetachedEvent, IncomingEvent, MediaEvent, StatusEvent};
use self::responses::{ErrorResponse, IncomingResponse};
use self::transactions::Transaction;

////////////////////////////////////////////////////////////////////////////////

const STREAM_UPLOAD_METHOD: &str = "stream.upload";
pub(crate) const JANUS_API_VERSION: &str = "v1";

////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn handle_response<C: Context>(
    context: &mut C,
    resp: &MQTTIncomingResponse<String>,
) -> MessageStream {
    handle_response_impl(context, resp)
        .await
        .unwrap_or_else(|app_error| {
            error!(
                context.logger(),
                "Failed to handle a response from janus: {}", app_error,
            );

            app_error.notify_sentry(context.logger());
            Box::new(stream::empty())
        })
}

async fn handle_response_impl<C: Context>(
    context: &mut C,
    resp: &MQTTIncomingResponse<String>,
) -> Result<MessageStream, AppError> {
    let respp = resp.properties();
    context.janus_client().finish_transaction(respp);

    let payload = MQTTIncomingResponse::convert_payload::<IncomingResponse>(&resp)
        .map_err(|err| anyhow!("Failed to parse response: {}", err))
        .error(AppErrorKind::MessageParsingFailed)?;

    match payload {
        IncomingResponse::Success(ref inresp) => {
            let txn = from_base64::<Transaction>(&inresp.transaction())
                .map_err(|err| err.context("Failed to parse transaction"))
                .error(AppErrorKind::MessageParsingFailed)?;

            match txn {
                // Session has been created.
                Transaction::CreateSession(tn) => {
                    // Create service handle.
                    let backreq = context
                        .janus_client()
                        .create_service_handle_request(
                            respp,
                            inresp.data().id(),
                            tn.capacity(),
                            tn.balancer_capacity(),
                            context.start_timestamp(),
                        )
                        .error(AppErrorKind::MessageBuildingFailed)?;

                    let boxed_backreq = Box::new(backreq) as Box<dyn IntoPublishableMessage + Send>;
                    Ok(Box::new(stream::once(boxed_backreq)))
                }
                // Service handle has been created.
                Transaction::CreateServiceHandle(tn) => {
                    let backend_id = respp.as_agent_id();
                    let handle_id = inresp.data().id();
                    let conn = context.get_conn()?;

                    let mut q =
                        janus_backend::UpsertQuery::new(backend_id, handle_id, tn.session_id());

                    if let Some(capacity) = tn.capacity() {
                        q = q.capacity(capacity);
                    }

                    if let Some(balancer_capacity) = tn.balancer_capacity() {
                        q = q.balancer_capacity(balancer_capacity);
                    }

                    q.execute(&conn)?;
                    Ok(Box::new(stream::empty()))
                }
                // Agent handle has been created.
                Transaction::CreateAgentHandle(tn) => {
                    let handle_id = HandleId::new(
                        inresp.data().id(),
                        tn.session_id(),
                        respp.as_agent_id().to_owned(),
                    );

                    // Make signal.create request.
                    let backreq = context
                        .janus_client()
                        .create_signal_request(
                            tn.reqp(),
                            &respp,
                            handle_id,
                            tn.jsep().to_owned(),
                            context.start_timestamp(),
                        )
                        .error(AppErrorKind::MessageBuildingFailed)?;

                    let boxed_backreq = Box::new(backreq) as Box<dyn IntoPublishableMessage + Send>;
                    Ok(Box::new(stream::once(boxed_backreq)))
                }
                // An unsupported incoming Success message has been received.
                _ => Ok(Box::new(stream::empty())),
            }
        }
        IncomingResponse::Ack(ref inresp) => {
            let txn = from_base64::<Transaction>(&inresp.transaction())
                .map_err(|err| err.context("Failed to parse transaction"))
                .error(AppErrorKind::MessageParsingFailed)?;

            match txn {
                // Conference Stream is being created
                Transaction::CreateStream(_tn) => Ok(Box::new(stream::empty())),
                // Trickle message has been received by Janus Gateway
                Transaction::Trickle(tn) => {
                    let resp = endpoint::signal::TrickleResponse::unicast(
                        endpoint::signal::TrickleResponseData::new(),
                        tn.reqp().to_response(
                            ResponseStatus::OK,
                            ShortTermTimingProperties::until_now(context.start_timestamp()),
                        ),
                        tn.reqp().as_agent_id(),
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
            context.add_logger_tags(o!("transaction" => inresp.transaction().to_string()));

            let txn = from_base64::<Transaction>(&inresp.transaction())
                .map_err(|err| err.context("Failed to parse transaction"))
                .error(AppErrorKind::MessageParsingFailed)?;

            match txn {
                // Signal has been created.
                Transaction::CreateSignal(ref tn) => {
                    context.add_logger_tags(o!("method" => tn.reqp().method().to_string()));

                    inresp
                        .jsep()
                        .ok_or_else(|| anyhow!("Missing 'jsep' in the response"))
                        .error(AppErrorKind::MessageParsingFailed)
                        .and_then(|jsep| {
                            let resp = endpoint::signal::CreateResponse::unicast(
                                endpoint::signal::CreateResponseData::new(
                                    tn.handle_id().to_owned(),
                                    jsep.to_owned(),
                                ),
                                tn.reqp().to_response(
                                    ResponseStatus::OK,
                                    ShortTermTimingProperties::until_now(context.start_timestamp()),
                                ),
                                tn.reqp(),
                                JANUS_API_VERSION,
                            );

                            let boxed_resp =
                                Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;

                            Ok(Box::new(stream::once(boxed_resp)) as MessageStream)
                        })
                        .or_else(|err| Ok(handle_response_error(context, &tn.reqp(), err)))
                }
                // Signal has been updated.
                Transaction::UpdateSignal(ref tn) => {
                    context.add_logger_tags(o!("method" => tn.reqp().method().to_string()));

                    inresp
                        .jsep()
                        .ok_or_else(|| anyhow!("Missing 'jsep' in the response"))
                        .error(AppErrorKind::MessageParsingFailed)
                        .and_then(|jsep| {
                            let resp = endpoint::signal::UpdateResponse::unicast(
                                endpoint::signal::UpdateResponseData::new(jsep.to_owned()),
                                tn.reqp().to_response(
                                    ResponseStatus::OK,
                                    ShortTermTimingProperties::until_now(context.start_timestamp()),
                                ),
                                tn.reqp(),
                                JANUS_API_VERSION,
                            );

                            let boxed_resp =
                                Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;

                            Ok(Box::new(stream::once(boxed_resp)) as MessageStream)
                        })
                        .or_else(|err| Ok(handle_response_error(context, &tn.reqp(), err)))
                }
                // Conference Stream has been created (an answer received)
                Transaction::CreateStream(ref tn) => {
                    context.add_logger_tags(o!("method" => tn.reqp().method().to_string()));

                    inresp
                        .plugin()
                        .data()
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
                            let timing =
                                ShortTermTimingProperties::until_now(context.start_timestamp());

                            let resp = endpoint::rtc::ConnectResponse::unicast(
                                endpoint::rtc::ConnectResponseData::new(),
                                tn.reqp().to_response(ResponseStatus::OK, timing),
                                tn.reqp().as_agent_id(),
                                JANUS_API_VERSION,
                            );

                            let boxed_resp =
                                Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;

                            Ok(Box::new(stream::once(boxed_resp)) as MessageStream)
                        })
                        .or_else(|err| Ok(handle_response_error(context, &tn.reqp(), err)))
                }
                // Conference Stream has been read (an answer received)
                Transaction::ReadStream(ref tn) => {
                    context.add_logger_tags(o!("method" => tn.reqp().method().to_string()));

                    inresp
                        .plugin()
                        .data()
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
                            let timing =
                                ShortTermTimingProperties::until_now(context.start_timestamp());

                            let resp = endpoint::rtc::ConnectResponse::unicast(
                                endpoint::rtc::ConnectResponseData::new(),
                                tn.reqp().to_response(ResponseStatus::OK, timing),
                                tn.reqp().as_agent_id(),
                                JANUS_API_VERSION,
                            );

                            let boxed_resp =
                                Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;

                            Ok(Box::new(stream::once(boxed_resp)) as MessageStream)
                        })
                        .or_else(|err| Ok(handle_response_error(context, &tn.reqp(), err)))
                }
                // Conference Stream has been uploaded to a storage backend (a confirmation)
                Transaction::UploadStream(ref tn) => {
                    context.add_logger_tags(o!(
                        "method" => tn.method().to_string(),
                        "rtc_id" => tn.rtc_id().to_string(),
                    ));

                    // TODO: improve error handling
                    let plugin_data = inresp.plugin().data();

                    plugin_data
                        .get("status")
                        .ok_or_else(|| anyhow!("Missing 'status' in the response"))
                        .error(AppErrorKind::MessageParsingFailed)
                        // We fail if the status isn't equal to 200
                        .and_then(|status| {
                            context.add_logger_tags(o!("status" => status.as_u64()));

                            match status {
                                val if val == "200" => Ok(()),
                                val if val == "404" => {
                                    let conn = context.get_conn()?;

                                    recording::UpdateQuery::new(tn.rtc_id())
                                        .status(recording::Status::Missing)
                                        .execute(&conn)?;

                                    Err(anyhow!("Janus is missing recording"))
                                        .error(AppErrorKind::BackendRecordingMissing)
                                }
                                _ => Err(anyhow!("Received error status"))
                                    .error(AppErrorKind::BackendRequestFailed),
                            }
                        })
                        .and_then(|_| {
                            let rtc_id = plugin_data
                                .get("id")
                                .ok_or_else(|| anyhow!("Missing 'id' in response"))
                                .error(AppErrorKind::MessageParsingFailed)
                                .and_then(|val| {
                                    serde_json::from_value::<Uuid>(val.clone())
                                        .map_err(|err| anyhow!("Invalid value for 'id': {}", err))
                                        .error(AppErrorKind::MessageParsingFailed)
                                })?;

                            let started_at = plugin_data
                                .get("started_at")
                                .ok_or_else(|| anyhow!("Missing 'started_at' in response"))
                                .error(AppErrorKind::MessageParsingFailed)
                                .and_then(|val| {
                                    let unix_ts = serde_json::from_value::<u64>(val.clone())
                                        .map_err(|err| anyhow!("Invalid value for 'started_at': {}", err))
                                        .error(AppErrorKind::MessageParsingFailed)?;

                                    let naive_datetime = NaiveDateTime::from_timestamp(
                                        unix_ts as i64 / 1000,
                                        ((unix_ts % 1000) * 1_000_000) as u32,
                                    );

                                    Ok(DateTime::<Utc>::from_utc(naive_datetime, Utc))
                                })?;

                            let segments = plugin_data
                                .get("time")
                                .ok_or_else(|| anyhow!("Missing time"))
                                .error(AppErrorKind::MessageParsingFailed)
                                .and_then(|segments| {
                                    Ok(serde_json::from_value::<Vec<(i64, i64)>>(segments.clone())
                                        .map_err(|err| anyhow!("Invalid value for 'time': {}", err))
                                        .error(AppErrorKind::MessageParsingFailed)?
                                        .into_iter()
                                        .map(|(start, end)| {
                                            (Bound::Included(start), Bound::Excluded(end))
                                        })
                                        .collect())
                                })?;

                            let (room, rtcs, recs): (room::Object, Vec<rtc::Object>, Vec<recording::Object>) = {
                                let conn = context.get_conn()?;

                                recording::UpdateQuery::new(rtc_id)
                                    .status(recording::Status::Ready)
                                    .started_at(started_at)
                                    .segments(segments)
                                    .execute(&conn)?;

                                let rtc = rtc::FindQuery::new()
                                    .id(rtc_id)
                                    .execute(&conn)?
                                    .ok_or_else(|| anyhow!("RTC not found"))
                                    .error(AppErrorKind::RtcNotFound)?;

                                let room = endpoint::helpers::find_room_by_rtc_id(
                                    context,
                                    rtc.id(),
                                    endpoint::helpers::RoomTimeRequirement::Any,
                                )?;

                                // TODO: move to db module
                                use diesel::prelude::*;
                                let rtcs = rtc::Object::belonging_to(&room).load(&conn)?;
                                let recs = recording::Object::belonging_to(&rtcs).load(&conn)?;

                                (room, rtcs, recs)
                            };

                            // Ensure that all rtcs have a recording.
                            let rtc_ids_with_recs = recs
                                .iter()
                                .map(|rec| rec.rtc_id())
                                .collect::<Vec<Uuid>>();

                            for rtc in rtcs {
                                if !rtc_ids_with_recs.contains(&rtc.id()) {
                                    let mut logger = context.logger().new(o!(
                                        "room_id" => room.id().to_string(),
                                        "rtc_id" => rtc.id().to_string(),
                                    ));

                                    if let Some(scope) = room.tags().get("scope") {
                                        logger = logger.new(o!("scope" => scope.to_string()));
                                    }

                                    info!(
                                        logger,
                                        "postpone 'room.upload' event because still waiting for rtcs being uploaded";
                                    );

                                    return Ok(Box::new(stream::empty()) as MessageStream);
                                }
                            }

                            // Send room.upload event.
                            let event = endpoint::system::upload_event(
                                context,
                                &room,
                                recs.into_iter(),
                                respp.tracking(),
                            )?;

                            let event_box = Box::new(event) as Box<dyn IntoPublishableMessage + Send>;
                            Ok(Box::new(stream::once(event_box)) as MessageStream)
                        })
                }
                // An unsupported incoming Event message has been received
                _ => Ok(Box::new(stream::empty())),
            }
        }
        IncomingResponse::Error(ErrorResponse::Session(ref inresp)) => {
            let err = anyhow!(
                "received an unexpected Error message (session): {:?}",
                inresp
            );
            Err(err).error(AppErrorKind::MessageParsingFailed)
        }
        IncomingResponse::Error(ErrorResponse::Handle(ref inresp)) => {
            let err = anyhow!(
                "received an unexpected Error message (handle): {:?}",
                inresp
            );
            Err(err).error(AppErrorKind::MessageParsingFailed)
        }
    }
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
    event: &MQTTIncomingEvent<String>,
) -> MessageStream {
    handle_event_impl(context, event)
        .await
        .unwrap_or_else(|app_error| {
            error!(
                context.logger(),
                "Failed to handle event from janus: {}; Payload: {}",
                event.payload(),
                app_error
            );

            app_error.notify_sentry(context.logger());
            Box::new(stream::empty())
        })
}

////////////////////////////////////////////////////////////////////////////////

async fn handle_event_impl<C: Context>(
    context: &mut C,
    event: &MQTTIncomingEvent<String>,
) -> Result<MessageStream, AppError> {
    context.add_logger_tags(o!("label" => event.properties().label().unwrap_or("").to_string()));

    let payload = MQTTIncomingEvent::convert_payload::<IncomingEvent>(&event)
        .map_err(|err| anyhow!("Failed to parse event: {}", err))
        .error(AppErrorKind::MessageParsingFailed)?;

    let evp = event.properties();

    // Event types hint:
    // WebRtcUp – DTLS connection has been initially set up. Not being dispatched on renegotiation.
    // Media – Started/stopped receiving audio/video. Not being dispatched on refresh or tab close.
    // SlowLink – Janus has detected many packet losses.
    // HangUp – ICE handle stopped. Not being dispatched for Chrome.
    // Detach – Janus plugin handle destroyed. FF dispatches both HangUp and Detach.
    match payload {
        IncomingEvent::Media(ref inev) => handle_media_event(context, inev, evp),
        IncomingEvent::Detached(ref inev) => handle_detached_event(context, inev, evp),
        IncomingEvent::HangUp(_)
        | IncomingEvent::WebRtcUp(_)
        | IncomingEvent::Timeout(_)
        | IncomingEvent::SlowLink(_) => Ok(Box::new(stream::empty())),
    }
}

fn handle_media_event<C: Context>(
    context: &mut C,
    inev: &MediaEvent,
    evp: &IncomingEventProperties,
) -> Result<MessageStream, AppError> {
    // Handle only video-related events to avoid duplication.
    if inev.is_video() {
        return Ok(Box::new(stream::empty()));
    }

    let conn = context.get_conn()?;

    let (janus_rtc_stream, room) =
        find_stream_with_room(context, &conn, inev.sender(), !inev.is_receiving())?;

    if inev.is_receiving() {
        start_stream(context, &conn, &janus_rtc_stream, &room, evp)
    } else {
        stop_stream(context, &conn, &janus_rtc_stream, &room, evp)
    }
}

fn handle_detached_event<C: Context>(
    context: &mut C,
    inev: &DetachedEvent,
    evp: &IncomingEventProperties,
) -> Result<MessageStream, AppError> {
    let conn = context.get_conn()?;
    let (janus_rtc_stream, room) = find_stream_with_room(context, &conn, inev.sender(), true)?;
    stop_stream(context, &conn, &janus_rtc_stream, &room, evp)
}

fn find_stream_with_room<C: Context>(
    context: &mut C,
    conn: &PgConnection,
    handle_id: i64,
    is_started: bool,
) -> Result<(janus_rtc_stream::Object, room::Object), AppError> {
    let (janus_rtc_stream, room) = janus_rtc_stream::FindWithRoomQuery::new()
        .handle_id(handle_id)
        .is_started(is_started)
        .is_stopped(false)
        .execute(conn)?;

    context.add_logger_tags(o!(
        "room_id" => room.id().to_string(),
        "rtc_id" => janus_rtc_stream.rtc_id().to_string(),
        "rtc_stream_id" => janus_rtc_stream.id().to_string(),
    ));

    Ok((janus_rtc_stream, room))
}

fn start_stream<C: Context>(
    context: &mut C,
    conn: &PgConnection,
    janus_rtc_stream: &janus_rtc_stream::Object,
    room: &room::Object,
    evp: &IncomingEventProperties,
) -> Result<MessageStream, AppError> {
    if let Some(rtc_stream) = janus_rtc_stream::start(janus_rtc_stream.id(), conn)? {
        info!(context.logger(), "Stream started");

        let event = endpoint::rtc_stream::update_event(
            room.id(),
            rtc_stream,
            context.start_timestamp(),
            evp.tracking(),
        )?;

        Ok(Box::new(stream::once(
            Box::new(event) as Box<dyn IntoPublishableMessage + Send>
        )))
    } else {
        Ok(Box::new(stream::empty()))
    }
}

fn stop_stream<C: Context>(
    context: &mut C,
    conn: &PgConnection,
    janus_rtc_stream: &janus_rtc_stream::Object,
    room: &room::Object,
    evp: &IncomingEventProperties,
) -> Result<MessageStream, AppError> {
    if let Some(rtc_stream) = janus_rtc_stream::stop(janus_rtc_stream.id(), conn)? {
        // Publish the update event only if the stream object has been changed.
        // If there's no actual media stream, the object wouldn't contain its start time.
        if rtc_stream.time().is_none() {
            return Ok(Box::new(stream::empty()));
        }

        info!(context.logger(), "Stream stopped");

        // Send rtc_stream.update event.
        let event = endpoint::rtc_stream::update_event(
            room.id(),
            rtc_stream,
            context.start_timestamp(),
            evp.tracking(),
        )?;

        let boxed_event = Box::new(event) as Box<dyn IntoPublishableMessage + Send>;
        Ok(Box::new(stream::once(boxed_event)))
    } else {
        Ok(Box::new(stream::empty()))
    }
}

////////////////////////////////////////////////////////////////////////////////

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

async fn handle_status_event_impl<C: Context>(
    context: &mut C,
    event: &MQTTIncomingEvent<String>,
) -> Result<MessageStream, AppError> {
    let evp = event.properties();
    context.add_logger_tags(o!("label" => evp.label().unwrap_or("").to_string()));

    let payload = MQTTIncomingEvent::convert_payload::<StatusEvent>(&event)
        .map_err(|err| anyhow!("Failed to parse event: {}", err))
        .error(AppErrorKind::MessageParsingFailed)?;

    if payload.online() {
        let event = context
            .janus_client()
            .create_session_request(&payload, evp, context.start_timestamp())
            .error(AppErrorKind::MessageBuildingFailed)?;

        let boxed_event = Box::new(event) as Box<dyn IntoPublishableMessage + Send>;
        Ok(Box::new(stream::once(boxed_event)))
    } else {
        let conn = context.get_conn()?;

        let streams_with_rtc = conn.transaction::<_, AppError, _>(|| {
            let streams_with_rtc = janus_rtc_stream::ListWithRtcQuery::new()
                .active(true)
                .backend_id(evp.as_agent_id())
                .execute(&conn)?;

            agent::BulkStatusUpdateQuery::new(agent::Status::Ready)
                .backend_id(evp.as_agent_id())
                .status(agent::Status::Connected)
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
                evp.tracking(),
            )?;

            events.push(Box::new(event) as Box<dyn IntoPublishableMessage + Send>);
        }

        Ok(Box::new(stream::from_iter(events)))
    }
}

////////////////////////////////////////////////////////////////////////////////

mod client;
mod events;
pub(crate) mod requests;
mod responses;
mod transactions;

pub(crate) use client::Client;
