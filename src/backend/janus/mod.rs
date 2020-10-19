use std::str::FromStr;

use anyhow::Result;
use async_std::stream;
use chrono::{DateTime, NaiveDateTime, Utc};
use std::ops::Bound;
use svc_agent::mqtt::{
    IncomingEvent as MQTTIncomingEvent, IncomingEventProperties, IncomingRequestProperties,
    IncomingResponse as MQTTIncomingResponse, IntoPublishableMessage, OutgoingResponse,
    ResponseStatus, ShortTermTimingProperties,
};
use svc_agent::Addressable;
use svc_error::{extension::sentry, Error as SvcError};
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

use self::events::{IncomingEvent, StatusEvent};
use self::responses::{ErrorResponse, IncomingResponse};
use self::transactions::Transaction;

////////////////////////////////////////////////////////////////////////////////

const STREAM_UPLOAD_METHOD: &str = "stream.upload";
pub(crate) const JANUS_API_VERSION: &str = "v1";

pub(crate) trait OpaqueId {
    fn opaque_id(&self) -> &str;
}

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

            let svc_error: SvcError = app_error.into();

            sentry::send(svc_error).unwrap_or_else(|err| {
                warn!(context.logger(), "Error sending error to Sentry: {}", err)
            });

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
                // Session has been created
                Transaction::CreateSession(tn) => {
                    // Creating Handle
                    let backreq = context
                        .janus_client()
                        .create_handle_request(
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
                // Handle has been created
                Transaction::CreateHandle(tn) => {
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
                // Rtc Handle has been created
                Transaction::CreateRtcHandle(tn) => {
                    let agent_id = respp.as_agent_id();
                    let reqp = tn.reqp();

                    // Returning Real-Time connection handle
                    let resp = endpoint::rtc::ConnectResponse::unicast(
                        endpoint::rtc::ConnectResponseData::new(HandleId::new(
                            tn.rtc_stream_id(),
                            tn.rtc_id(),
                            inresp.data().id(),
                            tn.session_id(),
                            agent_id.clone(),
                        )),
                        reqp.to_response(
                            ResponseStatus::OK,
                            ShortTermTimingProperties::until_now(context.start_timestamp()),
                        ),
                        reqp,
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
                .map_err(|err| err.context("Failed to parse transaction"))
                .error(AppErrorKind::MessageParsingFailed)?;

            match txn {
                // Conference Stream is being created
                Transaction::CreateStream(_tn) => Ok(Box::new(stream::empty())),
                // Trickle message has been received by Janus Gateway
                Transaction::Trickle(tn) => {
                    let resp = endpoint::rtc_signal::CreateResponse::unicast(
                        endpoint::rtc_signal::CreateResponseData::new(None),
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
                            // Getting answer (as JSEP)
                            let jsep = inresp
                                .jsep()
                                .ok_or_else(|| anyhow!("Missing 'jsep' in the response"))
                                .error(AppErrorKind::MessageParsingFailed)?;

                            let timing =
                                ShortTermTimingProperties::until_now(context.start_timestamp());

                            let resp = endpoint::rtc_signal::CreateResponse::unicast(
                                endpoint::rtc_signal::CreateResponseData::new(Some(jsep.clone())),
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
                            // Getting answer (as JSEP)
                            let jsep = inresp
                                .jsep()
                                .ok_or_else(|| anyhow!("Missing 'jsep' in the response"))
                                .error(AppErrorKind::MessageParsingFailed)?;

                            let timing =
                                ShortTermTimingProperties::until_now(context.start_timestamp());

                            let resp = endpoint::rtc_signal::CreateResponse::unicast(
                                endpoint::rtc_signal::CreateResponseData::new(Some(jsep.clone())),
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
    let svc_error: SvcError = app_error.into();

    context.add_logger_tags(o!(
        "kind" => svc_error.kind().to_owned(),
        "status" => svc_error.status_code().as_u16(),
    ));

    error!(
        context.logger(),
        "Failed to handle a response from janus: {}",
        svc_error.detail().unwrap_or(""),
    );

    sentry::send(svc_error.clone()).unwrap_or_else(|err| {
        warn!(context.logger(), "Error sending error to Sentry: {}", err);
    });

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
                "Failed to handle an event from janus: {}", app_error
            );

            let svc_error: SvcError = app_error.into();

            sentry::send(svc_error).unwrap_or_else(|err| {
                warn!(context.logger(), "Error sending error to Sentry: {}", err);
            });

            Box::new(stream::empty())
        })
}

async fn handle_event_impl<C: Context>(
    context: &mut C,
    event: &MQTTIncomingEvent<String>,
) -> Result<MessageStream, AppError> {
    context.add_logger_tags(o!("label" => event.properties().label().unwrap_or("").to_string()));

    let payload = MQTTIncomingEvent::convert_payload::<IncomingEvent>(&event)
        .map_err(|err| anyhow!("Failed to parse event: {}", err))
        .error(AppErrorKind::MessageParsingFailed)?;

    let evp = event.properties();
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
                )?;

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
        IncomingEvent::HangUp(ref inev) => handle_hangup_detach(context, inev, evp),
        IncomingEvent::Detached(ref inev) => handle_hangup_detach(context, inev, evp),
        IncomingEvent::Media(_) | IncomingEvent::Timeout(_) | IncomingEvent::SlowLink(_) => {
            // Ignore these kinds of events.
            Ok(Box::new(stream::empty()))
        }
    }
}

fn handle_hangup_detach<C: Context, E: OpaqueId>(
    context: &mut C,
    inev: &E,
    evp: &IncomingEventProperties,
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
        )?;

        // Publish the update event only if the stream object has been changed.
        // If there's no actual media stream, the object wouldn't contain its start time.
        if rtc_stream.time().is_some() {
            // Put connected `agents` back into `ready` status since the stream has gone and
            // they haven't been connected anymore.
            agent::BulkStatusUpdateQuery::new(agent::Status::Ready)
                .room_id(room.id())
                .status(agent::Status::Connected)
                .execute(&conn)?;

            // Send rtc_stream.update event.
            let event = endpoint::rtc_stream::update_event(
                room.id(),
                rtc_stream,
                context.start_timestamp(),
                evp.tracking(),
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

            let svc_error: SvcError = app_error.into();

            sentry::send(svc_error).unwrap_or_else(|err| {
                warn!(context.logger(), "Error sending error to Sentry: {}", err);
            });

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
