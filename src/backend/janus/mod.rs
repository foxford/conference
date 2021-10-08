use anyhow::{anyhow, Result};

use futures::stream;

use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
        OutgoingResponse, ResponseStatus, ShortTermTimingProperties,
    },
    Addressable, AgentId,
};
use svc_error::Error as SvcError;
use tracing::{error, Span};

use crate::{
    app::{
        context::Context,
        endpoint,
        error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind},
        message_handler::MessageStream,
        metrics::HistogramExt,
        API_VERSION,
    },
    db::{self, agent_connection, janus_rtc_stream, recording, room, rtc},
};

use serde::{Deserialize, Serialize};

use self::client::{create_handle::OpaqueId, transactions::TransactionKind, IncomingEvent};

////////////////////////////////////////////////////////////////////////////////

pub const JANUS_API_VERSION: &str = "v1";

const ALREADY_RUNNING_STATE: &str = "already_running";

fn handle_response_error<C: Context>(
    context: &mut C,
    reqp: &IncomingRequestProperties,
    err: AppError,
) -> MessageStream {
    error!(?err, "Failed to handle a response from janus",);
    let svc_error: SvcError = err.to_svc_error();
    err.notify_sentry();

    let timing = ShortTermTimingProperties::until_now(context.start_timestamp());
    let respp = reqp.to_response(svc_error.status_code(), timing);
    let resp = OutgoingResponse::unicast(svc_error, respp, reqp, API_VERSION);
    let boxed_resp = Box::new(resp) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
    Box::new(stream::once(std::future::ready(boxed_resp)))
}

pub async fn handle_event<C: Context>(context: &mut C, event: IncomingEvent) -> MessageStream {
    handle_event_impl(context, event)
        .await
        .unwrap_or_else(|err| {
            error!(?err, "Failed to handle an event from janus");
            err.notify_sentry();
            Box::new(stream::empty())
        })
}

async fn handle_event_impl<C: Context>(
    context: &mut C,
    payload: IncomingEvent,
) -> Result<MessageStream, AppError> {
    match payload {
        IncomingEvent::WebRtcUp(inev) => {
            // If the event relates to a publisher's handle,
            // we will find the corresponding stream and send event w/ updated stream object
            // to the room's topic.
            let conn = context.get_conn().await?;
            let start_timestamp = context.start_timestamp();
            crate::util::spawn_blocking(move || {
                if let Some(rtc_stream) = janus_rtc_stream::start(inev.opaque_id.stream_id, &conn)?
                {
                    let room = endpoint::helpers::find_room_by_rtc_id(
                        rtc_stream.rtc_id(),
                        endpoint::helpers::RoomTimeRequirement::Open,
                        &conn,
                    )?;

                    let event =
                        endpoint::rtc_stream::update_event(room.id(), rtc_stream, start_timestamp)?;

                    Ok(Box::new(stream::once(std::future::ready(
                        Box::new(event) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>
                    ))) as MessageStream)
                } else {
                    Ok(Box::new(stream::empty()) as MessageStream)
                }
            })
            .await
        }
        IncomingEvent::HangUp(inev) => handle_hangup_detach(context, inev.opaque_id).await,
        IncomingEvent::Detached(inev) => handle_hangup_detach(context, inev.opaque_id).await,
        IncomingEvent::Media(_) | IncomingEvent::Timeout(_) | IncomingEvent::SlowLink(_) => {
            // Ignore these kinds of events.
            Ok(Box::new(stream::empty()))
        }
        IncomingEvent::Event(resp) => {
            match resp.transaction.kind {
                Some(TransactionKind::AgentLeave) => Ok(Box::new(stream::empty())),
                Some(TransactionKind::CreateStream(tn)) => {
                    let jsep = resp.jsep;
                    let k = resp
                        .opaque_id
                        .as_ref()
                        .expect("Must be some")
                        .stream_id
                        .to_string();
                    resp.plugindata
                        .data
                        .as_ref()
                        .ok_or_else(|| anyhow!("Missing 'data' in the response"))
                        .error(AppErrorKind::MessageParsingFailed)?
                        .get("status")
                        .ok_or_else(|| anyhow!("Missing 'status' in the response"))
                        .error(AppErrorKind::MessageParsingFailed)
                        .and_then(|status| {
                            if status == "200" {
                                Ok(())
                            } else {
                                Err(anyhow!("Received {} status", status))
                                    .error(AppErrorKind::BackendRequestFailed)
                            }
                        })
                        .and_then(|_| {
                            // Getting answer (as JSEP)
                            let jsep = jsep
                                .ok_or_else(|| anyhow!("Missing 'jsep' in the response"))
                                .error(AppErrorKind::MessageParsingFailed)?;
                            let wait = context.wait().clone();
                            tokio::spawn(async move {
                                let _ = wait.put_value(k, jsep).await;
                            });
                            // let timing =
                            //     ShortTermTimingProperties::until_now(context.start_timestamp());

                            // let resp = endpoint::rtc_signal::CreateResponse::unicast(
                            //     endpoint::rtc_signal::CreateResponseData::new(Some(jsep)),
                            //     tn.reqp.to_response(ResponseStatus::OK, timing),
                            //     tn.reqp.as_agent_id(),
                            //     JANUS_API_VERSION,
                            // );

                            // let boxed_resp = Box::new(resp)
                            //     as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
                            // context
                            //     .metrics()
                            //     .request_duration
                            //     .rtc_signal_create
                            //     .observe_timestamp(tn.start_timestamp);
                            // Ok(Box::new(stream::once(std::future::ready(boxed_resp)))
                            //     as MessageStream)
                            Ok(Box::new(stream::empty()) as MessageStream)
                        })
                        .or_else(|err| Ok(handle_response_error(context, &tn.reqp, err)))
                }
                Some(TransactionKind::ReadStream(tn)) => {
                    let jsep = resp.jsep;
                    let k = resp
                        .opaque_id
                        .as_ref()
                        .expect("Must be some")
                        .stream_id
                        .to_string();
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
                            if status == "200" {
                                Ok(())
                            } else {
                                Err(anyhow!("Received {} status", status))
                                    .error(AppErrorKind::BackendRequestFailed)
                            }
                        })
                        .and_then(|_| {
                            // Getting answer (as JSEP)
                            let jsep = jsep
                                .ok_or_else(|| anyhow!("Missing 'jsep' in the response"))
                                .error(AppErrorKind::MessageParsingFailed)?;

                            // let timing =
                            // ShortTermTimingProperties::until_now(context.start_timestamp());
                            let wait = context.wait().clone();
                            tokio::spawn(async move {
                                let _ = wait.put_value(k, jsep).await;
                            });
                            // let resp = endpoint::rtc_signal::CreateResponse::unicast(
                            //     endpoint::rtc_signal::CreateResponseData::new(Some(jsep)),
                            //     tn.reqp.to_response(ResponseStatus::OK, timing),
                            //     tn.reqp.as_agent_id(),
                            //     JANUS_API_VERSION,
                            // );

                            // let boxed_resp = Box::new(resp)
                            //     as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
                            // context
                            //     .metrics()
                            //     .request_duration
                            //     .rtc_signal_read
                            //     .observe_timestamp(tn.start_timestamp);
                            // Ok(Box::new(stream::once(std::future::ready(boxed_resp)))
                            //     as MessageStream)
                            Ok(Box::new(stream::empty()) as MessageStream)
                        })
                        .or_else(|err| Ok(handle_response_error(context, &tn.reqp, err)))
                }
                Some(TransactionKind::UpdateReaderConfig) => Ok(Box::new(stream::empty())),
                Some(TransactionKind::UpdateWriterConfig) => Ok(Box::new(stream::empty())),
                Some(TransactionKind::ServicePing) => Ok(Box::new(stream::empty())),
                // Conference Stream has been uploaded to a storage backend (a confirmation)
                Some(TransactionKind::UploadStream(ref tn)) => {
                    Span::current().record("rtc_id", &tn.rtc_id.to_string().as_str());

                    // TODO: improve error handling
                    let plugin_data = resp
                        .plugindata
                        .data
                        .ok_or_else(|| anyhow!("Missing 'data' in the response"))
                        .error(AppErrorKind::MessageParsingFailed)?;

                    let upload_stream = async {
                        let status = plugin_data
                            .get("status")
                            .ok_or_else(|| anyhow!("Missing 'status' in the response"))
                            .error(AppErrorKind::MessageParsingFailed)?;
                        match status {
                            val if val == "200" => Ok(()),
                            val if val == "404" => {
                                let conn = context.get_conn().await?;
                                let rtc_id = tn.rtc_id;
                                crate::util::spawn_blocking(move || {
                                    recording::UpdateQuery::new(rtc_id)
                                        .status(recording::Status::Missing)
                                        .execute(&conn)
                                })
                                .await?;

                                Err(anyhow!("Janus is missing recording"))
                                    .error(AppErrorKind::BackendRecordingMissing)
                            }
                            _ => Err(anyhow!("Received {} status", status))
                                .error(AppErrorKind::BackendRequestFailed),
                        }?;
                        let rtc_id = plugin_data
                            .get("id")
                            .ok_or_else(|| anyhow!("Missing 'id' in response"))
                            .error(AppErrorKind::MessageParsingFailed)
                            .and_then(|val| {
                                serde_json::from_value::<db::rtc::Id>(val.clone())
                                    .map_err(|err| anyhow!("Invalid value for 'id': {}", err))
                                    .error(AppErrorKind::MessageParsingFailed)
                            })?;

                        // if vacuuming was already started by previous request - just do nothing
                        let maybe_already_running =
                            plugin_data.get("state").and_then(|v| v.as_str())
                                == Some(ALREADY_RUNNING_STATE);
                        if maybe_already_running {
                            return Ok(Box::new(stream::empty()) as MessageStream);
                        }

                        let mjr_dumps_uris = plugin_data
                            .get("mjr_dumps_uris")
                            .ok_or_else(|| anyhow!("Missing 'mjr_dumps_uris' in response"))
                            .error(AppErrorKind::MessageParsingFailed)
                            .and_then(|dumps| {
                                serde_json::from_value::<Vec<String>>(dumps.clone())
                                    .map_err(|err| {
                                        anyhow!("Invalid value for 'dumps_uris': {}", err)
                                    })
                                    .error(AppErrorKind::MessageParsingFailed)
                            })?;

                        let (room, rtcs_with_recs): (
                            room::Object,
                            Vec<(rtc::Object, Option<recording::Object>)>,
                        ) = {
                            let conn = context.get_conn().await?;
                            crate::util::spawn_blocking(move || {
                                recording::UpdateQuery::new(rtc_id)
                                    .status(recording::Status::Ready)
                                    .mjr_dumps_uris(mjr_dumps_uris)
                                    .execute(&conn)?;

                                let rtc = rtc::FindQuery::new()
                                    .id(rtc_id)
                                    .execute(&conn)?
                                    .ok_or_else(|| anyhow!("RTC not found"))
                                    .error(AppErrorKind::RtcNotFound)?;

                                let room = endpoint::helpers::find_room_by_rtc_id(
                                    rtc.id(),
                                    endpoint::helpers::RoomTimeRequirement::Any,
                                    &conn,
                                )?;

                                let rtcs_with_recs =
                                    rtc::ListWithReadyRecordingQuery::new(room.id())
                                        .execute(&conn)?;

                                Ok::<_, AppError>((room, rtcs_with_recs))
                            })
                            .await?
                        };
                        // Ensure that all rtcs have a ready recording.
                        let rtcs_total = rtcs_with_recs.len();

                        let recs_with_rtcs = rtcs_with_recs
                            .into_iter()
                            .filter_map(|(rtc, maybe_recording)| {
                                let recording = maybe_recording?;
                                matches!(recording.status(), db::recording::Status::Ready)
                                    .then(|| (recording, rtc))
                            })
                            .collect::<Vec<_>>();

                        if recs_with_rtcs.len() < rtcs_total {
                            return Ok(Box::new(stream::empty()) as MessageStream);
                        }

                        // Send room.upload event.
                        let event = endpoint::system::upload_event(
                            context,
                            &room,
                            recs_with_rtcs.into_iter(),
                        )?;

                        let event_box = Box::new(event)
                            as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;

                        Ok(Box::new(stream::once(std::future::ready(event_box))) as MessageStream)
                    };
                    let response = upload_stream.await;
                    context
                        .metrics()
                        .request_duration
                        .upload_stream
                        .observe_timestamp(tn.start_timestamp);
                    response
                }
                Some(TransactionKind::AgentSpeaking) => {
                    let data = resp
                        .plugindata
                        .data
                        .ok_or_else(|| anyhow!("Missing data int response"))
                        .error(AppErrorKind::MessageParsingFailed)?;
                    let notification: SpeakingNotification = serde_json::from_value(data)
                        .map_err(anyhow::Error::from)
                        .error(AppErrorKind::MessageParsingFailed)?;
                    let opaque_id = resp
                        .opaque_id
                        .ok_or_else(|| anyhow!("Missing opaque id"))
                        .error(AppErrorKind::MessageParsingFailed)?;
                    let uri = format!("rooms/{}/events", opaque_id.room_id);
                    let timing = ShortTermTimingProperties::until_now(context.start_timestamp());
                    let props = OutgoingEventProperties::new("rtc_stream.agent_speaking", timing);
                    let event = OutgoingEvent::broadcast(notification, props, &uri);

                    Ok(Box::new(stream::once(std::future::ready(
                        Box::new(event) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>
                    ))) as MessageStream)
                }
                None => Ok(Box::new(stream::empty()) as MessageStream),
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct SpeakingNotification {
    speaking: bool,
    agent_id: AgentId,
}

async fn handle_hangup_detach<C: Context>(
    context: &mut C,
    opaque_id: OpaqueId,
) -> Result<MessageStream, AppError> {
    // If the event relates to the publisher's handle,
    // we will find the corresponding stream and send an event w/ updated stream object
    // to the room's topic.
    let conn = context.get_conn().await?;
    let start_timestamp = context.start_timestamp();
    crate::util::spawn_blocking(move || {
        if let Some(rtc_stream) = janus_rtc_stream::stop(opaque_id.stream_id, &conn)? {
            // Publish the update event only if the stream object has been changed.
            // If there's no actual media stream, the object wouldn't contain its start time.
            if rtc_stream.time().is_some() {
                // Disconnect agents.
                agent_connection::BulkDisconnectByRtcQuery::new(rtc_stream.rtc_id())
                    .execute(&conn)?;

                // Send rtc_stream.update event.
                let event = endpoint::rtc_stream::update_event(
                    opaque_id.room_id,
                    rtc_stream,
                    start_timestamp,
                )?;

                let boxed_event =
                    Box::new(event) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
                return Ok(Box::new(stream::once(std::future::ready(boxed_event))) as MessageStream);
            }
        }
        Ok::<_, AppError>(Box::new(stream::empty()) as MessageStream)
    })
    .await
}

////////////////////////////////////////////////////////////////////////////////
pub mod client;
pub mod client_pool;
pub mod metrics;
pub mod online_handler;
