use anyhow::{anyhow, Context as AnyhowContext, Result};
use futures::stream;
use serde::{Deserialize, Serialize};
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
        OutgoingResponse, ResponseStatus, ShortTermTimingProperties,
    },
    Addressable, AgentId,
};
use svc_error::Error as SvcError;
use tracing::{error, info, Span};

use self::client::{
    create_handle::OpaqueId, transactions::TransactionKind, HandleId, IncomingEvent,
};
use crate::{
    app::{
        context::Context,
        endpoint,
        error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind},
        message_handler::MessageStream,
        metrics::HistogramExt,
        API_VERSION,
    },
    client::conference::ConferenceClient,
    db::{self, agent_connection, janus_rtc_stream, recording, rtc},
};

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
            let mut conn = context.get_conn().await?;

            agent_connection::UpdateQuery::new(inev.sender, agent_connection::Status::Connected)
                .execute(&mut conn)
                .await?;

            let maybe_rtc_stream =
                janus_rtc_stream::start(inev.opaque_id.stream_id, &mut conn).await?;

            // If the event relates to a publisher's handle,
            // we will find the corresponding stream and send event w/ updated stream object
            // to the room's topic.
            let start_timestamp = context.start_timestamp();

            if let Some(rtc_stream) = maybe_rtc_stream {
                let room = endpoint::helpers::find_room_by_rtc_id(
                    rtc_stream.rtc_id(),
                    endpoint::helpers::RoomTimeRequirement::Open,
                    &mut conn,
                )
                .await?;

                let event =
                    endpoint::rtc_stream::update_event(room.id(), rtc_stream, start_timestamp);

                Ok(Box::new(stream::once(std::future::ready(
                    Box::new(event) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>
                ))) as MessageStream)
            } else {
                Ok(Box::new(stream::empty()) as MessageStream)
            }
        }
        IncomingEvent::HangUp(inev) => {
            handle_hangup_detach(context, inev.opaque_id, inev.sender).await
        }
        IncomingEvent::Detached(inev) => {
            handle_hangup_detach(context, inev.opaque_id, inev.sender).await
        }
        IncomingEvent::Media(_) | IncomingEvent::Timeout(_) | IncomingEvent::SlowLink(_) => {
            // Ignore these kinds of events.
            Ok(Box::new(stream::empty()))
        }
        IncomingEvent::Event(resp) => {
            match resp.transaction.kind {
                Some(TransactionKind::AgentLeave) => Ok(Box::new(stream::empty())),
                Some(TransactionKind::CreateStream(tn)) => {
                    let jsep = resp.jsep;
                    let response_data = resp
                        .plugindata
                        .data
                        .as_ref()
                        .context("Missing 'data' in the response")
                        .error(AppErrorKind::MessageParsingFailed)?
                        .get("status")
                        .context("Missing 'status' in the response")
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
                                .context("Missing 'jsep' in the response")
                                .error(AppErrorKind::MessageParsingFailed)?;
                            Ok(endpoint::rtc_signal::CreateResponseData::new(Some(jsep)))
                        });

                    match tn {
                        client::create_stream::CreateStreamTransaction::Mqtt {
                            reqp,
                            start_timestamp,
                        } => match response_data {
                            Ok(payload) => {
                                let timing =
                                    ShortTermTimingProperties::until_now(context.start_timestamp());

                                let resp = endpoint::rtc_signal::CreateResponse::unicast(
                                    payload,
                                    reqp.to_response(ResponseStatus::OK, timing),
                                    reqp.as_agent_id(),
                                    JANUS_API_VERSION,
                                );

                                context
                                    .metrics()
                                    .request_duration
                                    .rtc_signal_create
                                    .observe_timestamp(start_timestamp);

                                let boxed_resp = Box::new(resp)
                                    as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
                                Ok(Box::new(stream::once(std::future::ready(boxed_resp)))
                                    as MessageStream)
                            }
                            Err(err) => Ok(handle_response_error(context, &reqp, err)),
                        },
                        client::create_stream::CreateStreamTransaction::Http {
                            id,
                            replica_addr,
                        } => {
                            let own_ip_addr = context.janus_clients().own_ip_addr();

                            if own_ip_addr == replica_addr {
                                if let Err(err) = context
                                    .janus_clients()
                                    .stream_waitlist()
                                    .fire(id, response_data)
                                {
                                    error!(?err, "failed to fire the response to waitlist");
                                }
                            } else if let Err(err) = context
                                .conference_client()
                                .stream_callback(replica_addr, response_data, id)
                                .await
                            {
                                error!(?err, "failed to callback replica {}", replica_addr,);
                            }

                            Ok(Box::new(stream::empty()))
                        }
                    }
                }
                Some(TransactionKind::ReadStream(tn)) => {
                    let jsep = resp.jsep;
                    let response_data = resp
                        .plugindata
                        .data
                        .as_ref()
                        .context("Missing 'data' in the response")
                        .error(AppErrorKind::MessageParsingFailed)?
                        .get("status")
                        .context("Missing 'status' in the response")
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
                                .context("Missing 'jsep' in the response")
                                .error(AppErrorKind::MessageParsingFailed)?;

                            Ok(endpoint::rtc_signal::CreateResponseData::new(Some(jsep)))
                        });

                    match tn {
                        client::read_stream::ReadStreamTransaction::Mqtt {
                            reqp,
                            start_timestamp,
                        } => match response_data {
                            Ok(payload) => {
                                let timing =
                                    ShortTermTimingProperties::until_now(context.start_timestamp());

                                let resp = endpoint::rtc_signal::CreateResponse::unicast(
                                    payload,
                                    reqp.to_response(ResponseStatus::OK, timing),
                                    reqp.as_agent_id(),
                                    JANUS_API_VERSION,
                                );

                                let boxed_resp = Box::new(resp)
                                    as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
                                context
                                    .metrics()
                                    .request_duration
                                    .rtc_signal_read
                                    .observe_timestamp(start_timestamp);
                                Ok(Box::new(stream::once(std::future::ready(boxed_resp)))
                                    as MessageStream)
                            }
                            Err(err) => Ok(handle_response_error(context, &reqp, err)),
                        },
                        client::read_stream::ReadStreamTransaction::Http { id, replica_addr } => {
                            let own_ip_addr = context.janus_clients().own_ip_addr();

                            if own_ip_addr == replica_addr {
                                if let Err(err) = context
                                    .janus_clients()
                                    .stream_waitlist()
                                    .fire(id, response_data)
                                {
                                    error!(?err, "failed to fire the response to waitlist");
                                }
                            } else if let Err(err) = context
                                .conference_client()
                                .stream_callback(replica_addr, response_data, id)
                                .await
                            {
                                error!(?err, "failed to callback replica {}", replica_addr,);
                            }

                            Ok(Box::new(stream::empty()))
                        }
                    }
                }
                Some(TransactionKind::UpdateReaderConfig) => Ok(Box::new(stream::empty())),
                Some(TransactionKind::UpdateWriterConfig) => Ok(Box::new(stream::empty())),
                Some(TransactionKind::ServicePing) => Ok(Box::new(stream::empty())),
                // Conference Stream has been uploaded to a storage backend (a confirmation)
                Some(TransactionKind::UploadStream(ref tn)) => {
                    Span::current().record("rtc_id", tn.rtc_id.to_string().as_str());

                    let plugin_data = resp
                        .plugindata
                        .data
                        .context("Missing 'data' in the response")
                        .error(AppErrorKind::MessageParsingFailed)?;

                    let upload_stream = async {
                        let status = plugin_data
                            .get("status")
                            .context("Missing 'status' in the response")
                            .error(AppErrorKind::MessageParsingFailed)?;
                        match status {
                            val if val == "200" => Ok(()),
                            val if val == "404" => {
                                let mut conn = context.get_conn().await?;
                                recording::UpdateQuery::new(tn.rtc_id)
                                    .status(recording::Status::Missing)
                                    .execute(&mut conn)
                                    .await?;

                                Err(anyhow!("Janus is missing recording"))
                                    .error(AppErrorKind::BackendRecordingMissing)
                            }
                            _ => Err(anyhow!("Received {} status", status))
                                .error(AppErrorKind::BackendRequestFailed),
                        }?;
                        let rtc_id = plugin_data
                            .get("id")
                            .context("Missing 'id' in response")
                            .error(AppErrorKind::MessageParsingFailed)
                            .and_then(|val| {
                                serde_json::from_value::<db::rtc::Id>(val.clone())
                                    .context("Invalid value for 'id'")
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
                            .context("Missing 'mjr_dumps_uris' in response")
                            .error(AppErrorKind::MessageParsingFailed)
                            .and_then(|dumps| {
                                serde_json::from_value::<Vec<String>>(dumps.clone())
                                    .context("Invalid value for 'dumps_uris'")
                                    .error(AppErrorKind::MessageParsingFailed)
                            })?;

                        let mut conn = context.get_conn().await?;
                        let rtc = rtc::FindQuery::new(rtc_id)
                            .execute(&mut conn)
                            .await?
                            .context("RTC not found")
                            .error(AppErrorKind::RtcNotFound)?;

                        let room = endpoint::helpers::find_room_by_rtc_id(
                            rtc.id(),
                            endpoint::helpers::RoomTimeRequirement::Any,
                            &mut conn,
                        )
                        .await?;

                        recording::UpdateQuery::new(rtc_id)
                            .status(recording::Status::Ready)
                            .mjr_dumps_uris(mjr_dumps_uris)
                            .execute(&mut conn)
                            .await?;

                        let mut conn = context.get_conn().await?;
                        let rtcs_with_recs = rtc::ListWithRecordingQuery::new(room.id())
                            .execute(&mut conn)
                            .await?;

                        // Ensure that all rtcs with a recording have ready recording.
                        let room_done = rtcs_with_recs.iter().all(|(_rtc, maybe_recording)| {
                            match maybe_recording {
                                None => true,
                                Some(recording) => {
                                    recording.status() == db::recording::Status::Ready
                                }
                            }
                        });

                        if !room_done {
                            return Ok(Box::new(stream::empty()) as MessageStream);
                        }

                        let recs_with_rtcs =
                            rtcs_with_recs
                                .into_iter()
                                .filter_map(|(rtc, maybe_recording)| {
                                    let recording = maybe_recording?;
                                    matches!(recording.status(), db::recording::Status::Ready)
                                        .then(|| (recording, rtc))
                                });

                        info!(
                            class_id = %room.classroom_id(),
                            room_id = %room.id(),
                            "sending room.upload event"
                        );
                        // Send room.upload event.
                        let event = endpoint::system::upload_event(context, &room, recs_with_rtcs)?;

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
                        .context("Missing data int response")
                        .error(AppErrorKind::MessageParsingFailed)?;
                    let notification: SpeakingNotification =
                        serde_json::from_value(data).error(AppErrorKind::MessageParsingFailed)?;

                    let mut conn = context.get_conn().await?;
                    let active_ban = db::ban_account::FindQuery::new(&notification.agent_id)
                        .execute(&mut conn)
                        .await?;

                    match active_ban {
                        Some(_) => {
                            // ignore notification
                            Ok(Box::new(stream::empty()) as MessageStream)
                        }
                        None => {
                            let opaque_id = resp
                                .opaque_id
                                .context("Missing opaque id")
                                .error(AppErrorKind::MessageParsingFailed)?;
                            let uri = format!("rooms/{}/events", opaque_id.room_id);
                            let timing =
                                ShortTermTimingProperties::until_now(context.start_timestamp());
                            let props =
                                OutgoingEventProperties::new("rtc_stream.agent_speaking", timing);
                            let event = OutgoingEvent::broadcast(notification, props, &uri);

                            Ok(Box::new(stream::once(std::future::ready(Box::new(event)
                                as Box<dyn IntoPublishableMessage + Send + Sync + 'static>)))
                                as MessageStream)
                        }
                    }
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
    handle_id: HandleId,
) -> Result<MessageStream, AppError> {
    // If the event relates to the publisher's handle,
    // we will find the corresponding stream and send an event w/ updated stream object
    // to the room's topic.
    let mut conn = context.get_conn().await?;
    let stop_stream_evt = match janus_rtc_stream::stop(opaque_id.stream_id, &mut conn).await? {
        Some(rtc_stream) => {
            let start_timestamp = context.start_timestamp();
            let mut conn = context.get_conn().await?;

            // Publish the update event only if the stream object has been changed.
            // If there's no actual media stream, the object wouldn't contain its start time.
            if rtc_stream.time().is_some() {
                // Disconnect agents.
                agent_connection::BulkDisconnectByRtcQuery::new(rtc_stream.rtc_id())
                    .execute(&mut conn)
                    .await?;

                // Send rtc_stream.update event.
                let event = endpoint::rtc_stream::update_event(
                    opaque_id.room_id,
                    rtc_stream,
                    start_timestamp,
                );

                let boxed_event =
                    Box::new(event) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
                Some(boxed_event)
            } else {
                None
            }
        }
        None => {
            // Disconnect just this agent.
            agent_connection::DisconnectSingleAgentQuery::new(handle_id)
                .execute(&mut conn)
                .await?;

            None
        }
    };

    let stream = stream::iter(vec![stop_stream_evt].into_iter().flatten());
    Ok(Box::new(stream))
}

////////////////////////////////////////////////////////////////////////////////
pub mod client;
pub mod client_pool;
pub mod metrics;
pub mod online_handler;
mod waitlist;
