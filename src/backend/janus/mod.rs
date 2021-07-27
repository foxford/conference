use anyhow::{anyhow, Context as AnyhowContext, Result};
use async_std::{stream, task};
use chrono::{DateTime, NaiveDateTime, Utc};
use slog::{error, o};
use std::ops::Bound;
use svc_agent::{
    mqtt::{
        IncomingEvent as MQTTIncomingEvent, IncomingRequestProperties, IntoPublishableMessage,
        OutgoingResponse, ResponseStatus, ShortTermTimingProperties,
    },
    Addressable,
};
use svc_error::Error as SvcError;

use crate::{
    app::{
        context::Context,
        endpoint,
        error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind},
        message_handler::MessageStream,
        metrics::HistogramExt,
        API_VERSION,
    },
    backend::janus::client::{create_handle::CreateHandleRequest, JanusClient},
    db::{self, agent_connection, janus_backend, janus_rtc_stream, recording, room, rtc},
    diesel::Connection,
};

use serde::Deserialize;

use self::client::{transactions::Transaction, IncomingEvent};

////////////////////////////////////////////////////////////////////////////////

pub const JANUS_API_VERSION: &str = "v1";

const ALREADY_RUNNING_STATE: &str = "already_running";

pub trait OpaqueId {
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
        "Failed to handle a response from janus: {:?}",
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

pub async fn handle_event<C: Context>(context: &mut C, event: IncomingEvent) -> MessageStream {
    handle_event_impl(context, event)
        .await
        .unwrap_or_else(|app_error| {
            error!(
                context.logger(),
                "Failed to handle an event from janus: {:?}", app_error
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

            let rtc_stream_id = inev
                .opaque_id
                .parse()
                .map_err(|err| anyhow!("Failed to parse opaque id as UUID: {:?}", err))
                .error(AppErrorKind::MessageParsingFailed)?;

            // If the event relates to a publisher's handle,
            // we will find the corresponding stream and send event w/ updated stream object
            // to the room's topic.
            let conn = context.get_conn().await?;
            let start_timestamp = context.start_timestamp();
            task::spawn_blocking(move || {
                if let Some(rtc_stream) = janus_rtc_stream::start(rtc_stream_id, &conn)? {
                    let room = endpoint::helpers::find_room_by_rtc_id(
                        rtc_stream.rtc_id(),
                        endpoint::helpers::RoomTimeRequirement::Open,
                        &conn,
                    )?;

                    let event =
                        endpoint::rtc_stream::update_event(room.id(), rtc_stream, start_timestamp)?;

                    Ok(Box::new(stream::once(
                        Box::new(event) as Box<dyn IntoPublishableMessage + Send>
                    )) as MessageStream)
                } else {
                    Ok(Box::new(stream::empty()) as MessageStream)
                }
            })
            .await
        }
        IncomingEvent::HangUp(ref inev) => handle_hangup_detach(context, inev).await,
        IncomingEvent::Detached(ref inev) => handle_hangup_detach(context, inev).await,
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
                            context
                                .metrics()
                                .request_duration
                                .rtc_signal_create
                                .observe_timestamp(tn.start_timestamp);
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
                            context
                                .metrics()
                                .request_duration
                                .rtc_signal_read
                                .observe_timestamp(tn.start_timestamp);
                            Ok(Box::new(stream::once(boxed_resp)) as MessageStream)
                        })
                        .or_else(|err| Ok(handle_response_error(context, &tn.reqp, err)))
                }
                Transaction::UpdateReaderConfig => Ok(Box::new(stream::empty())),
                Transaction::UpdateWriterConfig => Ok(Box::new(stream::empty())),
                // Conference Stream has been uploaded to a storage backend (a confirmation)
                Transaction::UploadStream(ref tn) => {
                    context.add_logger_tags(o!(
                        "rtc_id" => tn.rtc_id.to_string(),
                    ));
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
                        // We fail if the status isn't equal to 200
                        context.add_logger_tags(o!("status" => status.as_u64()));

                        match status {
                            val if val == "200" => Ok(()),
                            val if val == "404" => {
                                let conn = context.get_conn().await?;
                                let rtc_id = tn.rtc_id;
                                task::spawn_blocking(move || {
                                    recording::UpdateQuery::new(rtc_id)
                                        .status(recording::Status::Missing)
                                        .execute(&conn)
                                })
                                .await?;

                                Err(anyhow!("Janus is missing recording"))
                                    .error(AppErrorKind::BackendRecordingMissing)
                            }
                            _ => Err(anyhow!("Received error status"))
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

                        let started_at = plugin_data
                            .get("started_at")
                            .ok_or_else(|| anyhow!("Missing 'started_at' in response"))
                            .error(AppErrorKind::MessageParsingFailed)
                            .and_then(|val| {
                                let unix_ts = serde_json::from_value::<u64>(val.clone())
                                    .map_err(|err| {
                                        anyhow!("Invalid value for 'started_at': {}", err)
                                    })
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
                        let mjr_dumps_uris = plugin_data
                            .get("mjr_dumps_uris")
                            .map(|dumps| {
                                serde_json::from_value::<Vec<String>>(dumps.clone())
                                    .map_err(|err| {
                                        anyhow!("Invalid value for 'dumps_uris': {}", err)
                                    })
                                    .error(AppErrorKind::MessageParsingFailed)
                            })
                            .transpose()?;

                        let (room, rtcs_with_recs): (
                            room::Object,
                            Vec<(rtc::Object, Option<recording::Object>)>,
                        ) = {
                            let conn = context.get_conn().await?;
                            task::spawn_blocking(move || {
                                recording::UpdateQuery::new(rtc_id)
                                    .status(recording::Status::Ready)
                                    .started_at(started_at)
                                    .segments(segments)
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
                                    rtc::ListWithRecordingQuery::new(room.id()).execute(&conn)?;

                                Ok::<_, AppError>((room, rtcs_with_recs))
                            })
                            .await?
                        };
                        endpoint::helpers::add_room_logger_tags(context, &room);

                        // Ensure that all rtcs have a recording.
                        let rtcs_total = rtcs_with_recs.len();

                        let recs_with_rtcs = rtcs_with_recs
                            .into_iter()
                            .filter_map(|(rtc, maybe_recording)| {
                                maybe_recording.map(|recording| (recording, rtc))
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

                        let event_box = Box::new(event) as Box<dyn IntoPublishableMessage + Send>;

                        Ok(Box::new(stream::once(event_box)) as MessageStream)
                    };
                    let response = upload_stream.await;
                    context
                        .metrics()
                        .request_duration
                        .upload_stream
                        .observe_timestamp(tn.start_timestamp);
                    response
                }
                Transaction::SpeakingNotification => {
                    dbg!(resp.plugindata);
                    Ok(Box::new(stream::empty()))
                }
            }
        }
        IncomingEvent::KeepAlive => Ok(Box::new(stream::empty())),
    }
}

async fn handle_hangup_detach<C: Context, E: OpaqueId>(
    context: &mut C,
    inev: &E,
) -> Result<MessageStream, AppError> {
    context.add_logger_tags(o!("rtc_stream_id" => inev.opaque_id().to_owned()));

    let rtc_stream_id = inev
        .opaque_id()
        .parse()
        .map_err(|err| anyhow!("Failed to parse opaque id as UUID: {}", err))
        .error(AppErrorKind::MessageParsingFailed)?;

    // If the event relates to the publisher's handle,
    // we will find the corresponding stream and send an event w/ updated stream object
    // to the room's topic.
    let conn = context.get_conn().await?;
    let start_timestamp = context.start_timestamp();
    task::spawn_blocking(move || {
        if let Some(rtc_stream) = janus_rtc_stream::stop(rtc_stream_id, &conn)? {
            let room = endpoint::helpers::find_room_by_rtc_id(
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
                let event =
                    endpoint::rtc_stream::update_event(room.id(), rtc_stream, start_timestamp)?;

                let boxed_event = Box::new(event) as Box<dyn IntoPublishableMessage + Send>;
                return Ok(Box::new(stream::once(boxed_event)) as MessageStream);
            }
        }
        Ok::<_, AppError>(Box::new(stream::empty()) as MessageStream)
    })
    .await
}

pub async fn handle_status_event<C: Context>(
    context: &mut C,
    event: &MQTTIncomingEvent<String>,
) -> MessageStream {
    handle_status_event_impl(context, event)
        .await
        .unwrap_or_else(|app_error| {
            error!(
                context.logger(),
                "Failed to handle a status event from janus: {:?}", app_error
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
    pub group: Option<String>,
    pub janus_url: Option<String>,
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
        let janus_url = payload
            .janus_url
            .as_ref()
            .ok_or_else(|| anyhow!("Missing url"))
            .error(AppErrorKind::BackendClientCreationFailed)?
            .clone();
        let janus_client =
            JanusClient::new(&janus_url).error(AppErrorKind::BackendClientCreationFailed)?;
        let session = janus_client
            .create_session()
            .await
            .context("CreateSession")
            .error(AppErrorKind::BackendRequestFailed)?;
        let handle = janus_client
            .create_handle(CreateHandleRequest {
                session_id: session.id,
                opaque_id: db::janus_rtc_stream::Id::random(),
            })
            .await
            .context("Create first handle")
            .error(AppErrorKind::BackendRequestFailed)?;
        let backend_id = evp.as_agent_id().clone();
        let conn = context.get_conn().await?;

        let backend = task::spawn_blocking(move || {
            let mut q =
                janus_backend::UpsertQuery::new(&backend_id, handle.id, session.id, &janus_url);

            if let Some(capacity) = payload.capacity {
                q = q.capacity(capacity);
            }

            if let Some(balancer_capacity) = payload.balancer_capacity {
                q = q.balancer_capacity(balancer_capacity);
            }

            if let Some(group) = payload.group.as_deref() {
                q = q.group(group);
            }

            q.execute(&conn)
        })
        .await?;
        context
            .janus_clients()
            .get_or_insert(&backend)
            .error(AppErrorKind::BrokerRequestFailed)?;
        Ok(Box::new(stream::empty()))
    } else {
        context.janus_clients().remove_client(evp.as_agent_id());
        let conn = context.get_conn().await?;
        let agent_id = evp.as_agent_id().clone();
        let streams_with_rtc = task::spawn_blocking(move || {
            conn.transaction::<_, AppError, _>(|| {
                let streams_with_rtc = janus_rtc_stream::ListWithRtcQuery::new()
                    .active(true)
                    .backend_id(&agent_id)
                    .execute(&conn)?;

                agent_connection::BulkDisconnectByBackendQuery::new(&agent_id).execute(&conn)?;

                janus_backend::DeleteQuery::new(&agent_id).execute(&conn)?;
                Ok(streams_with_rtc)
            })
        })
        .await?;

        let now = Utc::now();
        let mut events = Vec::with_capacity(streams_with_rtc.len());

        for (mut stream, rtc) in streams_with_rtc {
            stream.set_time(stream.time().map(|t| (t.0, Bound::Excluded(now))));

            let event = endpoint::rtc_stream::update_event(
                rtc.room_id(),
                stream,
                context.start_timestamp(),
            )?;

            events.push(Box::new(event) as Box<dyn IntoPublishableMessage + Send>);
        }

        Ok(Box::new(stream::from_iter(events)))
    }
}

////////////////////////////////////////////////////////////////////////////////
pub mod client;
pub mod client_pool;
pub mod metrics;
