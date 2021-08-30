use anyhow::{anyhow, Context as AnyhowContext, Result};
use async_std::{stream, task};
use chrono::Utc;
use slog::{error, o};
use std::ops::Bound;
use svc_agent::{
    mqtt::{
        IncomingEvent as MQTTIncomingEvent, IncomingRequestProperties, IntoPublishableMessage,
        OutgoingEvent, OutgoingEventProperties, OutgoingResponse, ResponseStatus,
        ShortTermTimingProperties,
    },
    Addressable, AgentId,
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
    backend::janus::client::{
        create_handle::CreateHandleRequest,
        service_ping::{ServicePingRequest, ServicePingRequestBody},
        JanusClient,
    },
    db::{self, agent_connection, janus_backend, janus_rtc_stream, recording, room, rtc},
    diesel::Connection,
};

use serde::{Deserialize, Serialize};

use self::client::{create_handle::OpaqueId, transactions::Transaction, IncomingEvent};

////////////////////////////////////////////////////////////////////////////////

pub const JANUS_API_VERSION: &str = "v1";

const ALREADY_RUNNING_STATE: &str = "already_running";

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
    context.add_logger_tags(o!("janus_event" => payload.event_kind()));
    match payload {
        IncomingEvent::WebRtcUp(inev) => {
            context.add_logger_tags(o!("rtc_stream_id" => inev.opaque_id.stream_id.to_string()));

            // If the event relates to a publisher's handle,
            // we will find the corresponding stream and send event w/ updated stream object
            // to the room's topic.
            let conn = context.get_conn().await?;
            let start_timestamp = context.start_timestamp();
            task::spawn_blocking(move || {
                if let Some(rtc_stream) = janus_rtc_stream::start(inev.opaque_id.stream_id, &conn)?
                {
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
        IncomingEvent::HangUp(inev) => handle_hangup_detach(context, inev.opaque_id).await,
        IncomingEvent::Detached(inev) => handle_hangup_detach(context, inev.opaque_id).await,
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
                Transaction::ServicePing => Ok(Box::new(stream::empty())),
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
                            task::spawn_blocking(move || {
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
                        endpoint::helpers::add_room_logger_tags(context, &room);

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
                Transaction::AgentSpeaking => {
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

                    Ok(Box::new(stream::once(
                        Box::new(event) as Box<dyn IntoPublishableMessage + Send>
                    )) as MessageStream)
                }
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
    context.add_logger_tags(o!("rtc_stream_id" => opaque_id.stream_id.to_string()));

    // If the event relates to the publisher's handle,
    // we will find the corresponding stream and send an event w/ updated stream object
    // to the room's topic.
    let conn = context.get_conn().await?;
    let start_timestamp = context.start_timestamp();
    task::spawn_blocking(move || {
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

    let payload = MQTTIncomingEvent::convert_payload::<StatusEvent>(event)
        .map_err(|err| anyhow!("Failed to parse event: {}", err))
        .error(AppErrorKind::MessageParsingFailed)?;
    let janus_backend = task::spawn_blocking({
        let conn = context.get_conn().await?;
        let backend_id = evp.as_agent_id().clone();
        move || {
            db::janus_backend::FindQuery::new()
                .id(&backend_id)
                .execute(&conn)
        }
    })
    .await?;
    if payload.online {
        handle_online(payload, evp.as_agent_id(), janus_backend, context).await?;
        Ok(Box::new(stream::empty()))
    } else {
        handle_offline(janus_backend, context).await
    }
}

async fn handle_online(
    event: StatusEvent,
    backend_id: &AgentId,
    existing_backend: Option<janus_backend::Object>,
    context: &mut impl Context,
) -> Result<(), AppError> {
    let janus_url = event
        .janus_url
        .as_ref()
        .ok_or_else(|| anyhow!("Missing url"))
        .error(AppErrorKind::BackendClientCreationFailed)?
        .clone();
    let janus_client =
        JanusClient::new(&janus_url).error(AppErrorKind::BackendClientCreationFailed)?;
    if let Some(backend) = existing_backend {
        let ping_response = janus_client
            .service_ping(ServicePingRequest {
                session_id: backend.session_id(),
                handle_id: backend.handle_id(),
                body: ServicePingRequestBody::new(),
            })
            .await;
        if ping_response.is_ok() {
            context
                .janus_clients()
                .get_or_insert(&backend)
                .error(AppErrorKind::BackendClientCreationFailed)?;

            return Ok(());
        }
    }

    let session = janus_client
        .create_session()
        .await
        .context("CreateSession")
        .error(AppErrorKind::BackendRequestFailed)?;
    let handle = janus_client
        .create_handle(CreateHandleRequest {
            session_id: session.id,
            opaque_id: None,
        })
        .await
        .context("Create first handle")
        .error(AppErrorKind::BackendRequestFailed)?;
    janus_client
        .service_ping(ServicePingRequest {
            session_id: session.id,
            handle_id: handle.id,
            body: ServicePingRequestBody::new(),
        })
        .await
        .error(AppErrorKind::BackendRequestFailed)?;

    let backend_id = backend_id.clone();
    let conn = context.get_conn().await?;

    let backend = task::spawn_blocking(move || {
        let mut q = janus_backend::UpsertQuery::new(&backend_id, handle.id, session.id, &janus_url);

        if let Some(capacity) = event.capacity {
            q = q.capacity(capacity);
        }

        if let Some(balancer_capacity) = event.balancer_capacity {
            q = q.balancer_capacity(balancer_capacity);
        }

        if let Some(group) = event.group.as_deref() {
            q = q.group(group);
        }

        q.execute(&conn)
    })
    .await?;
    context
        .janus_clients()
        .get_or_insert(&backend)
        .error(AppErrorKind::BackendClientCreationFailed)?;
    Ok(())
}

async fn handle_offline(
    existing_backend: Option<janus_backend::Object>,
    context: &mut impl Context,
) -> Result<MessageStream, AppError> {
    if let Some(backend) = existing_backend {
        let conn = context.get_conn().await?;
        let backend_id = backend.id().clone();
        let streams_with_rtc = task::spawn_blocking(move || {
            conn.transaction::<_, AppError, _>(|| {
                let streams_with_rtc = janus_rtc_stream::ListWithRtcQuery::new()
                    .active(true)
                    .backend_id(&backend_id)
                    .execute(&conn)?;
                agent_connection::BulkDisconnectByBackendQuery::new(&backend_id).execute(&conn)?;
                janus_backend::DeleteQuery::new(&backend_id).execute(&conn)?;
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
        context.janus_clients().remove_client(&backend);
        Ok(Box::new(stream::from_iter(events)))
    } else {
        Ok(Box::new(stream::empty()))
    }
}

////////////////////////////////////////////////////////////////////////////////
pub mod client;
pub mod client_pool;
pub mod metrics;

#[cfg(test)]
mod test {
    use std::time::Duration;

    use rand::Rng;

    use crate::{
        backend::janus::{
            client::service_ping::{ServicePingRequest, ServicePingRequestBody},
            handle_offline,
        },
        db,
        test_helpers::{
            authz::TestAuthz,
            context::TestContext,
            db::TestDb,
            prelude::{GlobalContext, TestAgent},
            shared_helpers,
            test_deps::LocalDeps,
            SVC_AUDIENCE,
        },
    };

    use super::{handle_online, StatusEvent};

    #[async_std::test]
    async fn test_online_when_backends_absent() -> anyhow::Result<()> {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let janus = local_deps.run_janus();
        let db = TestDb::with_local_postgres(&postgres);
        let mut context = TestContext::new(db, TestAuthz::new());
        let (tx, _rx) = crossbeam_channel::unbounded();
        context.with_janus(tx);
        let rng = rand::thread_rng();
        let label_suffix: String = rng
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(5)
            .collect();
        let label = format!("janus-gateway-{}", label_suffix);
        let backend_id = TestAgent::new("alpha", &label, SVC_AUDIENCE);
        let event = StatusEvent {
            online: true,
            capacity: Some(1),
            balancer_capacity: Some(2),
            group: None,
            janus_url: Some(janus.url.clone()),
        };

        handle_online(event, backend_id.agent_id(), None, &mut context).await?;

        let conn = context.get_conn().await?;
        let backend = db::janus_backend::FindQuery::new()
            .id(backend_id.agent_id())
            .execute(&conn)?
            .unwrap();
        // check if handle expired by timeout;
        async_std::task::sleep(Duration::from_secs(2)).await;
        let _ping_response = context
            .janus_clients()
            .get_or_insert(&backend)?
            .service_ping(ServicePingRequest {
                body: ServicePingRequestBody::new(),
                handle_id: backend.handle_id(),
                session_id: backend.session_id(),
            })
            .await?;
        context.janus_clients().remove_client(&backend);
        Ok(())
    }

    #[async_std::test]
    async fn test_online_when_backends_present() -> anyhow::Result<()> {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let janus = local_deps.run_janus();
        let db = TestDb::with_local_postgres(&postgres);
        let mut context = TestContext::new(db, TestAuthz::new());
        let conn = context.get_conn().await?;
        let (tx, _rx) = crossbeam_channel::unbounded();
        context.with_janus(tx);
        let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
        let backend =
            shared_helpers::insert_janus_backend(&conn, &janus.url, session_id, handle_id);
        let event = StatusEvent {
            online: true,
            capacity: Some(1),
            balancer_capacity: Some(2),
            group: None,
            janus_url: Some(janus.url.clone()),
        };

        handle_online(event, backend.id(), Some(backend.clone()), &mut context).await?;

        let new_backend = db::janus_backend::FindQuery::new()
            .id(backend.id())
            .execute(&conn)?
            .unwrap();
        assert_eq!(backend, new_backend);
        context.janus_clients().remove_client(&backend);
        context.janus_clients().remove_client(&new_backend);
        Ok(())
    }

    #[async_std::test]
    async fn test_online_after_offline() -> anyhow::Result<()> {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let janus = local_deps.run_janus();
        let db = TestDb::with_local_postgres(&postgres);
        let mut context = TestContext::new(db, TestAuthz::new());
        let conn = context.get_conn().await?;
        let (tx, _rx) = crossbeam_channel::unbounded();
        context.with_janus(tx);
        let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
        let backend =
            shared_helpers::insert_janus_backend(&conn, &janus.url, session_id, handle_id);
        let event = StatusEvent {
            online: true,
            capacity: Some(1),
            balancer_capacity: Some(2),
            group: None,
            janus_url: Some(janus.url.clone()),
        };

        let _ = handle_offline(Some(backend.clone()), &mut context).await?;
        handle_online(event, backend.id(), None, &mut context).await?;

        let new_backend = db::janus_backend::FindQuery::new()
            .id(backend.id())
            .execute(&conn)?
            .unwrap();
        assert_ne!(backend, new_backend);
        // check if handle expired by timeout;
        async_std::task::sleep(Duration::from_secs(2)).await;
        let _ping_response = context
            .janus_clients()
            .get_or_insert(&new_backend)?
            .service_ping(ServicePingRequest {
                body: ServicePingRequestBody::new(),
                handle_id: new_backend.handle_id(),
                session_id: new_backend.session_id(),
            })
            .await?;
        context.janus_clients().remove_client(&backend);
        context.janus_clients().remove_client(&new_backend);
        Ok(())
    }
}
