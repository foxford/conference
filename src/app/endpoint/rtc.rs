use anyhow::{anyhow, Context as AnyhowContext};
use async_trait::async_trait;
use axum::{
    extract::{Extension, Path, Query},
    Json,
};
use chrono::Duration;

use either::Either;
use serde::{Deserialize, Serialize};
use sqlx::Connection as SqlxConnection;
use std::{fmt, sync::Arc};
use svc_agent::{mqtt::ResponseStatus, Addressable, AgentId, Authenticable};
use svc_utils::extractors::AgentIdExtractor;

use tracing::{warn, Span};

use crate::{
    app::{
        context::{AppContext, Context, GlobalContext, MessageContext},
        endpoint::prelude::*,
        endpoint::{self, rtc_signal::start_rtc_stream},
        handle_id::HandleId,
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
    },
    authz::AuthzObject,
    backend::janus::client::{
        create_handle::{CreateHandleRequest, OpaqueId},
        create_stream::{
            CreateStreamRequest, CreateStreamRequestBody, CreateStreamTransaction, ReaderConfig,
            WriterConfig,
        },
        read_stream::{ReadStreamRequest, ReadStreamRequestBody, ReadStreamTransaction},
        Jsep, JsonSdp,
    },
    db::{self, agent, agent_connection, rtc::SharingPolicy as RtcSharingPolicy},
    diesel::Identifiable,
};
use tracing_attributes::instrument;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectResponseData {
    handle_id: HandleId,
}

impl ConnectResponseData {
    pub fn new(handle_id: HandleId) -> Self {
        Self { handle_id }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct CreateRequest {
    room_id: db::room::Id,
}

pub async fn create(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = CreateRequest { room_id };
    CreateHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct CreateHandler;

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;
    const ERROR_TITLE: &'static str = "Failed to create rtc";

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let RtcCreateResult {
            rtc,
            authz_time,
            notification_label,
            notification_topic,
        } = RtcCreate {
            ctx: context,
            room: Either::Right(payload.room_id),
            reqp,
        }
        .run()
        .await?;

        Span::current().record("rtc_id", rtc.id().to_string().as_str());

        // Respond and broadcast to the room topic.
        let mut response = Response::new(
            ResponseStatus::CREATED,
            rtc.clone(),
            context.start_timestamp(),
            Some(authz_time),
        );

        response.add_notification(
            notification_label,
            &notification_topic,
            rtc,
            context.start_timestamp(),
        );
        context
            .metrics()
            .request_duration
            .rtc_create
            .observe_timestamp(context.start_timestamp());

        Ok(response)
    }
}

pub struct RtcCreate<'a, C: ?Sized> {
    pub ctx: &'a C,
    pub room: Either<db::room::Object, db::room::Id>,
    pub reqp: RequestParams<'a>,
}

pub struct RtcCreateResult {
    pub rtc: db::rtc::Object,
    pub authz_time: Duration,
    pub notification_label: &'static str,
    pub notification_topic: String,
}

impl<'a, C: GlobalContext + ?Sized> RtcCreate<'a, C> {
    pub async fn run(self) -> Result<RtcCreateResult, AppError> {
        let room = match self.room {
            Either::Left(room) => room,
            Either::Right(room_id) => {
                let mut conn = self.ctx.get_conn_sqlx().await?;
                helpers::find_room_by_id(room_id, helpers::RoomTimeRequirement::Open, &mut conn)
                    .await?
            }
        };

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize room creation.
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id, "rtcs"]).into();

        let authz_time = self
            .ctx
            .authz()
            .authorize(room.audience().into(), self.reqp, object, "create".into())
            .await?;

        // Create an rtc.
        let mut conn = self.ctx.get_conn_sqlx().await?;
        let rtc = db::rtc::InsertQuery::new(room.id(), self.reqp.as_agent_id())
            .execute(&mut conn)
            .await?;

        let notification_topic = format!("rooms/{}/events", rtc.room_id());
        Ok(RtcCreateResult {
            rtc,
            authz_time,
            notification_label: "rtc.create",
            notification_topic,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct ReadRequest {
    id: db::rtc::Id,
}

pub async fn read(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(rtc_id): Path<db::rtc::Id>,
) -> RequestResult {
    tracing::Span::current().record("rtc_id", &tracing::field::display(rtc_id));

    let request = ReadRequest { id: rtc_id };
    ReadHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct ReadHandler;

#[async_trait]
impl RequestHandler for ReadHandler {
    type Payload = ReadRequest;
    const ERROR_TITLE: &'static str = "Failed to read rtc";

    #[instrument(skip(context, payload, reqp), fields(room_id = %payload.id))]
    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let room = {
            let mut conn = context.get_conn_sqlx().await?;
            helpers::find_room_by_rtc_id(payload.id, helpers::RoomTimeRequirement::Open, &mut conn)
                .await?
        };

        tracing::Span::current().record("room_id", &tracing::field::display(room.id()));
        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize rtc reading.
        let rtc_id = payload.id.to_string();
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id, "rtcs", &rtc_id]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "read".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        // Return rtc.
        let mut conn = context.get_conn_sqlx().await?;
        let rtc = db::rtc::FindQuery::new(payload.id)
            .execute(&mut conn)
            .await?
            .context("RTC not found")
            .error(AppErrorKind::RtcNotFound)?;

        context
            .metrics()
            .request_duration
            .rtc_read
            .observe_timestamp(context.start_timestamp());

        Ok(Response::new(
            ResponseStatus::OK,
            rtc,
            context.start_timestamp(),
            Some(authz_time),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 25;

#[derive(Debug, Deserialize)]
pub struct ListRequest {
    room_id: db::room::Id,
    offset: Option<i64>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    offset: Option<i64>,
    limit: Option<i64>,
}

pub async fn list(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(room_id): Path<db::room::Id>,
    query: Option<Query<ListParams>>,
) -> RequestResult {
    tracing::Span::current().record("room_id", &tracing::field::display(room_id));

    let request = match query {
        Some(x) => ListRequest {
            room_id,
            offset: x.offset,
            limit: x.limit,
        },
        None => ListRequest {
            room_id,
            offset: None,
            limit: None,
        },
    };
    ListHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct ListHandler;

#[async_trait]
impl RequestHandler for ListHandler {
    type Payload = ListRequest;
    const ERROR_TITLE: &'static str = "Failed to list rtcs";

    #[instrument(skip(context, payload, reqp), fields(room_id = %payload.room_id))]
    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let mut conn = context.get_conn_sqlx().await?;
        let room = helpers::find_room_by_id(
            payload.room_id,
            helpers::RoomTimeRequirement::Open,
            &mut conn,
        )
        .await?;

        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize rtc listing.
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id, "rtcs"]).into();

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, "list".into())
            .await?;
        context.metrics().observe_auth(authz_time);

        // Return rtc list.
        let mut conn = context.get_conn_sqlx().await?;
        let mut query = db::rtc::ListQuery::new().room_id(payload.room_id);

        if let Some(offset) = payload.offset {
            query = query.offset(offset);
        }

        let limit = std::cmp::min(payload.limit.unwrap_or(MAX_LIMIT), MAX_LIMIT);
        query = query.limit(limit);

        let rtcs = query.execute(&mut conn).await?;

        context
            .metrics()
            .request_duration
            .rtc_list
            .observe_timestamp(context.start_timestamp());

        Ok(Response::new(
            ResponseStatus::OK,
            rtcs,
            context.start_timestamp(),
            Some(authz_time),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ConnectIntent {
    Read,
    Write,
}

impl fmt::Display for ConnectIntent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Read => write!(f, "read"),
            Self::Write => write!(f, "write"),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ConnectRequest {
    id: db::rtc::Id,
    #[serde(default = "ConnectRequest::default_intent")]
    intent: ConnectIntent,
}

impl ConnectRequest {
    fn default_intent() -> ConnectIntent {
        ConnectIntent::Read
    }
}

#[derive(Debug, Deserialize)]
pub struct ConnectAndSignalPayload {
    #[serde(default = "ConnectRequest::default_intent")]
    intent: ConnectIntent,
    jsep: JsonSdp,
    label: Option<String>,
}

#[derive(Serialize)]
pub struct ConnectAndSignalResult {
    handle_id: HandleId,
    jsep: Option<serde_json::Value>,
}

#[instrument(skip(ctx, payload), fields(
    rtc_id = %rtc_id,
    intent = %payload.intent,
))]
pub async fn connect_and_signal(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(rtc_id): Path<db::rtc::Id>,
    Json(payload): Json<ConnectAndSignalPayload>,
) -> RequestResult {
    let ctx = &mut ctx.start_message();

    let response = ConnectAndSignal {
        ctx,
        rtc_id,
        intent: payload.intent,
        agent_id,
        jsep: payload.jsep,
        label: payload.label,
    }
    .run()
    .await?;

    Ok(Response::new(
        ResponseStatus::OK,
        response,
        ctx.start_timestamp(),
        None,
    ))
}

struct ConnectAndSignal<'a, C> {
    ctx: &'a mut C,
    rtc_id: db::rtc::Id,
    intent: ConnectIntent,
    agent_id: AgentId,
    jsep: JsonSdp,
    label: Option<String>,
}

impl<C> ConnectAndSignal<'_, C>
where
    C: Context,
{
    async fn authz(&self, room: &db::room::Object) -> Result<(), AppError> {
        let rtc_id = self.rtc_id.to_string();
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id, "rtcs", &rtc_id]).into();

        let action = match self.intent {
            ConnectIntent::Read => "read",
            ConnectIntent::Write => "update",
        };

        let authz_time = self
            .ctx
            .authz()
            .authorize(
                room.audience().into(),
                self.agent_id.clone(),
                object,
                action.into(),
            )
            .await?;
        self.ctx.metrics().observe_auth(authz_time);

        Ok(())
    }

    async fn check_room_policy(&self, room: &db::room::Object) -> Result<(), AppError> {
        // Authorize connecting to the rtc.
        match room.rtc_sharing_policy() {
            RtcSharingPolicy::None => {
                return Err(anyhow!(
                    "'rtc.connect' is not implemented for rtc_sharing_policy = '{}'",
                    room.rtc_sharing_policy(),
                ))
                .error(AppErrorKind::NotImplemented);
            }
            RtcSharingPolicy::Shared => (),
            RtcSharingPolicy::Owned => {
                if self.intent == ConnectIntent::Write {
                    // Check that the RTC is owned by the same agent.
                    let mut conn = self.ctx.get_conn_sqlx().await?;
                    let rtc_object = db::rtc::FindQuery::new(self.rtc_id)
                        .execute(&mut conn)
                        .await?
                        .context("RTC not found")
                        .error(AppErrorKind::RtcNotFound)?;

                    if *rtc_object.created_by() != self.agent_id {
                        return Err(anyhow!("RTC doesn't belong to the agent"))
                            .error(AppErrorKind::AccessDenied);
                    }
                }
            }
        }

        Ok(())
    }

    async fn fetch_reader_writer_configs(
        &self,
        handle_id: &HandleId,
    ) -> Result<
        (
            Option<db::rtc_writer_config::Object>,
            Vec<db::rtc_reader_config::Object>,
        ),
        AppError,
    > {
        let mut conn = self.ctx.get_conn_sqlx().await?;
        let reader_config =
            db::rtc_reader_config::read_config(handle_id.rtc_id(), &mut conn).await?;
        let writer_config =
            db::rtc_writer_config::read_config(handle_id.rtc_id(), &mut conn).await?;

        Ok((writer_config, reader_config))
    }

    async fn run(self) -> Result<ConnectAndSignalResult, AppError> {
        let payload_id = self.rtc_id;
        let room = {
            let mut conn = self.ctx.get_conn_sqlx().await?;
            helpers::find_room_by_rtc_id(payload_id, helpers::RoomTimeRequirement::Open, &mut conn)
                .await?
        };

        tracing::Span::current().record("rtc_id", &tracing::field::display(self.rtc_id));
        tracing::Span::current().record("room_id", &tracing::field::display(room.id()));
        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        tokio::try_join!(self.check_room_policy(&room), self.authz(&room))?;

        let mut conn = self.ctx.get_conn_sqlx().await?;
        let group = self.ctx.config().janus_group.clone();
        // There are 4 cases:
        // 1. Connecting as a writer for a webinar for the first time. There's no `backend_id` in that case.
        //    Select the most loaded backend that is capable to host the room's reservation.
        //    If there's no capable backend then select the least loaded and send a warning
        //    to Sentry. If there are no backends at all then return `no available backends`
        // 2. Connecting as a writer for a minigroup for the first time. There's no `backend_id` in that case.
        //    Select the least loaded backend and fallback on most loaded. Minigroups have a fixed size,
        //    that is why least loaded should work fine.
        // 3. Connecting as reader with existing `backend_id`. Choose it because Janus doesn't
        //    support clustering and it must be the same server that the writer is connected to.
        // 4. Reconnecting as writer with existing `backend_id`. Select it to avoid partitioning
        //    of the record across multiple servers.
        let backend = match room.backend_id() {
                Some(backend_id) => db::janus_backend::FindQuery::new(backend_id)
                    .execute(&mut conn)
                    .await?
                    .context("No backend found for stream")
                    .error(AppErrorKind::BackendNotFound)?,
                None if group.as_deref() == Some("minigroup") => {
                    let b = db::janus_backend::least_loaded(room.id(), group.as_deref(), &mut conn).await.transpose();
                    let b = match b {
                        Some(b) => Some(b),
                        None => {
                            db::janus_backend::most_loaded(room.id(), group.as_deref(), &mut conn).await.transpose()
                        }
                    };

                    b
                        .context("No available backends")
                        .error(AppErrorKind::NoAvailableBackends)??
                }
                None => match db::janus_backend::most_loaded(room.id(), group.as_deref(), &mut conn).await? {
                    Some(backend) => backend,
                    None => db::janus_backend::least_loaded(room.id(), group.as_deref(), &mut conn).await?
                        .map(|backend| {
                            use sentry::protocol::{value::Value, Event, Level};
                            let backend_id = backend.id().to_string();

                            warn!(%backend_id, "No capable backends to host the reserve; falling back to the least loaded backend");

                            let mut extra = std::collections::BTreeMap::new();
                            extra.insert(String::from("room_id"), Value::from(room.id().to_string()));
                            extra.insert(String::from("rtc_id"), Value::from(self.rtc_id.to_string()));
                            extra.insert(String::from("backend_id"), Value::from(backend_id));

                            if let Some(reserve) = room.reserve() {
                                extra.insert(String::from("reserve"), Value::from(reserve));
                            }

                            sentry::capture_event(Event {
                                message: Some(String::from("No capable backends to host the reserve; falling back to the least loaded backend")),
                                level: Level::Warning,
                                extra,
                                ..Default::default()
                            });

                            backend
                        })
                        .context("No available backends")
                        .error(AppErrorKind::NoAvailableBackends)?,
                },
            };

        match self.intent {
            ConnectIntent::Read => {
                // Check that the backend's capacity is not exceeded for readers.
                if db::janus_backend::free_capacity(self.rtc_id, &mut conn).await? == 0 {
                    return Err(anyhow!(
                        "Active agents number on the backend exceeded its capacity"
                    ))
                    .error(AppErrorKind::CapacityExceeded);
                }
            }
            ConnectIntent::Write => {
                if room.backend_id().is_none() {
                    db::room::UpdateQuery::new(room.id())
                        .backend_id(Some(backend.id()))
                        .execute(&mut conn)
                        .await?;
                }
            }
        }

        let rtc_stream_id = db::janus_rtc_stream::Id::random();

        let handle = self
            .ctx
            .janus_clients()
            .get_or_insert(&backend)
            .error(AppErrorKind::BackendClientCreationFailed)?
            .create_handle(CreateHandleRequest {
                session_id: backend.session_id(),
                opaque_id: Some(OpaqueId {
                    room_id: room.id(),
                    stream_id: rtc_stream_id,
                }),
            })
            .await
            .context("Handle creating")
            .error(AppErrorKind::BackendRequestFailed)?;

        let agent_id = self.agent_id.clone();
        let handle_id = handle.id;
        let room_id = room.id();

        let mut conn = self.ctx.get_conn_sqlx().await?;
        conn.transaction(|conn| {
            Box::pin(async move {
                // Find agent in the DB who made the original `rtc.connect` request.
                let maybe_agent = agent::ListQuery::new()
                    .agent_id(&agent_id)
                    .room_id(room_id)
                    .status(agent::Status::Ready)
                    .limit(1)
                    .execute(conn)
                    .await?;

                if let Some(agent) = maybe_agent.first() {
                    // Create agent connection in the DB.
                    agent_connection::UpsertQuery::new(*agent.id(), payload_id, handle_id)
                        .execute(conn)
                        .await?;

                    Ok(())
                } else {
                    // Agent may be already gone.
                    Err(anyhow!("Agent not found")).error(AppErrorKind::AgentNotEnteredTheRoom)
                }
            })
        })
        .await?;

        let handle_id = HandleId::new(
            rtc_stream_id,
            payload_id,
            handle.id,
            backend.session_id(),
            backend.id().clone(),
        );

        let current_span = Span::current();
        current_span.record("sdp_type", "offer");
        let is_recvonly = endpoint::rtc_signal::is_sdp_recvonly(self.jsep.sdp.as_str())
            .context("Invalid JSEP format")
            .error(AppErrorKind::InvalidJsepFormat)?;

        let jsep = Jsep::OfferOrAnswer(self.jsep.clone());

        let answer = if is_recvonly {
            let mut conn = self.ctx.get_conn_sqlx().await?;
            let reader_config =
                db::rtc_reader_config::read_config(handle_id.rtc_id(), &mut conn).await?;

            let request = ReadStreamRequest {
                body: ReadStreamRequestBody::new(
                    handle_id.rtc_id(),
                    self.agent_id.clone(),
                    reader_config
                        .into_iter()
                        .map(|r| ReaderConfig {
                            reader_id: r.reader_id().to_owned(),
                            receive_audio: r.receive_audio(),
                            receive_video: r.receive_video(),
                        })
                        .collect(),
                ),
                handle_id: handle_id.janus_handle_id(),
                session_id: handle_id.janus_session_id(),
                jsep,
            };

            let handle = self
                .ctx
                .janus_clients()
                .stream_waitlist()
                .register()
                .error(AppErrorKind::JanusResponseTimeout)?;

            let transaction = ReadStreamTransaction::Http {
                id: handle.id(),
                replica_addr: self.ctx.janus_clients().own_ip_addr(),
            };
            self.ctx
                .janus_clients()
                .get_or_insert(&backend)
                .error(AppErrorKind::BackendClientCreationFailed)?
                .read_stream(request, transaction)
                .await
                .error(AppErrorKind::BackendRequestFailed)?;

            let resp = handle
                .wait(self.ctx.config().waitlist_timeout)
                .await
                .error(AppErrorKind::JanusResponseTimeout)??;

            resp.jsep
        } else {
            // We've checked that the RTC is owned by the same agent earlier.

            let (_, (writer_config, reader_config)) = tokio::try_join!(
                start_rtc_stream(self.ctx, &handle_id, &self.agent_id, &self.label, &room),
                self.fetch_reader_writer_configs(&handle_id),
            )?;

            let agent_id = self.agent_id.to_owned();
            let request = CreateStreamRequest {
                body: CreateStreamRequestBody::new(
                    handle_id.rtc_id(),
                    agent_id,
                    writer_config.map(|w| WriterConfig {
                        send_video: w.send_video(),
                        send_audio: w.send_audio(),
                        video_remb: w.video_remb(),
                    }),
                    reader_config
                        .into_iter()
                        .map(|r| ReaderConfig {
                            reader_id: r.reader_id().to_owned(),
                            receive_audio: r.receive_audio(),
                            receive_video: r.receive_video(),
                        })
                        .collect(),
                ),
                handle_id: handle_id.janus_handle_id(),
                session_id: handle_id.janus_session_id(),
                jsep,
            };

            let handle = self
                .ctx
                .janus_clients()
                .stream_waitlist()
                .register()
                .error(AppErrorKind::JanusResponseTimeout)?;

            let transaction = CreateStreamTransaction::Http {
                id: handle.id(),
                replica_addr: self.ctx.janus_clients().own_ip_addr(),
            };
            self.ctx
                .janus_clients()
                .get_or_insert(&backend)
                .error(AppErrorKind::BackendClientCreationFailed)?
                .create_stream(request, transaction)
                .await
                .error(AppErrorKind::BackendRequestFailed)?;

            let resp = handle
                .wait(self.ctx.config().waitlist_timeout)
                .await
                .error(AppErrorKind::JanusResponseTimeout)??;

            resp.jsep
        };

        // Create recording if a writer connects for the first time.
        // We run it after successful signaling to avoid in-progress recording entries
        // which are not really bound to anything.
        if let ConnectIntent::Write = self.intent {
            let id = self.rtc_id;
            let mut conn = self.ctx.get_conn_sqlx().await?;

            conn.transaction::<_, _, AppError>(|conn| {
                Box::pin(async move {
                    let recording = db::recording::FindQuery::new(id).execute(conn).await?;

                    if recording.is_none() {
                        db::recording::InsertQuery::new(id).execute(conn).await?;
                    }

                    Ok(())
                })
            })
            .await?;
        }

        self.ctx
            .metrics()
            .request_duration
            .rtc_connect
            .observe_timestamp(self.ctx.start_timestamp());

        Ok(ConnectAndSignalResult {
            handle_id,
            jsep: answer,
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct ConnectPayload {
    #[serde(default = "ConnectRequest::default_intent")]
    intent: ConnectIntent,
    #[serde(default)]
    agent_label: Option<String>,
}

pub async fn connect(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Path(rtc_id): Path<db::rtc::Id>,
    Json(intent): Json<ConnectPayload>,
) -> RequestResult {
    tracing::Span::current().record("rtc_id", &tracing::field::display(rtc_id));

    let request = ConnectRequest {
        id: rtc_id,
        intent: intent.intent,
    };
    let agent_id = intent
        .agent_label
        .as_ref()
        .map(|label| AgentId::new(label, agent_id.as_account_id().to_owned()))
        .unwrap_or(agent_id);

    ConnectHandler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct ConnectHandler;

#[async_trait]
impl RequestHandler for ConnectHandler {
    type Payload = ConnectRequest;
    const ERROR_TITLE: &'static str = "Failed to connect to rtc";

    #[instrument(skip(context, payload, reqp), fields(
        rtc_id = %payload.id,
        intent = %payload.intent,
    ))]
    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let room = {
            let mut conn = context.get_conn_sqlx().await?;
            helpers::find_room_by_rtc_id(payload.id, helpers::RoomTimeRequirement::Open, &mut conn)
                .await?
        };

        tracing::Span::current().record("room_id", &tracing::field::display(room.id()));
        tracing::Span::current().record(
            "classroom_id",
            &tracing::field::display(room.classroom_id()),
        );

        // Authorize connecting to the rtc.
        match room.rtc_sharing_policy() {
            RtcSharingPolicy::None => {
                return Err(anyhow!(
                    "'rtc.connect' is not implemented for rtc_sharing_policy = '{}'",
                    room.rtc_sharing_policy(),
                ))
                .error(AppErrorKind::NotImplemented);
            }
            RtcSharingPolicy::Shared => (),
            RtcSharingPolicy::Owned => {
                if payload.intent == ConnectIntent::Write {
                    let mut conn = context.get_conn_sqlx().await?;
                    // Check that the RTC is owned by the same agent.
                    let rtc = db::rtc::FindQuery::new(payload.id)
                        .execute(&mut conn)
                        .await?
                        .context("RTC not found")
                        .error(AppErrorKind::RtcNotFound)?;

                    if rtc.created_by() != reqp.as_agent_id() {
                        return Err(anyhow!("RTC doesn't belong to the agent"))
                            .error(AppErrorKind::AccessDenied);
                    }
                }
            }
        }

        let rtc_id = payload.id.to_string();
        let classroom_id = room.classroom_id().to_string();
        let object = AuthzObject::new(&["classrooms", &classroom_id, "rtcs", &rtc_id]).into();

        let action = match payload.intent {
            ConnectIntent::Read => "read",
            ConnectIntent::Write => "update",
        };

        let authz_time = context
            .authz()
            .authorize(room.audience().into(), reqp, object, action.into())
            .await?;
        context.metrics().observe_auth(authz_time);
        let room_id = room.id();

        // Choose backend to connect.
        let mut conn = context.get_conn_sqlx().await?;
        let group = context.config().janus_group.clone();
        // There are 4 cases:
        // 1. Connecting as a writer for a webinar for the first time. There's no `backend_id` in that case.
        //    Select the most loaded backend that is capable to host the room's reservation.
        //    If there's no capable backend then select the least loaded and send a warning
        //    to Sentry. If there are no backends at all then return `no available backends`
        // 2. Connecting as a writer for a minigroup for the first time. There's no `backend_id` in that case.
        //    Select the least loaded backend and fallback on most loaded. Minigroups have a fixed size,
        //    that is why least loaded should work fine.
        // 3. Connecting as reader with existing `backend_id`. Choose it because Janus doesn't
        //    support clustering and it must be the same server that the writer is connected to.
        // 4. Reconnecting as writer with existing `backend_id`. Select it to avoid partitioning
        //    of the record across multiple servers.
        let backend = match room.backend_id() {
                Some(backend_id) => db::janus_backend::FindQuery::new(backend_id)
                    .execute(&mut conn)
                    .await?
                    .context("No backend found for stream")
                    .error(AppErrorKind::BackendNotFound)?,
                None if group.as_deref() == Some("minigroup") => {
                    let b = db::janus_backend::least_loaded(room.id(), group.as_deref(), &mut conn).await.transpose();
                    let b = match b {
                        Some(b) => Some(b),
                        None => {
                            db::janus_backend::most_loaded(room.id(), group.as_deref(), &mut conn).await.transpose()
                        }
                    };

                    b
                        .context("No available backends")
                        .error(AppErrorKind::NoAvailableBackends)??
                }
                None => match db::janus_backend::most_loaded(room.id(), group.as_deref(), &mut conn).await? {
                    Some(backend) => backend,
                    None => db::janus_backend::least_loaded(room.id(), group.as_deref(), &mut conn).await?
                        .map(|backend| {
                            use sentry::protocol::{value::Value, Event, Level};
                            let backend_id = backend.id().to_string();

                            warn!(%backend_id, "No capable backends to host the reserve; falling back to the least loaded backend");

                            let mut extra = std::collections::BTreeMap::new();
                            extra.insert(String::from("room_id"), Value::from(room.id().to_string()));
                            extra.insert(String::from("rtc_id"), Value::from(payload.id.to_string()));
                            extra.insert(String::from("backend_id"), Value::from(backend_id));

                            if let Some(reserve) = room.reserve() {
                                extra.insert(String::from("reserve"), Value::from(reserve));
                            }

                            sentry::capture_event(Event {
                                message: Some(String::from("No capable backends to host the reserve; falling back to the least loaded backend")),
                                level: Level::Warning,
                                extra,
                                ..Default::default()
                            });

                            backend
                        })
                        .context("No available backends")
                        .error(AppErrorKind::NoAvailableBackends)?,
                },
            };

        match payload.intent {
            ConnectIntent::Read => {
                // Check that the backend's capacity is not exceeded for readers.
                if db::janus_backend::free_capacity(payload.id, &mut conn).await? == 0 {
                    return Err(anyhow!(
                        "Active agents number on the backend exceeded its capacity"
                    ))
                    .error(AppErrorKind::CapacityExceeded);
                }
            }
            ConnectIntent::Write => {
                if room.backend_id().is_none() {
                    db::room::UpdateQuery::new(room.id())
                        .backend_id(Some(backend.id()))
                        .execute(&mut conn)
                        .await?;
                }
            }
        }

        let rtc_stream_id = db::janus_rtc_stream::Id::random();

        let handle = context
            .janus_clients()
            .get_or_insert(&backend)
            .error(AppErrorKind::BackendClientCreationFailed)?
            .create_handle(CreateHandleRequest {
                session_id: backend.session_id(),
                opaque_id: Some(OpaqueId {
                    room_id,
                    stream_id: rtc_stream_id,
                }),
            })
            .await
            .context("Handle creating")
            .error(AppErrorKind::BackendRequestFailed)?;

        let agent_id = reqp.as_agent_id().clone();
        let handle_id = handle.id;

        let mut conn = context.get_conn_sqlx().await?;
        let payload_id = payload.id;
        conn.transaction(|conn| {
            Box::pin(async move {
                // Find agent in the DB who made the original `rtc.connect` request.
                let maybe_agent = agent::ListQuery::new()
                    .agent_id(&agent_id)
                    .room_id(room_id)
                    .status(agent::Status::Ready)
                    .limit(1)
                    .execute(conn)
                    .await?;

                if let Some(agent) = maybe_agent.first() {
                    // Create agent connection in the DB.
                    agent_connection::UpsertQuery::new(*agent.id(), payload_id, handle_id)
                        .execute(conn)
                        .await?;

                    Ok(())
                } else {
                    // Agent may be already gone.
                    Err(anyhow!("Agent not found")).error(AppErrorKind::AgentNotEnteredTheRoom)
                }
            })
        })
        .await?;

        // Returning Real-Time connection handle
        let resp = Response::new(
            ResponseStatus::OK,
            endpoint::rtc::ConnectResponseData::new(HandleId::new(
                rtc_stream_id,
                payload.id,
                handle.id,
                backend.session_id(),
                backend.id().clone(),
            )),
            context.start_timestamp(),
            None,
        );
        context
            .metrics()
            .request_duration
            .rtc_connect
            .observe_timestamp(context.start_timestamp());

        Ok(resp)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod create {
        use crate::{
            db::rtc::Object as Rtc,
            test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn create() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let mut authz = TestAuthz::new();

            let mut conn = db_sqlx.get_conn().await;

            // Insert a room.
            let room = shared_helpers::insert_room(&mut conn).await;

            // Allow user to create rtcs in the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let classroom_id = room.classroom_id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs"];
            authz.allow(agent.account_id(), object, "create");

            // Make rtc.create request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let payload = CreateRequest { room_id: room.id() };

            let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect("Rtc creation failed");

            // Assert response.
            let (rtc, respp, _) = find_response::<Rtc>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::CREATED);
            assert_eq!(rtc.room_id(), room.id());

            // Assert notification.
            let (rtc, evp, topic) = find_event::<Rtc>(messages.as_slice());
            assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
            assert_eq!(evp.label(), "rtc.create");
            assert_eq!(rtc.room_id(), room.id());
        }

        #[tokio::test]
        async fn create_rtc_missing_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;
            let payload = CreateRequest {
                room_id: db::room::Id::random(),
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }

        #[tokio::test]
        async fn create_rtc_duplicate() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let mut authz = TestAuthz::new();

            let mut conn = db_sqlx.get_conn().await;

            // Insert a room.
            let room = shared_helpers::insert_room(&mut conn).await;

            // Allow user to create rtcs in the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let classroom_id = room.classroom_id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs"];
            authz.allow(agent.account_id(), object, "create");

            // Make rtc.create request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let payload = CreateRequest { room_id: room.id() };

            let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect("Rtc creation failed");

            // Assert response.
            let (rtc, respp, _) = find_response::<Rtc>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::CREATED);
            assert_eq!(rtc.room_id(), room.id());

            // Make rtc.create request second time.
            let payload = CreateRequest { room_id: room.id() };
            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc creation");

            // This should fail with already exists
            assert_eq!(err.status(), ResponseStatus::UNPROCESSABLE_ENTITY);
            assert_eq!(err.kind(), "database_query_failed");
        }

        #[tokio::test]
        async fn create_rtc_for_different_agents_with_owned_sharing_policy() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let mut authz = TestAuthz::new();

            // Insert a room.
            let mut conn = db_sqlx.get_conn().await;
            let room = shared_helpers::insert_room_with_owned(&mut conn).await;

            // Allow agents to create RTCs in the room.
            let agent1 = TestAgent::new("web", "user123", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user456", USR_AUDIENCE);
            let classroom_id = room.classroom_id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs"];
            authz.allow(agent1.account_id(), object.clone(), "create");
            authz.allow(agent2.account_id(), object, "create");

            // Make two rtc.create requests.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let payload = CreateRequest { room_id: room.id() };

            let messages1 = handle_request::<CreateHandler>(&mut context, &agent1, payload)
                .await
                .expect("RTC creation failed");

            let payload = CreateRequest { room_id: room.id() };

            let messages2 = handle_request::<CreateHandler>(&mut context, &agent2, payload)
                .await
                .expect("RTC creation failed");

            // Assert responses.
            let (rtc1, respp1, _) = find_response::<Rtc>(messages1.as_slice());
            assert_eq!(respp1.status(), ResponseStatus::CREATED);
            assert_eq!(rtc1.room_id(), room.id());
            assert_eq!(rtc1.created_by(), agent1.agent_id());

            let (rtc2, respp2, _) = find_response::<Rtc>(messages2.as_slice());
            assert_eq!(respp2.status(), ResponseStatus::CREATED);
            assert_eq!(rtc2.room_id(), room.id());
            assert_eq!(rtc2.created_by(), agent2.agent_id());
        }

        #[tokio::test]
        async fn create_rtc_for_the_same_agent_with_owned_sharing_policy() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let mut authz = TestAuthz::new();

            // Insert a room.
            let mut conn = db_sqlx.get_conn().await;
            let room = shared_helpers::insert_room_with_owned(&mut conn).await;

            // Allow agent to create RTCs in the room.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let classroom_id = room.classroom_id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs"];
            authz.allow(agent.account_id(), object, "create");

            // Make the first rtc.create request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let payload = CreateRequest { room_id: room.id() };

            let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect("RTC creation failed");

            // Assert response.
            let (rtc, respp, _) = find_response::<Rtc>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::CREATED);
            assert_eq!(rtc.room_id(), room.id());
            assert_eq!(rtc.created_by(), agent.agent_id());

            // Make the second rtc.create request and expect fail.
            let payload = CreateRequest { room_id: room.id() };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on RTC creation");

            assert_eq!(err.status(), ResponseStatus::UNPROCESSABLE_ENTITY);
            assert_eq!(err.kind(), "database_query_failed");
        }

        #[tokio::test]
        async fn create_rtc_unauthorized() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert a room.
            let mut conn = db_sqlx.get_conn().await;
            let room = shared_helpers::insert_room(&mut conn).await;

            // Make rtc.create request.
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;
            let payload = CreateRequest { room_id: room.id() };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc creation");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }
    }

    mod read {
        use crate::{
            db::rtc::Object as Rtc,
            test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn read_rtc() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (rtc, classroom_id) = {
                // Insert a room.
                let mut conn = db_sqlx.get_conn().await;
                let room = shared_helpers::insert_room_with_owned(&mut conn).await;

                // Create rtc.
                let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

                (rtc, room.classroom_id().to_string())
            };

            // Allow agent to read the rtc.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut authz = TestAuthz::new();
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc.read request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let payload = ReadRequest { id: rtc.id() };

            let messages = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect("RTC reading failed");

            // Assert response.
            let (resp_rtc, respp, _) = find_response::<Rtc>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(resp_rtc.room_id(), rtc.room_id());
        }

        #[tokio::test]
        async fn read_rtc_not_authorized() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let rtc = {
                let mut conn = db_sqlx.get_conn().await;
                shared_helpers::insert_rtc(&mut conn).await
            };

            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;
            let payload = ReadRequest { id: rtc.id() };

            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc reading");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }

        #[tokio::test]
        async fn read_rtc_missing() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;
            let payload = ReadRequest {
                id: db::rtc::Id::random(),
            };

            let err = handle_request::<ReadHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc reading");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }
    }

    mod list {
        use crate::{
            db::rtc::Object as Rtc,
            test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn list_rtcs() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let (rtc, classroom_id) = {
                // Insert a room.
                let mut conn = db_sqlx.get_conn().await;
                let room = shared_helpers::insert_room_with_owned(&mut conn).await;

                // Create rtc.
                let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

                (rtc, room.classroom_id().to_string())
            };

            // Allow agent to list rtcs in the room.
            let mut authz = TestAuthz::new();
            let object = vec!["classrooms", &classroom_id, "rtcs"];
            authz.allow(agent.account_id(), object, "list");

            // Make rtc.list request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;

            let payload = ListRequest {
                room_id: rtc.room_id(),
                offset: None,
                limit: None,
            };

            let messages = handle_request::<ListHandler>(&mut context, &agent, payload)
                .await
                .expect("Rtc listing failed");

            // Assert response.
            let (rtcs, respp, _) = find_response::<Vec<Rtc>>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(rtcs.len(), 1);
            assert_eq!(rtcs[0].id(), rtc.id());
            assert_eq!(rtcs[0].room_id(), rtc.room_id());
        }

        #[tokio::test]
        async fn list_rtcs_not_authorized() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let mut conn = db_sqlx.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = ListRequest {
                room_id: room.id(),
                offset: None,
                limit: None,
            };

            let err = handle_request::<ListHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc listing");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }

        #[tokio::test]
        async fn list_rtcs_missing_room() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = ListRequest {
                room_id: db::room::Id::random(),
                offset: None,
                limit: None,
            };

            let err = handle_request::<ListHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc listing");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }
    }

    mod connect {
        use std::ops::Bound;

        use chrono::{Duration, Utc};
        use http::StatusCode;

        use crate::{
            db::{agent::Status as AgentStatus, rtc::SharingPolicy as RtcSharingPolicy},
            test_helpers::{db_sqlx, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        #[tokio::test]
        async fn connect_to_rtc_only() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let mut authz = TestAuthz::new();
            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;

            let mut conn = db_sqlx.get_conn().await;
            // Insert janus backends.
            let backend1 =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;
            let backend2 =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            // Insert an rtc and janus backend.
            // The first backend has an active agent.
            let room1 = shared_helpers::insert_room_with_backend_id(&mut conn, backend1.id()).await;
            let rtc1 = shared_helpers::insert_rtc_with_room(&mut conn, &room1).await;
            let s1a1 = TestAgent::new("web", "s1a1", USR_AUDIENCE);

            shared_helpers::insert_connected_agent(
                &mut conn,
                s1a1.agent_id(),
                room1.id(),
                rtc1.id(),
            )
            .await;

            // The second backend has 2 agents.
            let room2 = shared_helpers::insert_room_with_backend_id(&mut conn, backend2.id()).await;
            let rtc2 = shared_helpers::insert_rtc_with_room(&mut conn, &room2).await;
            let s2a1 = TestAgent::new("web", "s2a1", USR_AUDIENCE);

            shared_helpers::insert_connected_agent(
                &mut conn,
                s2a1.agent_id(),
                room2.id(),
                rtc2.id(),
            )
            .await;

            let s2a2 = TestAgent::new("web", "s2a2", USR_AUDIENCE);

            shared_helpers::insert_connected_agent(
                &mut conn,
                s2a2.agent_id(),
                room2.id(),
                rtc2.id(),
            )
            .await;

            // The new rtc for which we will balance the stream.
            let room3 = shared_helpers::insert_room(&mut conn).await;
            let rtc3 = shared_helpers::insert_rtc_with_room(&mut conn, &room3).await;
            let s3a1 = TestAgent::new("web", "s3a1", USR_AUDIENCE);
            shared_helpers::insert_agent(&mut conn, s3a1.agent_id(), room3.id()).await;

            let classroom_id = room3.classroom_id().to_string();

            // Allow user to read the rtc.
            let rtc_id = rtc3.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(s3a1.account_id(), object, "read");

            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);
            // Make rtc.connect request.

            let payload = ConnectRequest {
                id: rtc3.id(),
                intent: ConnectIntent::Read,
            };

            let messages = handle_request::<ConnectHandler>(&mut context, &s3a1, payload)
                .await
                .expect("RTC connect failed");
            let (resp, respp, _topic) = find_response::<ConnectResponseData>(messages.as_slice());
            context.janus_clients().remove_client(&backend2);

            assert_eq!(respp.status(), StatusCode::OK);
            assert_eq!(resp.handle_id.rtc_id(), rtc3.id());
            assert_eq!(resp.handle_id.janus_session_id(), session_id);
            assert_eq!(resp.handle_id.backend_id(), backend2.id());
            assert_ne!(resp.handle_id.janus_handle_id(), handle_id);
        }

        #[tokio::test]
        async fn connect_to_ongoing_rtc() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let mut authz = TestAuthz::new();
            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;

            let mut conn = db_sqlx.get_conn().await;
            let _backend1 =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;
            let backend2 =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            // Insert an rtc and janus backend.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend2.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

            let backend = backend2;
            let classroom_id = room.classroom_id().to_string();

            // Allow user to read the rtc.
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc.connect request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = ConnectRequest {
                id: rtc.id(),
                intent: ConnectIntent::Read,
            };

            let messages = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                .await
                .expect("RTC connect failed");
            let (resp, respp, _topic) = find_response::<ConnectResponseData>(messages.as_slice());
            context.janus_clients().remove_client(&backend);

            assert_eq!(respp.status(), StatusCode::OK);
            assert_eq!(resp.handle_id.rtc_id(), rtc.id());
            assert_eq!(resp.handle_id.janus_session_id(), session_id);
            assert_eq!(resp.handle_id.backend_id(), backend.id());
            assert_ne!(resp.handle_id.janus_handle_id(), handle_id);
        }

        #[tokio::test]
        async fn connect_to_rtc_with_reservation() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();

            let mut conn = db_sqlx.get_conn().await;
            // The first backend is big enough but has some load.
            let backend1_id = {
                let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                agent.agent_id().to_owned()
            };

            let backend1 =
                factory::JanusBackend::new(backend1_id, handle_id, session_id, janus.url.clone())
                    .capacity(20)
                    .insert(&mut conn)
                    .await;

            // The second backend is too small but has no load.
            let backend2_id = {
                let agent = TestAgent::new("beta", "janus", SVC_AUDIENCE);
                agent.agent_id().to_owned()
            };

            factory::JanusBackend::new(backend2_id, handle_id, session_id, janus.url.clone())
                .capacity(5)
                .insert(&mut conn)
                .await;

            let room1 = shared_helpers::insert_room_with_backend_id(&mut conn, backend1.id()).await;
            let _rtc1 = shared_helpers::insert_rtc_with_room(&mut conn, &room1).await;

            let agent = TestAgent::new("web", "user456", SVC_AUDIENCE);
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room1.id()).await;

            let agent = TestAgent::new("web", "user456", USR_AUDIENCE);
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room1.id()).await;

            // It should balance to the first one despite of the load.
            let now = Utc::now();

            let room2 = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(now),
                    Bound::Excluded(now + Duration::hours(1)),
                ))
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .reserve(15)
                .insert(&mut conn)
                .await;
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let rtc2 = shared_helpers::insert_rtc_with_room(&mut conn, &room2).await;
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room2.id()).await;

            let (rtc, classroom_id, backend) = (rtc2, room2.classroom_id().to_string(), backend1);

            // Allow user to read the rtc.
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc.connect request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = ConnectRequest {
                id: rtc.id(),
                intent: ConnectIntent::Read,
            };

            let messages = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                .await
                .expect("RTC connect failed");
            let (resp, respp, _topic) = find_response::<ConnectResponseData>(messages.as_slice());
            context.janus_clients().remove_client(&backend);

            assert_eq!(respp.status(), StatusCode::OK);
            assert_eq!(resp.handle_id.rtc_id(), rtc.id());
            assert_eq!(resp.handle_id.janus_session_id(), session_id);
            assert_eq!(resp.handle_id.backend_id(), backend.id());
            assert_ne!(resp.handle_id.janus_handle_id(), handle_id);
        }

        #[tokio::test]
        async fn connect_to_rtc_take_reserved_slot() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            let reader1 = TestAgent::new("web", "reader1", USR_AUDIENCE);
            let reader2 = TestAgent::new("web", "reader2", USR_AUDIENCE);
            let writer1 = TestAgent::new("web", "writer1", USR_AUDIENCE);
            let writer2 = TestAgent::new("web", "writer2", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;

            // Insert backend with capacity = 4.
            let backend_id = {
                let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                agent.agent_id().to_owned()
            };

            let backend =
                factory::JanusBackend::new(backend_id, handle_id, session_id, janus.url.clone())
                    .capacity(4)
                    .insert(&mut conn)
                    .await;

            // Insert rooms: 1 with reserve = 2 and the other without reserve.
            let now = Utc::now();

            let room1 = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(now),
                    Bound::Excluded(now + Duration::hours(1)),
                ))
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .backend_id(backend.id())
                .reserve(2)
                .insert(&mut conn)
                .await;

            let room2 = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;

            // Insert rtcs.
            let rtc1 = factory::Rtc::new(room1.id()).insert(&mut conn).await;
            let rtc2 = factory::Rtc::new(room2.id()).insert(&mut conn).await;

            // Insert active agents.
            shared_helpers::insert_agent(&mut conn, writer1.agent_id(), room1.id()).await;
            shared_helpers::insert_agent(&mut conn, writer2.agent_id(), room2.id()).await;

            factory::Agent::new()
                .agent_id(reader1.agent_id())
                .room_id(room2.id())
                .status(AgentStatus::Ready)
                .insert(&mut conn)
                .await;
            factory::Agent::new()
                .agent_id(reader1.agent_id())
                .room_id(room1.id())
                .status(AgentStatus::Ready)
                .insert(&mut conn)
                .await;

            shared_helpers::insert_agent(&mut conn, reader2.agent_id(), room2.id()).await;

            let (classroom_id1, classroom_id2) = (
                room1.classroom_id().to_string(),
                room2.classroom_id().to_string(),
            );

            // Allow user to read rtcs.
            for rtc in &[(&rtc1, &classroom_id1), (&rtc2, &classroom_id2)] {
                let rtc_id = rtc.0.id().to_string();
                let object = vec!["classrooms", rtc.1, "rtcs", &rtc_id];
                authz.allow(reader1.account_id(), object, "read");
            }

            // Connect to the rtc in the room without reserve.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = ConnectRequest {
                id: rtc2.id(),
                intent: ConnectIntent::Read,
            };

            // Should be ok since we disregard reserves.
            handle_request::<ConnectHandler>(&mut context, &reader1, payload)
                .await
                .expect("RTC connect failed");

            // Delete agent
            let row_count = db::agent::DeleteQuery::new()
                .agent_id(reader1.agent_id())
                .room_id(rtc2.room_id())
                .execute(&mut conn)
                .await
                .expect("Failed to delete user from agents");
            // Check that we actually deleted something
            assert_eq!(row_count, 1);

            // Connect to the rtc in the room with free reserved slots.
            let payload = ConnectRequest {
                id: rtc1.id(),
                intent: ConnectIntent::Read,
            };

            // Expect success.
            handle_request::<ConnectHandler>(&mut context, &reader1, payload)
                .await
                .expect("RTC connect failed");
            context.janus_clients().remove_client(&backend);
        }

        #[tokio::test]
        async fn connect_to_rtc_as_last_reader() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            let writer = TestAgent::new("web", "writer", USR_AUDIENCE);
            let reader = TestAgent::new("web", "reader", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            // Insert backend.
            let backend_id = {
                let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                agent.agent_id().to_owned()
            };

            let backend =
                factory::JanusBackend::new(backend_id, handle_id, session_id, janus.url.clone())
                    .capacity(2)
                    .insert(&mut conn)
                    .await;

            // Insert room and rtc.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            // Insert active agents.
            shared_helpers::insert_agent(&mut conn, writer.agent_id(), room.id()).await;

            factory::Agent::new()
                .agent_id(reader.agent_id())
                .room_id(room.id())
                .status(AgentStatus::Ready)
                .insert(&mut conn)
                .await;

            let classroom_id = room.classroom_id().to_string();

            // Allow user to read the rtc.
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(reader.account_id(), object, "read");

            // Make rtc.connect request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = ConnectRequest {
                id: rtc.id(),
                intent: ConnectIntent::Read,
            };

            handle_request::<ConnectHandler>(&mut context, &reader, payload)
                .await
                .expect("RTC connect failed");
            context.janus_clients().remove_client(&backend);
        }

        #[tokio::test]
        async fn connect_to_rtc_full_server_as_reader() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            let writer = TestAgent::new("web", "writer", USR_AUDIENCE);
            let reader1 = TestAgent::new("web", "reader1", USR_AUDIENCE);
            let reader2 = TestAgent::new("web", "reader2", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            // Insert backend.
            let backend_id = {
                let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                agent.agent_id().to_owned()
            };

            let backend =
                factory::JanusBackend::new(backend_id, handle_id, session_id, janus.url.clone())
                    .capacity(2)
                    .insert(&mut conn)
                    .await;

            // Insert room and rtc.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            // Insert active agents.
            shared_helpers::insert_connected_agent(
                &mut conn,
                writer.agent_id(),
                room.id(),
                rtc.id(),
            )
            .await;
            shared_helpers::insert_connected_agent(
                &mut conn,
                reader1.agent_id(),
                room.id(),
                rtc.id(),
            )
            .await;

            let classroom_id = room.classroom_id().to_string();

            factory::Agent::new()
                .agent_id(reader2.agent_id())
                .room_id(room.id())
                .status(AgentStatus::Ready)
                .insert(&mut conn)
                .await;

            // Allow user to read the rtc.
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(reader2.account_id(), object, "read");

            // Make rtc.connect request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = ConnectRequest {
                id: rtc.id(),
                intent: ConnectIntent::Read,
            };

            let err = handle_request::<ConnectHandler>(&mut context, &reader2, payload)
                .await
                .expect_err("Unexpected success on rtc connecting");

            assert_eq!(err.status(), ResponseStatus::SERVICE_UNAVAILABLE);
            assert_eq!(err.kind(), "capacity_exceeded");
        }

        #[tokio::test]
        async fn connect_to_rtc_full_server_as_writer() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            let writer = TestAgent::new("web", "writer", USR_AUDIENCE);
            let reader = TestAgent::new("web", "reader", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            // Insert backend.
            let backend_id = {
                let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                agent.agent_id().to_owned()
            };

            let backend =
                factory::JanusBackend::new(backend_id, handle_id, session_id, janus.url.clone())
                    .capacity(1)
                    .insert(&mut conn)
                    .await;

            // Insert rtc.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            // Insert active agents.
            shared_helpers::insert_agent(&mut conn, reader.agent_id(), room.id()).await;

            factory::Agent::new()
                .agent_id(writer.agent_id())
                .room_id(room.id())
                .status(AgentStatus::Ready)
                .insert(&mut conn)
                .await;

            let classroom_id = room.classroom_id().to_string();

            // Allow user to update the rtc.
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(writer.account_id(), object, "update");

            // Make rtc.connect request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = ConnectRequest {
                id: rtc.id(),
                intent: ConnectIntent::Write,
            };

            handle_request::<ConnectHandler>(&mut context, &writer, payload)
                .await
                .expect("RTC connect failed");
            context.janus_clients().remove_client(&backend);
        }

        #[tokio::test]
        async fn connect_to_rtc_too_big_reserve() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            let new_writer = TestAgent::new("web", "new-writer", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            let now = Utc::now();

            // We have two backends with cap=800 and balance_cap=700 each
            // We have two rooms with reserves 500 and 600, each at its own backend
            // Room with reserve 500 has 1 writer and 2 readers, ie its load is 3
            // Room with reserve 600 has 1 writer and 1 readers, ie its load is 2
            // We want to balance a room with reserve 400
            // Since it doesnt fit anywhere it should go to backend with smallest current load,
            // ie to backend 2 (though it has only 100 free reserve, and backend1 has 200 free reserve)

            // Insert alpha and beta backends.
            let backend1 = {
                let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                let id = agent.agent_id().to_owned();
                factory::JanusBackend::new(id, handle_id, session_id, janus.url.clone())
                    .balancer_capacity(700)
                    .capacity(800)
                    .insert(&mut conn)
                    .await
            };

            let backend_beta = {
                let agent = TestAgent::new("beta", "janus", SVC_AUDIENCE);
                let id = agent.agent_id().to_owned();
                factory::JanusBackend::new(id, handle_id, session_id, janus.url.clone())
                    .balancer_capacity(700)
                    .capacity(800)
                    .insert(&mut conn)
                    .await
            };

            let backend_gamma = {
                let agent = TestAgent::new("gamma", "janus", SVC_AUDIENCE);
                let id = agent.agent_id().to_owned();
                factory::JanusBackend::new(id, handle_id, session_id, janus.url.clone())
                    .balancer_capacity(700)
                    .capacity(800)
                    .insert(&mut conn)
                    .await
            };

            let backend_delta = {
                let agent = TestAgent::new("delta", "janus", SVC_AUDIENCE);
                let id = agent.agent_id().to_owned();
                factory::JanusBackend::new(id, handle_id, session_id, janus.url.clone())
                    .balancer_capacity(700)
                    .capacity(800)
                    .insert(&mut conn)
                    .await
            };

            // Setup three rooms with 500, 600 and 400 reserves.
            let room1 = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(now - Duration::minutes(1)),
                    Bound::Excluded(now + Duration::hours(1)),
                ))
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .backend_id(backend1.id())
                .reserve(500)
                .insert(&mut conn)
                .await;

            let room2 = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(now - Duration::minutes(1)),
                    Bound::Excluded(now + Duration::hours(1)),
                ))
                .reserve(600)
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .backend_id(backend_beta.id())
                .insert(&mut conn)
                .await;

            let room3 = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(now - Duration::minutes(1)),
                    Bound::Excluded(now + Duration::hours(1)),
                ))
                .reserve(400)
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .insert(&mut conn)
                .await;

            // Insert rtcs for each room.
            let rtc1 = shared_helpers::insert_rtc_with_room(&mut conn, &room1).await;
            let rtc2 = shared_helpers::insert_rtc_with_room(&mut conn, &room2).await;
            let rtc3 = shared_helpers::insert_rtc_with_room(&mut conn, &room3).await;

            // Insert writer for room 1 @ backend 1
            let agent = TestAgent::new("web", "writer1", USR_AUDIENCE);
            shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                room1.id(),
                rtc1.id(),
            )
            .await;

            // Insert two readers for room 1 @ backend 1
            let agent = TestAgent::new("web", "reader1-1", USR_AUDIENCE);
            shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                room1.id(),
                rtc1.id(),
            )
            .await;

            let agent = TestAgent::new("web", "reader1-2", USR_AUDIENCE);
            shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                room1.id(),
                rtc1.id(),
            )
            .await;

            // Insert writer for room 2 @ backend 2
            let agent = TestAgent::new("web", "writer2", USR_AUDIENCE);
            shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                room2.id(),
                rtc2.id(),
            )
            .await;

            // Insert reader for room 2 @ backend 2
            let agent = TestAgent::new("web", "reader2", USR_AUDIENCE);
            shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                room2.id(),
                rtc2.id(),
            )
            .await;

            let classroom_id = room3.classroom_id().to_string();

            // Allow user to update the rtc.
            let rtc_id = rtc3.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(new_writer.account_id(), object, "update");

            // Make an rtc.connect request.
            // Despite none of the backends are capable to host the reserve it should
            // select the least loaded one.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = ConnectRequest {
                id: rtc3.id(),
                intent: ConnectIntent::Write,
            };

            let messages = handle_request::<ConnectHandler>(&mut context, &new_writer, payload)
                .await
                .expect("RTC connect failed");
            let (resp, respp, _topic) = find_response::<ConnectResponseData>(messages.as_slice());
            context.janus_clients().remove_client(&backend_beta);

            assert_eq!(respp.status(), StatusCode::OK);
            assert_eq!(resp.handle_id.rtc_id(), rtc3.id());
            assert_eq!(resp.handle_id.janus_session_id(), session_id);
            assert!([backend_beta.id(), backend_gamma.id(), backend_delta.id()]
                .contains(&resp.handle_id.backend_id()));
            assert_ne!(resp.handle_id.janus_handle_id(), handle_id);
        }

        #[tokio::test]
        async fn connect_to_rtc_reserve_overflow() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            let new_reader = TestAgent::new("web", "new-reader", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            let now = Utc::now();

            // Lets say we have a single backend with cap=800
            // Somehow reserves of all rooms that were allocated to it overflow its capacity
            // We should allow users to connect to rooms with reserves if reserve and cap allows them
            // But not allow to connect to room with no reserve

            // Insert alpha backend.
            let backend = {
                let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                let id = agent.agent_id().to_owned();
                factory::JanusBackend::new(id, handle_id, session_id, janus.url.clone())
                    .balancer_capacity(700)
                    .capacity(800)
                    .insert(&mut conn)
                    .await
            };

            // Setup three rooms with 500, 600 and none.
            let room1 = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(now),
                    Bound::Excluded(now + Duration::hours(1)),
                ))
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .backend_id(backend.id())
                .reserve(500)
                .insert(&mut conn)
                .await;

            let room2 = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(now),
                    Bound::Excluded(now + Duration::hours(1)),
                ))
                .reserve(600)
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .backend_id(backend.id())
                .insert(&mut conn)
                .await;

            let room3 = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(now),
                    Bound::Excluded(now + Duration::hours(1)),
                ))
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .backend_id(backend.id())
                .insert(&mut conn)
                .await;

            // Insert rtcs for each room.
            let rtc1 = shared_helpers::insert_rtc_with_room(&mut conn, &room1).await;
            let rtc2 = shared_helpers::insert_rtc_with_room(&mut conn, &room2).await;
            let rtc3 = shared_helpers::insert_rtc_with_room(&mut conn, &room3).await;

            // Insert writer for room 1
            let agent = TestAgent::new("web", "writer1", USR_AUDIENCE);
            shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                room1.id(),
                rtc1.id(),
            )
            .await;

            // Insert 450 readers for room 1
            for i in 0..450 {
                let agent = TestAgent::new("web", &format!("reader1-{}", i), USR_AUDIENCE);

                shared_helpers::insert_connected_agent(
                    &mut conn,
                    agent.agent_id(),
                    room1.id(),
                    rtc1.id(),
                )
                .await;
            }

            // Insert writer for room 3
            let agent = TestAgent::new("web", "writer3", USR_AUDIENCE);
            shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                room2.id(),
                rtc3.id(),
            )
            .await;

            // Insert reader for room 3
            let agent = TestAgent::new("web", "reader3", USR_AUDIENCE);
            shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                room3.id(),
                rtc3.id(),
            )
            .await;

            shared_helpers::insert_agent(&mut conn, new_reader.agent_id(), room1.id()).await;
            shared_helpers::insert_agent(&mut conn, new_reader.agent_id(), room2.id()).await;
            shared_helpers::insert_agent(&mut conn, new_reader.agent_id(), room3.id()).await;

            let rtcs = [
                (rtc1, room1.classroom_id().to_string()),
                (rtc2, room2.classroom_id().to_string()),
                (rtc3, room3.classroom_id().to_string()),
            ];

            // Allow user to read the rtcs.
            for rtc in rtcs.iter() {
                let rtc_id = rtc.0.id().to_string();
                let object = vec!["classrooms", &rtc.1, "rtcs", &rtc_id];
                authz.allow(new_reader.account_id(), object, "read");
            }

            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            // First two rooms have reserves AND there is free capacity so we can connect to them
            for rtc in rtcs.iter().take(2) {
                let payload = ConnectRequest {
                    id: rtc.0.id(),
                    intent: ConnectIntent::Read,
                };

                // Make an rtc.connect request.
                let messages = handle_request::<ConnectHandler>(&mut context, &new_reader, payload)
                    .await
                    .expect("RTC connect failed");
                let (resp, respp, _topic) =
                    find_response::<ConnectResponseData>(messages.as_slice());

                assert_eq!(respp.status(), StatusCode::OK);
                assert_eq!(resp.handle_id.rtc_id(), rtc.0.id());
                assert_eq!(resp.handle_id.janus_session_id(), session_id);
                assert_eq!(resp.handle_id.backend_id(), backend.id());
                assert_ne!(resp.handle_id.janus_handle_id(), handle_id);
            }

            let payload = ConnectRequest {
                id: rtcs[2].0.id(),
                intent: ConnectIntent::Read,
            };

            // Last room has NO reserve AND there is free capacity BUT it was exhausted by first two rooms
            // So in theory we should not be able to connect to this room due to capacity_exceeded error
            // But we still let the user through because:
            //   1. we almost never fill any server with users upto max capacity
            //   2. thus there are unused slots anyway
            // So its better to let them in
            handle_request::<ConnectHandler>(&mut context, &new_reader, payload)
                .await
                .expect("RTC connect failed");
            context.janus_clients().remove_client(&backend);
        }

        #[tokio::test]
        async fn connect_to_shared_rtc_created_by_someone_else() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            let backend =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            // Create an RTC.
            let now = Utc::now();
            let creator = TestAgent::new("web", "creator", USR_AUDIENCE);

            let room = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((Bound::Included(now), Bound::Unbounded))
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .backend_id(backend.id())
                .insert(&mut conn)
                .await;

            let rtc = factory::Rtc::new(room.id())
                .created_by(creator.agent_id().to_owned())
                .insert(&mut conn)
                .await;
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

            let classroom_id = room.classroom_id().to_string();

            // Allow agent to update the RTC.
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "update");

            // Make rtc.connect request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = ConnectRequest {
                id: rtc.id(),
                intent: ConnectIntent::Write,
            };

            handle_request::<ConnectHandler>(&mut context, &agent, payload)
                .await
                .expect("RTC connect failed");
            context.janus_clients().remove_client(&backend);
        }

        #[tokio::test]
        async fn connect_to_owned_rtc_created_by_someone_else_for_writing() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            let backend =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            // Create an RTC.
            let now = Utc::now();
            let creator = TestAgent::new("web", "creator", USR_AUDIENCE);

            let room = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((Bound::Included(now), Bound::Unbounded))
                .rtc_sharing_policy(RtcSharingPolicy::Owned)
                .backend_id(backend.id())
                .insert(&mut conn)
                .await;

            let rtc = factory::Rtc::new(room.id())
                .created_by(creator.agent_id().to_owned())
                .insert(&mut conn)
                .await;
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

            let classroom_id = room.classroom_id().to_string();

            // Allow agent to update the RTC.
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "update");

            // Make rtc.connect request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = ConnectRequest {
                id: rtc.id(),
                intent: ConnectIntent::Write,
            };

            let err = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on RTC connection");
            context.janus_clients().remove_client(&backend);

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }

        #[tokio::test]
        async fn connect_to_owned_rtc_created_by_someone_else_for_reading() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            let backend =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            // Create an RTC.
            let now = Utc::now();
            let creator = TestAgent::new("web", "creator", USR_AUDIENCE);

            let room = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((Bound::Included(now), Bound::Unbounded))
                .rtc_sharing_policy(RtcSharingPolicy::Owned)
                .backend_id(backend.id())
                .insert(&mut conn)
                .await;

            let rtc = factory::Rtc::new(room.id())
                .created_by(creator.agent_id().to_owned())
                .insert(&mut conn)
                .await;
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

            let classroom_id = room.classroom_id().to_string();

            // Allow agent to read the RTC.
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc.connect request.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let payload = ConnectRequest {
                id: rtc.id(),
                intent: ConnectIntent::Read,
            };

            handle_request::<ConnectHandler>(&mut context, &agent, payload)
                .await
                .expect("RTC connect failed");
            context.janus_clients().remove_client(&backend)
        }

        #[tokio::test]
        async fn connect_to_rtc_with_backend_grouping() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db_sqlx.get_conn().await;
            // Insert two backends in different groups.
            let backend1_agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);

            let backend1 = factory::JanusBackend::new(
                backend1_agent.agent_id().to_owned(),
                handle_id,
                session_id,
                janus.url.clone(),
            )
            .group("wrong")
            .insert(&mut conn)
            .await;

            let backend2_agent = TestAgent::new("beta", "janus", SVC_AUDIENCE);

            let backend2 = factory::JanusBackend::new(
                backend2_agent.agent_id().to_owned(),
                handle_id,
                session_id,
                janus.url.clone(),
            )
            .group("right")
            .insert(&mut conn)
            .await;

            // Add some load to the first backend.
            let room1 = shared_helpers::insert_room_with_backend_id(&mut conn, backend1.id()).await;
            let rtc1 = shared_helpers::insert_rtc_with_room(&mut conn, &room1).await;
            let someone = TestAgent::new("web", "user456", USR_AUDIENCE);

            shared_helpers::insert_connected_agent(
                &mut conn,
                someone.agent_id(),
                rtc1.room_id(),
                rtc1.id(),
            )
            .await;

            // Insert an RTC to connect to
            let room2 = shared_helpers::insert_room(&mut conn).await;
            let rtc2 = shared_helpers::insert_rtc_with_room(&mut conn, &room2).await;
            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room2.id()).await;

            let (rtc, backend, classroom_id) = (rtc2, backend2, room2.classroom_id().to_string());

            // Allow agent to read the RTC.
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Configure the app to the `right` janus group.
            let mut context = TestContext::new(db, db_sqlx, authz).await;
            context.config_mut().janus_group = Some(String::from("right"));
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            context.with_grouped_janus("right", tx);

            // Make rtc.connect request.
            let payload = ConnectRequest {
                id: rtc.id(),
                intent: ConnectIntent::Read,
            };

            let messages = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                .await
                .expect("RTC connect failed");
            let (resp, respp, _topic) = find_response::<ConnectResponseData>(messages.as_slice());
            context.janus_clients().remove_client(&backend);

            assert_eq!(respp.status(), StatusCode::OK);
            assert_eq!(resp.handle_id.rtc_id(), rtc.id());
            assert_eq!(resp.handle_id.janus_session_id(), session_id);
            assert_eq!(resp.handle_id.backend_id(), backend.id());
            assert_ne!(resp.handle_id.janus_handle_id(), handle_id);
        }

        #[tokio::test]
        async fn connect_to_rtc_not_authorized() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let rtc = {
                let mut conn = db_sqlx.get_conn().await;
                shared_helpers::insert_rtc(&mut conn).await
            };

            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = ConnectRequest {
                id: rtc.id(),
                intent: ConnectIntent::Read,
            };

            let err = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc connecting");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
        }

        #[tokio::test]
        async fn connect_to_rtc_missing() {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let db_sqlx = db_sqlx::TestDb::with_local_postgres(&postgres).await;

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(db, db_sqlx, TestAuthz::new()).await;

            let payload = ConnectRequest {
                id: db::rtc::Id::random(),
                intent: ConnectIntent::Read,
            };

            let err = handle_request::<ConnectHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc connecting");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        }
    }
}
