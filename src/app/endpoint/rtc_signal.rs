use crate::{
    app::{
        context::{AppContext, Context, MessageContext},
        endpoint,
        endpoint::prelude::*,
        handle_id::HandleId,
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
    },
    authz::AuthzObject,
    backend::janus::client::{
        create_stream::{
            CreateStreamRequest, CreateStreamRequestBody, CreateStreamTransaction, ReaderConfig,
            WriterConfig,
        },
        read_stream::{ReadStreamRequest, ReadStreamRequestBody, ReadStreamTransaction},
        trickle::TrickleRequest,
        IceCandidateSdp, Jsep, JsepType, JsonSdp,
    },
    db,
};
use anyhow::{anyhow, Context as AnyhowContext};
use async_trait::async_trait;
use axum::{Extension, Json};
use chrono::{Duration, Utc};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::Connection;
use std::{ops::Bound, result::Result as StdResult, sync::Arc};
use svc_agent::{
    mqtt::{OutgoingResponse, ResponseStatus},
    Addressable, AgentId, Authenticable,
};
use svc_utils::extractors::AgentIdExtractor;

use tracing::Span;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateResponseData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jsep: Option<JsonValue>,
}

impl CreateResponseData {
    pub fn new(jsep: Option<JsonValue>) -> Self {
        Self { jsep }
    }
}

pub type CreateResponse = OutgoingResponse<CreateResponseData>;

///////////////////////////////////////////////////////////////////////////////

pub async fn create(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Json(payload): Json<CreateRequest>,
) -> RequestResult {
    tracing::Span::current().record(
        "rtc_id",
        &tracing::field::display(payload.handle_id.rtc_id()),
    );

    let agent_id = payload
        .agent_label
        .as_ref()
        .map(|label| AgentId::new(label, agent_id.as_account_id().to_owned()))
        .unwrap_or(agent_id);

    CreateHandler::handle(
        &mut ctx.start_message(),
        payload,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

#[derive(Debug, Deserialize)]
pub struct CreateRequest {
    handle_id: HandleId,
    jsep: Jsep,
    label: Option<String>,
    #[serde(default)]
    agent_label: Option<String>,
}

pub struct CreateHandler;

pub async fn start_rtc_stream<C: Context>(
    ctx: &C,
    handle_id: &HandleId,
    agent_id: &AgentId,
    label: &Option<String>,
    room: &db::room::Object,
) -> Result<(), AppError> {
    let label = label
        .as_ref()
        .context("Missing label")
        .error(AppErrorKind::MessageParsingFailed)?
        .clone();

    let handle_id = handle_id.clone();
    let agent_id = agent_id.clone();
    let room = room.clone();

    let max_room_duration = ctx.config().max_room_duration;
    let mut conn = ctx.get_conn().await?;

    conn.transaction(|conn| {
        Box::pin(async move {
            if let Some(max_room_duration) = max_room_duration {
                if !room.infinite() {
                    if let (start, Bound::Unbounded) = room.time() {
                        let new_time = (
                            start,
                            Bound::Excluded(Utc::now() + Duration::hours(max_room_duration)),
                        );

                        db::room::UpdateQuery::new(room.id())
                            .time(Some(new_time))
                            .execute(conn)
                            .await?;
                    }
                }
            }

            db::janus_rtc_stream::InsertQuery::new(
                handle_id.rtc_stream_id(),
                handle_id.janus_handle_id(),
                handle_id.rtc_id(),
                handle_id.backend_id(),
                &label,
                &agent_id,
            )
            .execute(conn)
            .await
        })
    })
    .await?;

    Ok(())
}

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;
    const ERROR_TITLE: &'static str = "Failed to create rtc";

    async fn handle<C: Context + Send + Sync>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        // Validate RTC and room presence.
        let (room, rtc, backend) = {
            let agent_id = reqp.as_agent_id().clone();
            let handle_id = payload.handle_id.clone();

            let mut conn = context.get_conn().await?;

            let rtc = db::rtc::FindQuery::new(handle_id.rtc_id())
                .execute(&mut conn)
                .await?
                .context("RTC not found")
                .error(AppErrorKind::RtcNotFound)?;

            let room = helpers::find_room_by_id(
                rtc.room_id(),
                helpers::RoomTimeRequirement::Open,
                &mut conn,
            )
            .await?;

            tracing::Span::current().record("room_id", &tracing::field::display(room.id()));
            tracing::Span::current().record(
                "classroom_id",
                &tracing::field::display(room.classroom_id()),
            );

            helpers::check_room_presence(&room, reqp.as_agent_id(), &mut conn).await?;

            // Validate backend and janus session id.
            if let Some(backend_id) = room.backend_id() {
                if handle_id.backend_id() != backend_id {
                    return Err(anyhow!("Backend id specified in the handle ID doesn't match the one from the room object"))
                        .error(AppErrorKind::InvalidHandleId);
                }
            } else {
                return Err(anyhow!("Room backend not set")).error(AppErrorKind::BackendNotFound);
            }

            let janus_backend = db::janus_backend::FindQuery::new(handle_id.backend_id())
                .execute(&mut conn)
                .await?
                .context("Backend not found")
                .error(AppErrorKind::BackendNotFound)?;

            if handle_id.janus_session_id() != janus_backend.session_id() {
                return Err(anyhow!("Backend session specified in the handle ID doesn't match the one from the backend object"))
                    .error(AppErrorKind::InvalidHandleId)?;
            }

            // Validate agent connection and handle id.
            let agent_connection = db::agent_connection::FindQuery::new(&agent_id, rtc.id())
                .execute(&mut conn)
                .await?
                .context("Agent not connected")
                .error(AppErrorKind::AgentNotConnected)?;

            if handle_id.janus_handle_id() != agent_connection.handle_id() {
                return Err(anyhow!("Janus handle ID specified in the handle ID doesn't match the one from the agent connection"))
                    .error(AppErrorKind::InvalidHandleId)?;
            }

            (room, rtc, janus_backend)
        };

        match payload.jsep {
            Jsep::OfferOrAnswer(JsonSdp { kind, ref sdp }) => {
                match kind {
                    JsepType::Offer => {
                        let current_span = Span::current();
                        current_span.record("sdp_type", "offer");
                        let is_recvonly = is_sdp_recvonly(sdp)
                            .context("Invalid JSEP format")
                            .error(AppErrorKind::InvalidJsepFormat)?;

                        if is_recvonly {
                            current_span.record("intent", "read");

                            let mut conn = context.get_conn().await?;
                            let reader_config = db::rtc_reader_config::read_config(
                                payload.handle_id.rtc_id(),
                                &mut conn,
                            )
                            .await?;

                            // Authorization
                            let _authz_time =
                                authorize(context, &payload.handle_id, reqp, "read", &room).await?;

                            let request = ReadStreamRequest {
                                body: ReadStreamRequestBody::new(
                                    payload.handle_id.rtc_id(),
                                    reqp.as_agent_id().clone(),
                                    reader_config
                                        .into_iter()
                                        .map(|r| ReaderConfig {
                                            reader_id: r.reader_id().to_owned(),
                                            receive_audio: r.receive_audio(),
                                            receive_video: r.receive_video(),
                                        })
                                        .collect(),
                                ),
                                handle_id: payload.handle_id.janus_handle_id(),
                                session_id: payload.handle_id.janus_session_id(),
                                jsep: payload.jsep,
                            };

                            match reqp.as_mqtt_params() {
                                Ok(mqtt_params) => {
                                    let transaction = ReadStreamTransaction::Mqtt {
                                        reqp: mqtt_params.clone(),
                                        start_timestamp: context.start_timestamp(),
                                    };
                                    context
                                        .janus_clients()
                                        .get_or_insert(&backend)
                                        .error(AppErrorKind::BackendClientCreationFailed)?
                                        .read_stream(request, transaction)
                                        .await
                                        .error(AppErrorKind::BackendRequestFailed)?;

                                    Ok(Response::new(
                                        ResponseStatus::NO_CONTENT,
                                        json!({}),
                                        context.start_timestamp(),
                                        None,
                                    ))
                                }
                                Err(_err) => {
                                    let handle = context
                                        .janus_clients()
                                        .stream_waitlist()
                                        .register()
                                        .error(AppErrorKind::JanusResponseTimeout)?;

                                    let transaction = ReadStreamTransaction::Http {
                                        id: handle.id(),
                                        replica_addr: context.janus_clients().own_ip_addr(),
                                    };
                                    context
                                        .janus_clients()
                                        .get_or_insert(&backend)
                                        .error(AppErrorKind::BackendClientCreationFailed)?
                                        .read_stream(request, transaction)
                                        .await
                                        .error(AppErrorKind::BackendRequestFailed)?;

                                    let resp = handle
                                        .wait(context.config().waitlist_timeout)
                                        .await
                                        .error(AppErrorKind::JanusResponseTimeout)??;

                                    Ok(Response::new(
                                        ResponseStatus::OK,
                                        resp,
                                        context.start_timestamp(),
                                        None,
                                    ))
                                }
                            }
                        } else {
                            current_span.record("intent", "update");

                            if room.rtc_sharing_policy() == db::rtc::SharingPolicy::Owned
                                && reqp.as_agent_id() != rtc.created_by()
                            {
                                return Err(anyhow!("Signaling to other's RTC with sendonly or sendrecv SDP is not allowed"))
                                    .error(AppErrorKind::AccessDenied);
                            }

                            let _authz_time =
                                authorize(context, &payload.handle_id, reqp, "update", &room)
                                    .await?;

                            // Updating the Real-Time Connection state
                            start_rtc_stream(
                                context,
                                &payload.handle_id,
                                reqp.as_agent_id(),
                                &payload.label,
                                &room,
                            )
                            .await?;

                            let mut conn = context.get_conn().await?;
                            let reader_config = db::rtc_reader_config::read_config(
                                payload.handle_id.rtc_id(),
                                &mut conn,
                            )
                            .await?;

                            let writer_config = db::rtc_writer_config::read_config(
                                payload.handle_id.rtc_id(),
                                &mut conn,
                            )
                            .await?;

                            let agent_id = reqp.as_agent_id().to_owned();
                            let request = CreateStreamRequest {
                                body: CreateStreamRequestBody::new(
                                    payload.handle_id.rtc_id(),
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
                                handle_id: payload.handle_id.janus_handle_id(),
                                session_id: payload.handle_id.janus_session_id(),
                                jsep: payload.jsep,
                            };

                            match reqp.as_mqtt_params() {
                                Ok(mqtt_params) => {
                                    let transaction = CreateStreamTransaction::Mqtt {
                                        reqp: mqtt_params.clone(),
                                        start_timestamp: context.start_timestamp(),
                                    };
                                    context
                                        .janus_clients()
                                        .get_or_insert(&backend)
                                        .error(AppErrorKind::BackendClientCreationFailed)?
                                        .create_stream(request, transaction)
                                        .await
                                        .error(AppErrorKind::BackendRequestFailed)?;

                                    Ok(Response::new(
                                        ResponseStatus::NO_CONTENT,
                                        json!({}),
                                        context.start_timestamp(),
                                        None,
                                    ))
                                }
                                Err(_err) => {
                                    let handle = context
                                        .janus_clients()
                                        .stream_waitlist()
                                        .register()
                                        .error(AppErrorKind::JanusResponseTimeout)?;

                                    let transaction = CreateStreamTransaction::Http {
                                        id: handle.id(),
                                        replica_addr: context.janus_clients().own_ip_addr(),
                                    };
                                    context
                                        .janus_clients()
                                        .get_or_insert(&backend)
                                        .error(AppErrorKind::BackendClientCreationFailed)?
                                        .create_stream(request, transaction)
                                        .await
                                        .error(AppErrorKind::BackendRequestFailed)?;

                                    let resp = handle
                                        .wait(context.config().waitlist_timeout)
                                        .await
                                        .error(AppErrorKind::JanusResponseTimeout)??;

                                    Ok(Response::new(
                                        ResponseStatus::OK,
                                        resp,
                                        context.start_timestamp(),
                                        None,
                                    ))
                                }
                            }
                        }
                    }
                    JsepType::Answer => Err(anyhow!("sdp_type = 'answer' is not allowed"))
                        .error(AppErrorKind::InvalidSdpType)?,
                }
            }
            Jsep::IceCandidate(_) => {
                let current_span = Span::current();
                current_span.record("sdp_type", "ice_candidate");
                current_span.record("intent", "read");

                let _authz_time =
                    authorize(context, &payload.handle_id, reqp, "read", &room).await?;

                let request = TrickleRequest {
                    candidate: payload.jsep,
                    handle_id: payload.handle_id.janus_handle_id(),
                    session_id: payload.handle_id.janus_session_id(),
                };
                context
                    .janus_clients()
                    .get_or_insert(&backend)
                    .error(AppErrorKind::BackendClientCreationFailed)?
                    .trickle_request(request)
                    .await
                    .error(AppErrorKind::BackendRequestFailed)?;
                let response = Response::new(
                    ResponseStatus::OK,
                    endpoint::rtc_signal::CreateResponseData::new(None),
                    context.start_timestamp(),
                    None,
                );

                context
                    .metrics()
                    .request_duration
                    .rtc_signal_trickle
                    .observe_timestamp(context.start_timestamp());

                Ok(response)
            }
        }
    }
}

#[derive(Deserialize)]
pub struct TricklePayload {
    candidates: IceCandidateSdp,
    handle_id: HandleId,
}

pub async fn trickle(
    Extension(ctx): Extension<Arc<AppContext>>,
    AgentIdExtractor(agent_id): AgentIdExtractor,
    Json(payload): Json<TricklePayload>,
) -> RequestResult {
    tracing::Span::current().record(
        "rtc_id",
        &tracing::field::display(payload.handle_id.rtc_id()),
    );

    let ctx = &mut ctx.start_message();

    Trickle {
        ctx,
        handle_id: payload.handle_id,
        candidates: payload.candidates,
        agent_id,
    }
    .run()
    .await?;

    Ok(Response::new(
        ResponseStatus::OK,
        serde_json::json!({}),
        ctx.start_timestamp(),
        None,
    ))
}

struct Trickle<'a, C> {
    ctx: &'a mut C,
    handle_id: HandleId,
    candidates: IceCandidateSdp,
    agent_id: AgentId,
}

impl<C: Context> Trickle<'_, C> {
    async fn run(self) -> Result<(), AppError> {
        // Validate RTC and room presence.
        let (room, backend) = {
            let agent_id = self.agent_id.clone();
            let handle_id = self.handle_id.clone();

            let mut conn = self.ctx.get_conn().await?;
            let janus_backend = db::janus_backend::FindQuery::new(handle_id.backend_id())
                .execute(&mut conn)
                .await?
                .context("Backend not found")
                .error(AppErrorKind::BackendNotFound)?;

            let rtc = db::rtc::FindQuery::new(handle_id.rtc_id())
                .execute(&mut conn)
                .await?
                .context("RTC not found")
                .error(AppErrorKind::RtcNotFound)?;

            // Validate agent connection and handle id.
            let agent_connection = db::agent_connection::FindQuery::new(&agent_id, rtc.id())
                .execute(&mut conn)
                .await?
                .context("Agent not connected")
                .error(AppErrorKind::AgentNotConnected)?;

            if handle_id.janus_handle_id() != agent_connection.handle_id() {
                return Err(anyhow!("Janus handle ID specified in the handle ID doesn't match the one from the agent connection"))
                    .error(AppErrorKind::InvalidHandleId)?;
            }

            let room = helpers::find_room_by_id(
                rtc.room_id(),
                helpers::RoomTimeRequirement::Open,
                &mut conn,
            )
            .await?;

            tracing::Span::current().record("room_id", &tracing::field::display(room.id()));
            tracing::Span::current().record(
                "classroom_id",
                &tracing::field::display(room.classroom_id()),
            );

            helpers::check_room_presence(&room, &self.agent_id, &mut conn).await?;

            // Validate backend and janus session id.
            if let Some(backend_id) = room.backend_id() {
                if handle_id.backend_id() != backend_id {
                    return Err(anyhow!("Backend id specified in the handle ID doesn't match the one from the room object"))
                        .error(AppErrorKind::InvalidHandleId);
                }
            } else {
                return Err(anyhow!("Room backend not set")).error(AppErrorKind::BackendNotFound);
            }

            if handle_id.janus_session_id() != janus_backend.session_id() {
                return Err(anyhow!("Backend session specified in the handle ID doesn't match the one from the backend object"))
                    .error(AppErrorKind::InvalidHandleId)?;
            }

            (room, janus_backend)
        };

        let current_span = Span::current();
        current_span.record("sdp_type", "ice_candidate");
        current_span.record("intent", "read");

        let _authz_time =
            authorize(self.ctx, &self.handle_id, self.agent_id, "read", &room).await?;
        let jsep = Jsep::IceCandidate(self.candidates);

        let request = TrickleRequest {
            candidate: jsep,
            handle_id: self.handle_id.janus_handle_id(),
            session_id: self.handle_id.janus_session_id(),
        };
        self.ctx
            .janus_clients()
            .get_or_insert(&backend)
            .error(AppErrorKind::BackendClientCreationFailed)?
            .trickle_request(request)
            .await
            .error(AppErrorKind::BackendRequestFailed)?;

        self.ctx
            .metrics()
            .request_duration
            .rtc_signal_trickle
            .observe_timestamp(self.ctx.start_timestamp());

        Ok(())
    }
}

async fn authorize<A: Authenticable, C: Context>(
    context: &mut C,
    handle_id: &HandleId,
    reqp: A,
    action: &str,
    room: &db::room::Object,
) -> StdResult<Duration, AppError> {
    let rtc_id = handle_id.rtc_id();

    if room.rtc_sharing_policy() == db::rtc::SharingPolicy::None {
        let err = anyhow!(
            "'rtc_signal.create' is not implemented for the rtc_sharing_policy = '{}'.",
            room.rtc_sharing_policy()
        );

        return Err(err).error(AppErrorKind::NotImplemented);
    }

    let classroom_id = room.classroom_id().to_string();
    let rtc_id = rtc_id.to_string();
    let object = AuthzObject::new(&["classrooms", &classroom_id, "rtcs", &rtc_id]).into();

    let elapsed = context
        .authz()
        .authorize(room.audience().into(), reqp, object, action.into())
        .await?;
    context.metrics().observe_auth(elapsed);
    Ok(elapsed)
}

pub fn is_sdp_recvonly(sdp: &str) -> anyhow::Result<bool> {
    use webrtc_sdp::{attribute_type::SdpAttributeType, parse_sdp};
    let sdp = parse_sdp(sdp, false).context("Invalid SDP")?;

    // Returning true if all media section contains 'recvonly' attribute
    Ok(sdp.media.iter().all(|item| {
        let recvonly = item.get_attribute(SdpAttributeType::Recvonly).is_some();
        let sendonly = item.get_attribute(SdpAttributeType::Sendonly).is_some();
        let sendrecv = item.get_attribute(SdpAttributeType::Sendrecv).is_some();

        matches!((recvonly, sendonly, sendrecv), (true, false, false))
    }))
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod create {
        use std::ops::Bound;

        use chrono::{SubsecRound, Utc};
        use serde::Deserialize;
        use serde_json::json;
        use svc_agent::mqtt::ResponseStatus;

        use crate::{
            app::handle_id::HandleId,
            backend::janus::client::{
                events::EventResponse,
                transactions::{Transaction, TransactionKind},
                IncomingEvent, SessionId,
            },
            db::{room::FindQueryable, rtc::SharingPolicy as RtcSharingPolicy},
            test_helpers::{db::TestDb, prelude::*, test_deps::LocalDeps},
        };

        use super::super::*;

        const SDP_OFFER: &str = r#"v=0
o=- 20518 0 IN IP4 0.0.0.0
s=-
t=0 0
a=group:BUNDLE audio video
a=group:LS audio video
a=ice-options:trickle
m=audio 54609 UDP/TLS/RTP/SAVPF 109 0 8
c=IN IP4 203.0.113.141
a=mid:audio
a=msid:ma ta
a=sendrecv
a=rtpmap:109 opus/48000/2
a=rtpmap:0 PCMU/8000
a=rtpmap:8 PCMA/8000
a=maxptime:120
a=ice-ufrag:074c6550
a=ice-pwd:a28a397a4c3f31747d1ee3474af08a068
a=fingerprint:sha-256 19:E2:1C:3B:4B:9F:81:E6:B8:5C:F4:A5:A8:D8:73:04:BB:05:2F:70:9F:04:A9:0E:05:E9:26:33:E8:70:88:A2
a=setup:actpass
a=tls-id:1
a=rtcp-mux
a=rtcp-mux-only
a=rtcp-rsize
a=extmap:1 urn:ietf:params:rtp-hdrext:ssrc-audio-level
a=extmap:2 urn:ietf:params:rtp-hdrext:sdes:mid
a=candidate:0 1 UDP 2122194687 192.0.2.4 61665 typ host
a=candidate:1 1 UDP 1685987071 203.0.113.141 54609 typ srflx raddr 192.0.2.4 rport 61665
a=end-of-candidates
m=video 54609 UDP/TLS/RTP/SAVPF 99 120
c=IN IP4 203.0.113.141
a=mid:video
a=msid:ma tb
a=sendrecv
a=rtpmap:99 H264/90000
a=fmtp:99 profile-level-id=4d0028;packetization-mode=1
a=rtpmap:120 VP8/90000
a=rtcp-fb:99 nack
a=rtcp-fb:99 nack pli
a=rtcp-fb:99 ccm fir
a=rtcp-fb:120 nack
a=rtcp-fb:120 nack pli
a=rtcp-fb:120 ccm fir
a=extmap:2 urn:ietf:params:rtp-hdrext:sdes:mid
"#;

        #[sqlx::test]
        async fn offer(pool: sqlx::PgPool) -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let janus = local_deps.run_janus();
            let db = TestDb::new(pool);

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let user_handle = shared_helpers::create_handle(&janus.url, session_id).await;
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            // Insert room with backend and rtc and an agent connection.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let (_, agent_connection) = shared_helpers::insert_connected_to_handle_agent(
                &mut conn,
                agent.agent_id(),
                rtc.room_id(),
                rtc.id(),
                user_handle,
            )
            .await;

            let classroom_id = room.classroom_id().to_string();

            // Allow user to update the rtc.
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object.clone(), "update");

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, authz).await;
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);
            let rtc_stream_id = db::janus_rtc_stream::Id::random();
            let handle_id = HandleId::new(
                rtc_stream_id,
                rtc.id(),
                agent_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );
            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id: handle_id.clone(),
                jsep,
                label: Some(String::from("whatever")),
                agent_label: None,
            };

            handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect("Rtc signal creation failed");
            rx.recv().await.unwrap();
            context.janus_clients().remove_client(&backend);
            match rx.recv().await.unwrap() {
                IncomingEvent::Event(EventResponse {
                    transaction:
                        Transaction {
                            kind: Some(TransactionKind::CreateStream(_tn)),
                            ..
                        },
                    jsep: Some(_jsep),
                    session_id: s_id,
                    plugindata: _,
                    opaque_id: _,
                }) => {
                    assert_eq!(session_id, s_id);
                }
                _ => {
                    panic!("Got wrong event")
                }
            }

            // Assert rtc stream presence in the DB.
            let mut conn = context.get_conn().await.unwrap();
            let rtc_stream: crate::db::janus_rtc_stream::Object =
                crate::db::janus_rtc_stream::get_rtc_stream(&mut conn, rtc_stream_id)
                    .await
                    .expect("failed to get janus rtc stream")
                    .expect("janus rtc stream not found");

            assert_eq!(rtc_stream.handle_id(), handle_id.janus_handle_id());
            assert_eq!(rtc_stream.rtc_id(), rtc.id());
            assert_eq!(rtc_stream.backend_id(), backend.id());
            assert_eq!(rtc_stream.label(), "whatever");
            assert_eq!(rtc_stream.sent_by(), agent.agent_id());
            Ok(())
        }

        #[sqlx::test]
        async fn offer_unauthorized(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Insert room with backend and rtc and an agent connection.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let (_, agent_connection) = shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                rtc.room_id(),
                rtc.id(),
            )
            .await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                agent_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: Some(String::from("whatever")),
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
            Ok(())
        }

        const SDP_ANSWER: &str = r#"v=0
o=- 16833 0 IN IP4 0.0.0.0
s=-
t=0 0
m=audio 49203 RTP/AVP 109
c=IN IP4 203.0.113.77
a=rtpmap:109 opus/48000
a=ptime:20
a=sendrecv
a=ice-ufrag:c300d85b
a=ice-pwd:de4e99bd291c325921d5d47efbabd9a2
a=fingerprint:sha-256 BB:05:2F:70:9F:04:A9:0E:05:E9:26:33:E8:70:88:A2:19:E2:1C:3B:4B:9F:81:E6:B8:5C:F4:A5:A8:D8:73:04
a=rtcp-mux
a=candidate:0 1 UDP 2113667327 198.51.100.7 49203 typ host
a=candidate:1 1 UDP 1694302207 203.0.113.77 49203 typ srflx raddr 198.51.100.7 rport 49203
m=video 63130 RTP/SAVP 120
c=IN IP4 203.0.113.77
a=rtpmap:120 VP8/90000
a=sendrecv
a=ice-ufrag:e39091na
a=ice-pwd:dbc325921d5dd29e4e99147efbabd9a2
a=fingerprint:sha-256 BB:0A:90:E0:5E:92:63:3E:87:08:8A:25:2F:70:9F:04:19:E2:1C:3B:4B:9F:81:52:F7:09:F0:4F:4A:5A:8D:80
a=rtcp-mux
a=candidate:0 1 UDP 2113667327 198.51.100.7 63130 typ host
a=candidate:1 1 UDP 1694302207 203.0.113.77 63130 typ srflx raddr 198.51.100.7 rport 63130
a=rtcp-fb:120 nack pli
a=rtcp-fb:120 ccm fir
"#;

        #[sqlx::test]
        async fn answer(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Insert room with backend and rtc and an agent connection.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let (_, agent_connection) = shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                rtc.room_id(),
                rtc.id(),
            )
            .await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                agent_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep =
                serde_json::from_value::<Jsep>(json!({ "type": "answer", "sdp": SDP_ANSWER }))
                    .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: Some(String::from("whatever")),
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_sdp_type");
            Ok(())
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct RtcSignalCreateJanusRequestIceCandidate {
            janus: String,
            session_id: i64,
            handle_id: i64,
            transaction: String,
            candidate: RtcSignalCreateJanusRequestIceCandidateCandidate,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct RtcSignalCreateJanusRequestIceCandidateCandidate {
            #[serde(rename = "sdpMid")]
            sdp_m_id: String,
            #[serde(rename = "sdpMLineIndex")]
            sdp_m_line_index: u16,
            candidate: String,
        }

        const ICE_CANDIDATE: &str = "candidate:0 1 UDP 2113667327 198.51.100.7 49203 typ host";

        #[sqlx::test]
        async fn candidate(pool: sqlx::PgPool) -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let janus = local_deps.run_janus();
            let db = TestDb::new(pool);

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let user_handle = shared_helpers::create_handle(&janus.url, session_id).await;
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            // Insert room with backend and rtc and an agent connection.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let (_, agent_connection) = shared_helpers::insert_connected_to_handle_agent(
                &mut conn,
                agent.agent_id(),
                rtc.room_id(),
                rtc.id(),
                user_handle,
            )
            .await;

            let classroom_id = room.classroom_id().to_string();

            // Allow user to read the rtc.
            let rtc_id = rtc.id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, authz).await;
            let (tx, _) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                agent_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({
                "sdpMid": "v",
                "sdpMLineIndex": 0,
                "candidate": ICE_CANDIDATE
            }))
            .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect("Rtc signal creation failed");
            context.janus_clients().remove_client(&backend);
            let (resp, _, _) = find_response::<CreateResponseData>(&messages);

            assert!(resp.jsep.is_none());

            Ok(())
        }

        #[sqlx::test]
        async fn candidate_unauthorized(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Insert room with backend and rtc and an agent connection.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let (_, agent_connection) = shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                rtc.room_id(),
                rtc.id(),
            )
            .await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                agent_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({
                "sdpMid": "v",
                "sdpMLineIndex": 0,
                "candidate": ICE_CANDIDATE
            }))
            .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
            Ok(())
        }

        #[sqlx::test]
        async fn wrong_rtc_id(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Insert room with backend and entered agent.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;

            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                db::rtc::Id::random(),
                crate::backend::janus::client::HandleId::stub_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "rtc_not_found");
            Ok(())
        }

        #[sqlx::test]
        async fn rtc_id_from_another_room(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Insert room with backend and RTC and an RTC in another room.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let (_, agent_connection) = shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                rtc.room_id(),
                rtc.id(),
            )
            .await;

            let rtc = shared_helpers::insert_rtc(&mut conn).await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                agent_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
            Ok(())
        }

        #[sqlx::test]
        async fn wrong_backend_id(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            // Insert room with backend, rtc and connected agent and another backend.
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let (_, agent_connection) = shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                rtc.room_id(),
                rtc.id(),
            )
            .await;

            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                agent_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_handle_id");
            Ok(())
        }

        #[sqlx::test]
        async fn offline_backend(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let backend = TestAgent::new("offline-instance", "janus-gateway", SVC_AUDIENCE);

            let mut conn = db.get_conn().await;

            // Insert room with backend, rtc and connected agent.
            let room =
                shared_helpers::insert_room_with_backend_id(&mut conn, backend.agent_id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let (_, agent_connection) = shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                rtc.room_id(),
                rtc.id(),
            )
            .await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                agent_connection.handle_id(),
                crate::backend::janus::client::SessionId::random(),
                backend.agent_id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "backend_not_found");
            Ok(())
        }

        #[sqlx::test]
        async fn not_entered(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Insert room with backend, rtc and connected agent and another backend.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                crate::backend::janus::client::HandleId::stub_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
            Ok(())
        }

        #[sqlx::test]
        async fn not_connected(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Insert room with backend, rtc and entered agent.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            shared_helpers::insert_agent(&mut conn, agent.agent_id(), room.id()).await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                crate::backend::janus::client::HandleId::stub_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::UNPROCESSABLE_ENTITY);
            assert_eq!(err.kind(), "agent_not_connected");
            Ok(())
        }

        #[sqlx::test]
        async fn wrong_handle_id(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Insert room with backend, rtc and a connect agent.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                room.id(),
                rtc.id(),
            )
            .await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                crate::backend::janus::client::HandleId::random(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_handle_id");
            Ok(())
        }

        #[sqlx::test]
        async fn spoof_handle_id(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);

            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::random(),
            )
            .await;

            // Insert room with backend, rtc and 2 connect agents.
            let room = shared_helpers::insert_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            shared_helpers::insert_connected_agent(
                &mut conn,
                agent1.agent_id(),
                room.id(),
                rtc.id(),
            )
            .await;

            let agent = shared_helpers::insert_agent(&mut conn, agent2.agent_id(), room.id()).await;

            let agent2_connection = factory::AgentConnection::new(
                agent.id(),
                rtc.id(),
                crate::backend::janus::client::HandleId::random(),
            )
            .insert(&mut conn)
            .await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                agent2_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent1, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_handle_id");
            Ok(())
        }

        #[sqlx::test]
        async fn closed_room(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Insert closed room with backend, rtc and connected agent.
            let room =
                shared_helpers::insert_closed_room_with_backend_id(&mut conn, backend.id()).await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let (_, agent_connection) = shared_helpers::insert_connected_agent(
                &mut conn,
                agent.agent_id(),
                room.id(),
                rtc.id(),
            )
            .await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                agent_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
            Ok(())
        }

        #[sqlx::test]
        async fn spoof_owned_rtc(pool: sqlx::PgPool) -> std::io::Result<()> {
            let db = TestDb::new(pool);

            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let now = Utc::now();

            let mut conn = db.get_conn().await;
            let backend = shared_helpers::insert_janus_backend(
                &mut conn,
                "test",
                SessionId::random(),
                crate::backend::janus::client::HandleId::stub_id(),
            )
            .await;

            // Insert room with backend, rtc owned by agent2 and connect agent1.
            let room = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((Bound::Included(now), Bound::Unbounded))
                .rtc_sharing_policy(RtcSharingPolicy::Owned)
                .backend_id(backend.id())
                .insert(&mut conn)
                .await;

            let rtc = factory::Rtc::new(room.id())
                .created_by(agent2.agent_id().to_owned())
                .insert(&mut conn)
                .await;

            let (_, agent_connection) = shared_helpers::insert_connected_agent(
                &mut conn,
                agent1.agent_id(),
                room.id(),
                rtc.id(),
            )
            .await;

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new()).await;

            let handle_id = HandleId::new(
                db::janus_rtc_stream::Id::random(),
                rtc.id(),
                agent_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );

            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id,
                jsep,
                label: None,
                agent_label: None,
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent1, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
            Ok(())
        }

        #[sqlx::test]
        async fn create_in_unbounded_room(pool: sqlx::PgPool) {
            let local_deps = LocalDeps::new();
            let janus = local_deps.run_janus();
            let db = TestDb::new(pool);

            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let user_handle = shared_helpers::create_handle(&janus.url, session_id).await;
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let mut conn = db.get_conn().await;
            let backend =
                shared_helpers::insert_janus_backend(&mut conn, &janus.url, session_id, handle_id)
                    .await;

            // Insert room with backend and rtc and an agent connection.
            let room = factory::Room::new()
                .audience(USR_AUDIENCE)
                .time((
                    Bound::Included(Utc::now().trunc_subsecs(0)),
                    Bound::Unbounded,
                ))
                .rtc_sharing_policy(RtcSharingPolicy::Shared)
                .backend_id(backend.id())
                .insert(&mut conn)
                .await;
            let rtc = shared_helpers::insert_rtc_with_room(&mut conn, &room).await;

            let (_, agent_connection) = shared_helpers::insert_connected_to_handle_agent(
                &mut conn,
                agent.agent_id(),
                rtc.room_id(),
                rtc.id(),
                user_handle,
            )
            .await;

            // Allow user to update the rtc.
            let rtc_id = rtc.id().to_string();
            let classroom_id = room.classroom_id().to_string();
            let object = vec!["classrooms", &classroom_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object.clone(), "update");

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, authz).await;
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            context.with_janus(tx);
            let rtc_stream_id = db::janus_rtc_stream::Id::random();
            let handle_id = HandleId::new(
                rtc_stream_id,
                rtc.id(),
                agent_connection.handle_id(),
                backend.session_id(),
                backend.id().to_owned(),
            );
            let jsep = serde_json::from_value::<Jsep>(json!({ "type": "offer", "sdp": SDP_OFFER }))
                .expect("Failed to build JSEP");

            let payload = CreateRequest {
                handle_id: handle_id.clone(),
                jsep,
                label: Some(String::from("whatever")),
                agent_label: None,
            };

            handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect("Rtc signal creation failed");

            let room = db::room::FindQuery::new(room.id())
                .execute(&mut conn)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(room.time().1, Bound::Unbounded);
        }
    }
}
