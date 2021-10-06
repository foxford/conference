use crate::{
    app::{
        context::Context,
        endpoint,
        endpoint::prelude::*,
        handle_id::HandleId,
        metrics::HistogramExt,
        service_utils::{RequestParams, Response},
    },
    authz::AuthzObject,
    backend::janus::{
        client::{
            create_stream::{
                CreateStreamRequest, CreateStreamRequestBody, CreateStreamTransaction,
                ReaderConfig, WriterConfig,
            },
            read_stream::{ReadStreamRequest, ReadStreamRequestBody, ReadStreamTransaction},
            trickle::TrickleRequest,
            Jsep, JsepType,
        },
        JANUS_API_VERSION,
    },
    db,
};
use anyhow::{anyhow, Context as AnyhowContext};
use async_trait::async_trait;
use chrono::Duration;
use futures::stream;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::result::Result as StdResult;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingResponse, ResponseStatus,
        ShortTermTimingProperties,
    },
    Addressable,
};

use tracing::Span;
use tracing_attributes::instrument;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateResponseData {
    #[serde(skip_serializing_if = "Option::is_none")]
    jsep: Option<JsonValue>,
}

impl CreateResponseData {
    pub fn new(jsep: Option<JsonValue>) -> Self {
        Self { jsep }
    }
}

pub type CreateResponse = OutgoingResponse<CreateResponseData>;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct CreateRequest {
    handle_id: HandleId,
    jsep: Jsep,
    label: Option<String>,
}

pub struct CreateHandler;

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;
    const ERROR_TITLE: &'static str = "Failed to create rtc";

    #[instrument(skip(context, payload, reqp), fields(
        rtc_id = %payload.handle_id.rtc_id(),
        rtc_stream_id = %payload.handle_id.rtc_stream_id(),
        janus_session_id = %payload.handle_id.janus_session_id(),
        janus_handle_id = %payload.handle_id.janus_handle_id(),
        backend_id = %payload.handle_id.backend_id().to_string()),
        rtc_stream_label = ?payload.label
    )]
    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        let mqtt_params = reqp.as_mqtt_params()?;
        // Validate RTC and room presence.
        let conn = context.get_conn().await?;
        let (room, rtc, backend) =crate::util::spawn_blocking({
            let agent_id = reqp.as_agent_id().clone();
            let handle_id = payload.handle_id.clone();
            move ||{

            let rtc = db::rtc::FindQuery::new()
                .id(handle_id.rtc_id())
                .execute(&conn)?
                .ok_or_else(|| anyhow!("RTC not found"))
                .error(AppErrorKind::RtcNotFound)?;

            let room = helpers::find_room_by_id(
                rtc.room_id(),
                helpers::RoomTimeRequirement::Open,
                &conn,
            )?;

            helpers::check_room_presence(&room, &agent_id, &conn)?;

            // Validate backend and janus session id.
            if let Some(backend_id) = room.backend_id() {
                if handle_id.backend_id() != backend_id {
                    return Err(anyhow!("Backend id specified in the handle ID doesn't match the one from the room object"))
                        .error(AppErrorKind::InvalidHandleId);
                }
            } else {
                return Err(anyhow!("Room backend not set")).error(AppErrorKind::BackendNotFound);
            }

            let janus_backend = db::janus_backend::FindQuery::new()
                .id(handle_id.backend_id())
                .execute(&conn)?
                .ok_or_else(|| anyhow!("Backend not found"))
                .error(AppErrorKind::BackendNotFound)?;

            if handle_id.janus_session_id() != janus_backend.session_id() {
                return Err(anyhow!("Backend session specified in the handle ID doesn't match the one from the backend object"))
                    .error(AppErrorKind::InvalidHandleId)?;
            }

            // Validate agent connection and handle id.
            let agent_connection =
                db::agent_connection::FindQuery::new(&agent_id, rtc.id())
                    .execute(&conn)?
                    .ok_or_else(|| anyhow!("Agent not connected"))
                    .error(AppErrorKind::AgentNotConnected)?;

            if handle_id.janus_handle_id() != agent_connection.handle_id() {
                return Err(anyhow!("Janus handle ID specified in the handle ID doesn't match the one from the agent connection"))
                    .error(AppErrorKind::InvalidHandleId)?;
            }

            Ok::<_, AppError>((room, rtc, janus_backend))
        }}).await?;

        match payload.jsep {
            Jsep::OfferOrAnswer { kind, ref sdp } => {
                match kind {
                    JsepType::Offer => {
                        let current_span = Span::current();
                        current_span.record("sdp_type", &"offer");
                        let is_recvonly = is_sdp_recvonly(sdp)
                            .context("Invalid JSEP format")
                            .error(AppErrorKind::InvalidJsepFormat)?;

                        if is_recvonly {
                            current_span.record("intent", &"read");

                            // Authorization
                            let _authz_time =
                                authorize(context, &payload, reqp, "read", &room).await?;

                            let request = ReadStreamRequest {
                                body: ReadStreamRequestBody::new(
                                    payload.handle_id.rtc_id(),
                                    reqp.as_agent_id().clone(),
                                ),
                                handle_id: payload.handle_id.janus_handle_id(),
                                session_id: payload.handle_id.janus_session_id(),
                                jsep: payload.jsep,
                            };
                            let transaction = ReadStreamTransaction {
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
                        } else {
                            current_span.record("intent", &"update");

                            if room.rtc_sharing_policy() == db::rtc::SharingPolicy::Owned
                                && reqp.as_agent_id() != rtc.created_by()
                            {
                                return Err(anyhow!("Signaling to other's RTC with sendonly or sendrecv SDP is not allowed"))
                                    .error(AppErrorKind::AccessDenied);
                            }

                            let _authz_time =
                                authorize(context, &payload, reqp, "update", &room).await?;

                            // Updating the Real-Time Connection state
                            let label = payload
                                .label
                                .as_ref()
                                .ok_or_else(|| anyhow!("Missing label"))
                                .error(AppErrorKind::MessageParsingFailed)?
                                .clone();

                            let conn = context.get_conn().await?;

                            crate::util::spawn_blocking({
                                let handle_id = payload.handle_id.clone();
                                let agent_id = reqp.as_agent_id().clone();
                                move || {
                                    db::janus_rtc_stream::InsertQuery::new(
                                        handle_id.rtc_stream_id(),
                                        handle_id.janus_handle_id(),
                                        handle_id.rtc_id(),
                                        handle_id.backend_id(),
                                        &label,
                                        &agent_id,
                                    )
                                    .execute(&conn)
                                }
                            })
                            .await?;
                            let (writer_config, reader_config) = crate::util::spawn_blocking({
                                let rtc_id = payload.handle_id.rtc_id();
                                let conn = context.get_conn().await?;
                                move || {
                                    Ok::<_, diesel::result::Error>((
                                        db::rtc_writer_config::read_config(rtc_id, &conn)?,
                                        db::rtc_reader_config::read_config(rtc_id, &conn)?,
                                    ))
                                }
                            })
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
                                    reader_config.map(|r| {
                                        r.into_iter()
                                            .map(|r| ReaderConfig {
                                                reader_id: r.reader_id().to_owned(),
                                                receive_audio: r.receive_audio(),
                                                receive_video: r.receive_video(),
                                            })
                                            .collect()
                                    }),
                                ),
                                handle_id: payload.handle_id.janus_handle_id(),
                                session_id: payload.handle_id.janus_session_id(),
                                jsep: payload.jsep,
                            };
                            let transaction = CreateStreamTransaction {
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
                    }
                    JsepType::Answer => Err(anyhow!("sdp_type = 'answer' is not allowed"))
                        .error(AppErrorKind::InvalidSdpType)?,
                }
            }
            Jsep::IceCandidate(_) => {
                let current_span = Span::current();
                current_span.record("sdp_type", &"ice_candidate");
                current_span.record("intent", &"read");

                let _authz_time = authorize(context, &payload, reqp, "read", &room).await?;

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

async fn authorize<C: Context>(
    context: &mut C,
    payload: &CreateRequest,
    reqp: RequestParams<'_>,
    action: &str,
    room: &db::room::Object,
) -> StdResult<Duration, AppError> {
    let rtc_id = payload.handle_id.rtc_id();

    if room.rtc_sharing_policy() == db::rtc::SharingPolicy::None {
        let err = anyhow!(
            "'rtc_signal.create' is not implemented for the rtc_sharing_policy = '{}'.",
            room.rtc_sharing_policy()
        );

        return Err(err).error(AppErrorKind::NotImplemented);
    }

    let room_id = room.id().to_string();
    let rtc_id = rtc_id.to_string();
    let object = AuthzObject::new(&["rooms", &room_id, "rtcs", &rtc_id]).into();

    let elapsed = context
        .authz()
        .authorize(room.audience().into(), reqp, object, action.into())
        .await?;
    context.metrics().observe_auth(elapsed);
    Ok(elapsed)
}

fn is_sdp_recvonly(sdp: &str) -> anyhow::Result<bool> {
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

        use chrono::Utc;
        use diesel::prelude::*;
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
            db::rtc::SharingPolicy as RtcSharingPolicy,
            test_helpers::{prelude::*, test_deps::LocalDeps},
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

        #[tokio::test]
        async fn offer() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let user_handle = shared_helpers::create_handle(&janus.url, session_id).await;
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and rtc and an agent connection.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn, &janus.url, session_id, handle_id,
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    let (_, agent_connection) = shared_helpers::insert_connected_to_handle_agent(
                        &conn,
                        agent.agent_id(),
                        rtc.room_id(),
                        rtc.id(),
                        user_handle,
                    );
                    (backend, rtc, agent_connection)
                })
                .unwrap();
            // Allow user to update the rtc.
            let room_id = rtc.room_id().to_string();
            let rtc_id = rtc.id().to_string();
            let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object.clone(), "update");

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, authz);
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
            let conn = context.get_conn().await.unwrap();
            let query = crate::schema::janus_rtc_stream::table.find(rtc_stream_id);

            let rtc_stream: crate::db::janus_rtc_stream::Object = query.get_result(&conn).unwrap();

            assert_eq!(rtc_stream.handle_id(), handle_id.janus_handle_id());
            assert_eq!(rtc_stream.rtc_id(), rtc.id());
            assert_eq!(rtc_stream.backend_id(), backend.id());
            assert_eq!(rtc_stream.label(), "whatever");
            assert_eq!(rtc_stream.sent_by(), agent.agent_id());
            Ok(())
        }

        #[tokio::test]
        async fn offer_unauthorized() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);

            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and rtc and an agent connection.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    let (_, agent_connection) = shared_helpers::insert_connected_agent(
                        &conn,
                        agent.agent_id(),
                        rtc.room_id(),
                        rtc.id(),
                    );

                    (backend, rtc, agent_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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

        #[tokio::test]
        async fn answer() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and rtc and an agent connection.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    let (_, agent_connection) = shared_helpers::insert_connected_agent(
                        &conn,
                        agent.agent_id(),
                        rtc.room_id(),
                        rtc.id(),
                    );

                    (backend, rtc, agent_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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

        #[tokio::test]
        async fn candidate() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let janus = local_deps.run_janus();
            let db = TestDb::with_local_postgres(&postgres);
            let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
            let user_handle = shared_helpers::create_handle(&janus.url, session_id).await;
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and rtc and an agent connection.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn, &janus.url, session_id, handle_id,
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    let (_, agent_connection) = shared_helpers::insert_connected_to_handle_agent(
                        &conn,
                        agent.agent_id(),
                        rtc.room_id(),
                        rtc.id(),
                        user_handle,
                    );

                    (backend, rtc, agent_connection)
                })
                .unwrap();

            // Allow user to read the rtc.
            let room_id = rtc.room_id().to_string();
            let rtc_id = rtc.id().to_string();
            let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, authz);
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
            };

            let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect("Rtc signal creation failed");
            context.janus_clients().remove_client(&backend);
            let (resp, _, _) = find_response::<CreateResponseData>(&messages);

            assert!(resp.jsep.is_none());

            Ok(())
        }

        #[tokio::test]
        async fn candidate_unauthorized() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and rtc and an agent connection.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    let (_, agent_connection) = shared_helpers::insert_connected_agent(
                        &conn,
                        agent.agent_id(),
                        rtc.room_id(),
                        rtc.id(),
                    );

                    (backend, rtc, agent_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
            Ok(())
        }

        #[tokio::test]
        async fn wrong_rtc_id() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and entered agent.
            let backend = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    backend
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "rtc_not_found");
            Ok(())
        }

        #[tokio::test]
        async fn rtc_id_from_another_room() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and RTC and an RTC in another room.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    let (_, agent_connection) = shared_helpers::insert_connected_agent(
                        &conn,
                        agent.agent_id(),
                        rtc.room_id(),
                        rtc.id(),
                    );

                    let other_rtc = shared_helpers::insert_rtc(&conn);
                    (backend, other_rtc, agent_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
            Ok(())
        }

        #[tokio::test]
        async fn wrong_backend_id() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend, rtc and connected agent and another backend.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    let (_, agent_connection) = shared_helpers::insert_connected_agent(
                        &conn,
                        agent.agent_id(),
                        rtc.room_id(),
                        rtc.id(),
                    );

                    let other_backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    (other_backend, rtc, agent_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_handle_id");
            Ok(())
        }

        #[tokio::test]
        async fn offline_backend() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let backend = TestAgent::new("offline-instance", "janus-gateway", SVC_AUDIENCE);

            // Insert room with backend, rtc and connected agent.
            let (rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let room =
                        shared_helpers::insert_room_with_backend_id(&conn, backend.agent_id());

                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    let (_, agent_connection) = shared_helpers::insert_connected_agent(
                        &conn,
                        agent.agent_id(),
                        rtc.room_id(),
                        rtc.id(),
                    );

                    (rtc, agent_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "backend_not_found");
            Ok(())
        }

        #[tokio::test]
        async fn not_entered() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend, rtc and connected agent and another backend.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);
                    (backend, rtc)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "agent_not_entered_the_room");
            Ok(())
        }

        #[tokio::test]
        async fn not_connected() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend, rtc and entered agent.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    (backend, rtc)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::UNPROCESSABLE_ENTITY);
            assert_eq!(err.kind(), "agent_not_connected");
            Ok(())
        }

        #[tokio::test]
        async fn wrong_handle_id() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend, rtc and a connect agent.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    shared_helpers::insert_connected_agent(
                        &conn,
                        agent.agent_id(),
                        room.id(),
                        rtc.id(),
                    );

                    (backend, rtc)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_handle_id");
            Ok(())
        }

        #[tokio::test]
        async fn spoof_handle_id() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);

            // Insert room with backend, rtc and 2 connect agents.
            let (backend, rtc, agent2_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::random(),
                    );
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    shared_helpers::insert_connected_agent(
                        &conn,
                        agent1.agent_id(),
                        room.id(),
                        rtc.id(),
                    );

                    let agent = shared_helpers::insert_agent(&conn, agent2.agent_id(), room.id());

                    let agent2_connection = factory::AgentConnection::new(
                        *agent.id(),
                        rtc.id(),
                        crate::backend::janus::client::HandleId::random(),
                    )
                    .insert(&conn);

                    (backend, rtc, agent2_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent1, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
            assert_eq!(err.kind(), "invalid_handle_id");
            Ok(())
        }

        #[tokio::test]
        async fn closed_room() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert closed room with backend, rtc and connected agent.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room =
                        shared_helpers::insert_closed_room_with_backend_id(&conn, backend.id());

                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    let (_, agent_connection) = shared_helpers::insert_connected_agent(
                        &conn,
                        agent.agent_id(),
                        room.id(),
                        rtc.id(),
                    );

                    (backend, rtc, agent_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_closed");
            Ok(())
        }

        #[tokio::test]
        async fn spoof_owned_rtc() -> std::io::Result<()> {
            let local_deps = LocalDeps::new();
            let postgres = local_deps.run_postgres();
            let db = TestDb::with_local_postgres(&postgres);
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let now = Utc::now();

            // Insert room with backend, rtc owned by agent2 and connect agent1.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(
                        &conn,
                        "test",
                        SessionId::random(),
                        crate::backend::janus::client::HandleId::stub_id(),
                    );
                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((Bound::Included(now), Bound::Unbounded))
                        .rtc_sharing_policy(RtcSharingPolicy::Owned)
                        .backend_id(backend.id())
                        .insert(&conn);

                    let rtc = factory::Rtc::new(room.id())
                        .created_by(agent2.agent_id().to_owned())
                        .insert(&conn);

                    let (_, agent_connection) = shared_helpers::insert_connected_agent(
                        &conn,
                        agent1.agent_id(),
                        room.id(),
                        rtc.id(),
                    );

                    (backend, rtc, agent_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

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
            };

            let err = handle_request::<CreateHandler>(&mut context, &agent1, payload)
                .await
                .expect_err("Unexpected success on rtc signal creation");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
            assert_eq!(err.kind(), "access_denied");
            Ok(())
        }
    }
}
