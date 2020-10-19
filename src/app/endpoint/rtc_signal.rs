use std::result::Result as StdResult;

use anyhow::Context as AnyhowContext;
use async_std::stream;
use async_trait::async_trait;
use chrono::Duration;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{IncomingRequestProperties, IntoPublishableMessage, OutgoingResponse};
use svc_agent::Addressable;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::handle_id::HandleId;
use crate::db;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct CreateResponseData {
    #[serde(skip_serializing_if = "Option::is_none")]
    jsep: Option<JsonValue>,
}

impl CreateResponseData {
    pub(crate) fn new(jsep: Option<JsonValue>) -> Self {
        Self { jsep }
    }
}

pub(crate) type CreateResponse = OutgoingResponse<CreateResponseData>;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequest {
    handle_id: HandleId,
    jsep: JsonValue,
    label: Option<String>,
}

pub(crate) struct CreateHandler;

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;
    const ERROR_TITLE: &'static str = "Failed to create rtc";

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        context.add_logger_tags(o!(
            "rtc_id" => payload.handle_id.rtc_id().to_string(),
            "rtc_stream_id" => payload.handle_id.rtc_stream_id().to_string(),
            "janus_session_id" => payload.handle_id.janus_session_id(),
            "janus_handle_id" => payload.handle_id.janus_handle_id(),
            "backend_id" => payload.handle_id.backend_id().to_string(),
        ));

        if let Some(ref label) = payload.label {
            context.add_logger_tags(o!("rtc_stream_label" => label.to_owned()));
        }

        let sdp_type = match parse_sdp_type(&payload.jsep) {
            Ok(sdp_type) => sdp_type,
            Err(err) => {
                return Err(err.context("Failed to parse SDP type"))
                    .error(AppErrorKind::InvalidSdpType)
            }
        };

        let req = match sdp_type {
            SdpType::Offer => {
                let is_recvonly = is_sdp_recvonly(&payload.jsep)
                    .context("Invalid JSEP format")
                    .error(AppErrorKind::InvalidJsepFormat)?;

                if is_recvonly {
                    context.add_logger_tags(o!("sdp_type" => "offer", "intent" => "read"));

                    // Authorization
                    let authz_time = authorize(context, &payload, reqp, "read").await?;

                    context
                        .janus_client()
                        .read_stream_request(
                            reqp.clone(),
                            payload.handle_id.janus_session_id(),
                            payload.handle_id.janus_handle_id(),
                            payload.handle_id.rtc_id(),
                            payload.jsep.clone(),
                            payload.handle_id.backend_id(),
                            context.start_timestamp(),
                            authz_time,
                        )
                        .map(|req| Box::new(req) as Box<dyn IntoPublishableMessage + Send>)
                        .context("Error creating a backend request")
                        .error(AppErrorKind::MessageBuildingFailed)?
                } else {
                    context.add_logger_tags(o!("sdp_type" => "offer", "intent" => "update"));

                    // Authorization
                    let authz_time = authorize(context, &payload, reqp, "update").await?;

                    // Updating the Real-Time Connection state
                    {
                        let label = payload
                            .label
                            .as_ref()
                            .ok_or_else(|| anyhow!("Missing label"))
                            .error(AppErrorKind::MessageParsingFailed)?;

                        let conn = context.get_conn()?;

                        db::janus_rtc_stream::InsertQuery::new(
                            payload.handle_id.rtc_stream_id(),
                            payload.handle_id.janus_handle_id(),
                            payload.handle_id.rtc_id(),
                            payload.handle_id.backend_id(),
                            label,
                            reqp.as_agent_id(),
                        )
                        .execute(&conn)?;
                    }

                    context
                        .janus_client()
                        .create_stream_request(
                            reqp.clone(),
                            payload.handle_id.janus_session_id(),
                            payload.handle_id.janus_handle_id(),
                            payload.handle_id.rtc_id(),
                            payload.jsep.clone(),
                            payload.handle_id.backend_id(),
                            context.start_timestamp(),
                            authz_time,
                        )
                        .map(|req| Box::new(req) as Box<dyn IntoPublishableMessage + Send>)
                        .context("Error creating a backend request")
                        .error(AppErrorKind::MessageBuildingFailed)?
                }
            }
            SdpType::Answer => Err(anyhow!("sdp_type = 'answer' is not allowed"))
                .error(AppErrorKind::InvalidSdpType)?,
            SdpType::IceCandidate => {
                context.add_logger_tags(o!("sdp_type" => "ice_candidate", "intent" => "read"));

                // Authorization
                let authz_time = authorize(context, &payload, reqp, "read").await?;

                context
                    .janus_client()
                    .trickle_request(
                        reqp.clone(),
                        payload.handle_id.janus_session_id(),
                        payload.handle_id.janus_handle_id(),
                        payload.jsep.clone(),
                        payload.handle_id.backend_id(),
                        context.start_timestamp(),
                        authz_time,
                    )
                    .map(|req| Box::new(req) as Box<dyn IntoPublishableMessage + Send>)
                    .context("Error creating a backend request")
                    .error(AppErrorKind::MessageBuildingFailed)?
            }
        };

        Ok(Box::new(stream::once(req)))
    }
}

async fn authorize<C: Context>(
    context: &mut C,
    payload: &CreateRequest,
    reqp: &IncomingRequestProperties,
    action: &str,
) -> StdResult<Duration, AppError> {
    let rtc_id = payload.handle_id.rtc_id();
    let room = helpers::find_room_by_rtc_id(context, rtc_id, helpers::RoomTimeRequirement::Open)?;

    if room.backend() != db::room::RoomBackend::Janus {
        let err = anyhow!(
            "'rtc_signal.create' is not implemented for the backend = '{}'.",
            room.backend()
        );

        return Err(err).error(AppErrorKind::NotImplemented);
    }

    let room_id = room.id().to_string();
    let rtc_id = rtc_id.to_string();
    let object = vec!["rooms", &room_id, "rtcs", &rtc_id];

    context
        .authz()
        .authorize(room.audience(), reqp, object, action)
        .await
        .map_err(AppError::from)
}

#[derive(Debug)]
enum SdpType {
    Offer,
    Answer,
    IceCandidate,
}

fn parse_sdp_type(jsep: &JsonValue) -> anyhow::Result<SdpType> {
    // '{"type": "offer", "sdp": _}' or '{"type": "answer", "sdp": _}'
    let sdp_type = jsep.get("type");

    // '{"sdpMid": _, "sdpMLineIndex": _, "candidate": _}' or '{"completed": true}' or 'null'
    let is_candidate = {
        let candidate = jsep.get("candidate");
        let completed = jsep.get("completed");
        candidate
            .map(|val| val.is_string())
            .unwrap_or_else(|| false)
            || completed
                .map(|val| val.as_bool().unwrap_or_else(|| false))
                .unwrap_or_else(|| false)
            || jsep.is_null()
    };

    match (sdp_type, is_candidate) {
        (Some(JsonValue::String(ref val)), false) if val == "offer" => Ok(SdpType::Offer),
        // {"type": "answer", "sdp": _}
        (Some(JsonValue::String(ref val)), false) if val == "answer" => Ok(SdpType::Answer),
        // {"completed": true} or {"sdpMid": _, "sdpMLineIndex": _, "candidate": _}
        (None, true) => Ok(SdpType::IceCandidate),
        _ => Err(anyhow!("invalid jsep = '{}'", jsep)),
    }
}

fn is_sdp_recvonly(jsep: &JsonValue) -> anyhow::Result<bool> {
    use webrtc_sdp::{attribute_type::SdpAttributeType, parse_sdp};

    let sdp = jsep.get("sdp").ok_or_else(|| anyhow!("Missing SDP"))?;

    let sdp = sdp
        .as_str()
        .ok_or_else(|| anyhow!("Invalid SDP: '{}'", sdp))?;

    let sdp = parse_sdp(sdp, false).context("Invalid SDP")?;

    // Returning true if all media section contains 'recvonly' attribute
    Ok(sdp.media.iter().all(|item| {
        let recvonly = item.get_attribute(SdpAttributeType::Recvonly).is_some();
        let sendonly = item.get_attribute(SdpAttributeType::Sendonly).is_some();
        let sendrecv = item.get_attribute(SdpAttributeType::Sendrecv).is_some();

        match (recvonly, sendonly, sendrecv) {
            (true, false, false) => true,
            _ => false,
        }
    }))
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    mod create {
        use diesel::prelude::*;
        use serde::Deserialize;
        use serde_json::json;
        use svc_agent::mqtt::ResponseStatus;
        use uuid::Uuid;

        use crate::app::handle_id::HandleId;
        use crate::backend::janus;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        const SDP_OFFER: &str = r#"
        v=0
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

        #[derive(Debug, PartialEq, Deserialize)]
        struct RtcSignalCreateJanusRequestOffer {
            janus: String,
            session_id: i64,
            handle_id: i64,
            transaction: String,
            body: RtcSignalCreateJanusRequestOfferBody,
            jsep: RtcSignalCreateJanusRequestOfferJsep,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct RtcSignalCreateJanusRequestOfferBody {
            method: String,
            id: Uuid,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct RtcSignalCreateJanusRequestOfferJsep {
            r#type: String,
            sdp: String,
        }

        #[test]
        fn create_rtc_signal_for_offer() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert a janus backend and an rtc.
                let (backend, rtc) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        (
                            shared_helpers::insert_janus_backend(&conn),
                            shared_helpers::insert_rtc(&conn),
                        )
                    })
                    .unwrap();

                // Allow user to update the rtc.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(agent.account_id(), object, "update");

                // Make rtc_signal.create request.
                let mut context = TestContext::new(db, authz);
                let rtc_stream_id = Uuid::new_v4();

                let handle_id = HandleId::new(
                    rtc_stream_id,
                    rtc.id(),
                    backend.handle_id(),
                    backend.session_id(),
                    backend.id().to_owned(),
                );

                let payload = CreateRequest {
                    handle_id,
                    jsep: json!({ "type": "offer", "sdp": SDP_OFFER }),
                    label: Some(String::from("whatever")),
                };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc signal creation failed");

                // Assert outgoing broker request.
                let (payload, _reqp, topic) =
                    find_request::<RtcSignalCreateJanusRequestOffer>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    backend.id(),
                    janus::JANUS_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, &expected_topic);
                assert_eq!(payload.janus, "message");
                assert_eq!(payload.session_id, backend.session_id());
                assert_eq!(payload.handle_id, backend.handle_id());
                assert_eq!(payload.body.method, "stream.create");
                assert_eq!(payload.body.id, rtc.id());
                assert_eq!(payload.jsep.r#type, "offer");
                assert_eq!(payload.jsep.sdp, SDP_OFFER);

                // Assert rtc stream presence in the DB.
                let conn = context.get_conn().unwrap();
                let query = crate::schema::janus_rtc_stream::table.find(rtc_stream_id);

                let rtc_stream: crate::db::janus_rtc_stream::Object =
                    query.get_result(&conn).unwrap();

                assert_eq!(rtc_stream.handle_id(), backend.handle_id());
                assert_eq!(rtc_stream.rtc_id(), rtc.id());
                assert_eq!(rtc_stream.backend_id(), backend.id());
                assert_eq!(rtc_stream.label(), "whatever");
                assert_eq!(rtc_stream.sent_by(), agent.agent_id());
            });
        }

        #[test]
        fn create_rtc_signal_for_offer_unauthorized() {
            async_std::task::block_on(async {
                let db = TestDb::new();

                // Insert a janus backend and an rtc.
                let (backend, rtc) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        (
                            shared_helpers::insert_janus_backend(&conn),
                            shared_helpers::insert_rtc(&conn),
                        )
                    })
                    .unwrap();

                // Make rtc_signal.create request.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(db, TestAuthz::new());
                let rtc_stream_id = Uuid::new_v4();

                let handle_id = HandleId::new(
                    rtc_stream_id,
                    rtc.id(),
                    backend.handle_id(),
                    backend.session_id(),
                    backend.id().to_owned(),
                );

                let payload = CreateRequest {
                    handle_id,
                    jsep: json!({ "type": "offer", "sdp": SDP_OFFER }),
                    label: Some(String::from("whatever")),
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }

        const SDP_ANSWER: &str = r#"
        v=0
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

        #[test]
        fn create_rtc_signal_for_answer() {
            async_std::task::block_on(async {
                let db = TestDb::new();

                // Insert a janus backend and an rtc.
                let (backend, rtc) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        (
                            shared_helpers::insert_janus_backend(&conn),
                            shared_helpers::insert_rtc(&conn),
                        )
                    })
                    .unwrap();

                // Make rtc_signal.create request.
                let mut context = TestContext::new(db, TestAuthz::new());
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let handle_id = HandleId::new(
                    Uuid::new_v4(),
                    rtc.id(),
                    backend.handle_id(),
                    backend.session_id(),
                    backend.id().to_owned(),
                );

                let payload = CreateRequest {
                    handle_id,
                    jsep: json!({ "type": "answer", "sdp": SDP_ANSWER }),
                    label: Some(String::from("whatever")),
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
                assert_eq!(err.kind(), "invalid_sdp_type");
            });
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
            sdp_m_id: usize,
            #[serde(rename = "sdpMLineIndex")]
            sdp_m_line_index: usize,
            candidate: String,
        }

        const ICE_CANDIDATE: &str = "candidate:0 1 UDP 2113667327 198.51.100.7 49203 typ host";

        #[test]
        fn create_rtc_signal_for_candidate() {
            async_std::task::block_on(async {
                let db = TestDb::new();
                let mut authz = TestAuthz::new();

                // Insert a janus backend and an rtc.
                let (backend, rtc) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        (
                            shared_helpers::insert_janus_backend(&conn),
                            shared_helpers::insert_rtc(&conn),
                        )
                    })
                    .unwrap();

                // Allow user to read the rtc.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let room_id = rtc.room_id().to_string();
                let rtc_id = rtc.id().to_string();
                let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
                authz.allow(agent.account_id(), object, "read");

                // Make rtc_signal.create request.
                let mut context = TestContext::new(db, authz);
                let rtc_stream_id = Uuid::new_v4();

                let handle_id = HandleId::new(
                    rtc_stream_id,
                    rtc.id(),
                    backend.handle_id(),
                    backend.session_id(),
                    backend.id().to_owned(),
                );

                let payload = CreateRequest {
                    handle_id,
                    jsep: json!({ "sdpMid": 0, "sdpMLineIndex": 0, "candidate": ICE_CANDIDATE }),
                    label: None,
                };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Rtc signal creation failed");

                // Assert outgoing broker request.
                let (payload, _reqp, topic) =
                    find_request::<RtcSignalCreateJanusRequestIceCandidate>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    backend.id(),
                    janus::JANUS_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(payload.janus, "trickle");
                assert_eq!(payload.session_id, backend.session_id());
                assert_eq!(payload.handle_id, backend.handle_id());
                assert_eq!(payload.candidate.sdp_m_id, 0);
                assert_eq!(payload.candidate.sdp_m_line_index, 0);
                assert_eq!(payload.candidate.candidate, ICE_CANDIDATE);
            });
        }

        #[test]
        fn create_rtc_signal_for_candidate_unauthorized() {
            async_std::task::block_on(async {
                let db = TestDb::new();

                // Insert a janus backend and an rtc.
                let (backend, rtc) = db
                    .connection_pool()
                    .get()
                    .map(|conn| {
                        (
                            shared_helpers::insert_janus_backend(&conn),
                            shared_helpers::insert_rtc(&conn),
                        )
                    })
                    .unwrap();

                // Make rtc_signal.create request.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(db, TestAuthz::new());
                let rtc_stream_id = Uuid::new_v4();

                let handle_id = HandleId::new(
                    rtc_stream_id,
                    rtc.id(),
                    backend.handle_id(),
                    backend.session_id(),
                    backend.id().to_owned(),
                );

                let payload = CreateRequest {
                    handle_id,
                    jsep: json!({ "sdpMid": 0, "sdpMLineIndex": 0, "candidate": ICE_CANDIDATE }),
                    label: None,
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on rtc creation");

                assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
                assert_eq!(err.kind(), "access_denied");
            });
        }
    }
}
