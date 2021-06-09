use std::result::Result as StdResult;

use anyhow::Context as AnyhowContext;
use async_std::stream;
use async_trait::async_trait;
use chrono::Duration;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{
    IncomingRequestProperties, IntoPublishableMessage, OutgoingMessage, OutgoingResponse,
    ResponseStatus, ShortTermTimingProperties,
};
use svc_agent::Addressable;
use svc_error::Error as SvcError;
use uuid::Uuid;

use super::janus::transactions::Transaction;
use super::MessageStream;
use crate::{app::handle_id::HandleId, backend::janus::requests::CreateStreamRequestBody};
use crate::{
    app::{context::Context, endpoint},
    backend::janus::JANUS_API_VERSION,
};
use crate::{
    app::{endpoint::prelude::*, API_VERSION},
    backend::janus::requests::{MessageRequest, ReadStreamRequestBody},
    util::to_base64,
};
use crate::{backend::janus::transactions::create_stream::TransactionData, db};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum Jsep {
    // '{"type": "offer", "sdp": _}' or '{"type": "answer", "sdp": _}'
    OfferOrAnswer {
        #[serde(rename = "type")]
        kind: JsepType,
        sdp: String,
    },
    IceCandidate(IceCandidateSdp),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum JsepType {
    Offer,
    Answer,
}

#[derive(Debug, Deserialize, Serialize)]
struct IceCandidate {
    #[serde(rename = "sdpMid")]
    _sdp_mid: String,
    #[serde(rename = "sdpMLineIndex")]
    _sdp_m_line_index: u16,
    #[serde(rename = "candidate")]
    _candidate: String,
    #[serde(rename = "usernameFragment")]
    _username_fragment: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum IceCandidateSdpItem {
    IceCandidate(IceCandidate),
    // {"completed": true}
    Completed {
        #[serde(rename = "completed")]
        _completed: bool,
    },
    // null
    Null(Option<usize>),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum IceCandidateSdp {
    // {"sdpMid": _, "sdpMLineIndex": _, "candidate": _}
    Single(IceCandidateSdpItem),
    // [{"sdpMid": _, "sdpMLineIndex": _, "candidate": _}, …, {"completed": true}]
    List(Vec<IceCandidateSdpItem>),
}

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
    jsep: Jsep,
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

        // Validate RTC and room presence.
        let (room, rtc) = {
            let conn = context.get_conn()?;

            let rtc = db::rtc::FindQuery::new()
                .id(payload.handle_id.rtc_id())
                .execute(&conn)?
                .ok_or_else(|| anyhow!("RTC not found"))
                .error(AppErrorKind::RtcNotFound)?;

            let room = helpers::find_room_by_id(
                context,
                rtc.room_id(),
                helpers::RoomTimeRequirement::Open,
                &conn,
            )?;

            helpers::check_room_presence(&room, reqp.as_agent_id(), &conn)?;

            // Validate backend and janus session id.
            if let Some(backend_id) = room.backend_id() {
                if payload.handle_id.backend_id() != backend_id {
                    return Err(anyhow!("Backend id specified in the handle ID doesn't match the one from the room object"))
                        .error(AppErrorKind::InvalidHandleId);
                }
            } else {
                return Err(anyhow!("Room backend not set")).error(AppErrorKind::BackendNotFound);
            }

            let janus_backend = db::janus_backend::FindQuery::new()
                .id(payload.handle_id.backend_id())
                .execute(&conn)?
                .ok_or_else(|| anyhow!("Backend not found"))
                .error(AppErrorKind::BackendNotFound)?;

            if payload.handle_id.janus_session_id() != janus_backend.session_id() {
                return Err(anyhow!("Backend session specified in the handle ID doesn't match the one from the backend object"))
                    .error(AppErrorKind::InvalidHandleId)?;
            }

            // Validate agent connection and handle id.
            let agent_connection =
                db::agent_connection::FindQuery::new(reqp.as_agent_id(), rtc.id())
                    .execute(&conn)?
                    .ok_or_else(|| anyhow!("Agent not connected"))
                    .error(AppErrorKind::AgentNotConnected)?;

            if payload.handle_id.janus_handle_id() != agent_connection.handle_id() {
                return Err(anyhow!("Janus handle ID specified in the handle ID doesn't match the one from the agent connection"))
                    .error(AppErrorKind::InvalidHandleId)?;
            }

            (room, rtc)
        };

        let req = match payload.jsep {
            Jsep::OfferOrAnswer { kind, ref sdp } => {
                match kind {
                    JsepType::Offer => {
                        let is_recvonly = is_sdp_recvonly(sdp)
                            .context("Invalid JSEP format")
                            .error(AppErrorKind::InvalidJsepFormat)?;

                        if is_recvonly {
                            context.add_logger_tags(o!("sdp_type" => "offer", "intent" => "read"));

                            // Authorization
                            let authz_time =
                                authorize(context, &payload, reqp, "read", &room).await?;

                            let jsep = serde_json::to_value(&payload.jsep)
                                .context("Error serializing JSEP")
                                .error(AppErrorKind::MessageBuildingFailed)?;
                            let body = ReadStreamRequestBody::new(
                                payload.handle_id.rtc_id(),
                                reqp.as_agent_id().clone(),
                            );
                            let payload = MessageRequest::new(
                                &Uuid::new_v4().to_string(),
                                payload.handle_id.janus_session_id(),
                                payload.handle_id.janus_handle_id(),
                                serde_json::to_value(&body)
                                    .map_err(anyhow::Error::from)
                                    .error(AppErrorKind::MessageBuildingFailed)?,
                                Some(jsep),
                            );
                            let stream_read = context
                                .janus_http_client()
                                .read_stream(&payload)
                                .await
                                .context("Stream read error")
                                .error(AppErrorKind::MessageBuildingFailed)?;
                            stream_read
                                .plugin()
                                .data()
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
                                    let jsep = stream_read
                                        .jsep()
                                        .ok_or_else(|| anyhow!("Missing 'jsep' in the response"))
                                        .error(AppErrorKind::MessageParsingFailed)?;

                                    let timing = ShortTermTimingProperties::until_now(
                                        context.start_timestamp(),
                                    );

                                    let resp = endpoint::rtc_signal::CreateResponse::unicast(
                                        endpoint::rtc_signal::CreateResponseData::new(Some(
                                            jsep.clone(),
                                        )),
                                        reqp.to_response(ResponseStatus::OK, timing),
                                        reqp.as_agent_id(),
                                        JANUS_API_VERSION,
                                    );

                                    let boxed_resp =
                                        Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
                                    Ok(boxed_resp)
                                    // Ok(Box::new(stream::once(boxed_resp)) as MessageStream)
                                })?
                            // .or_else(|err| Ok(handle_response_error(context, &reqp, err)))?
                            // context
                            //     .janus_client()
                            //     .read_stream_request(
                            //         reqp.clone(),
                            //         payload.handle_id.janus_session_id(),
                            //         payload.handle_id.janus_handle_id(),
                            //         payload.handle_id.rtc_id(),
                            //         jsep,
                            //         payload.handle_id.backend_id(),
                            //         context.start_timestamp(),
                            //         authz_time,
                            //     )
                            //     .map(|req| Box::new(req) as Box<dyn IntoPublishableMessage + Send>)
                            //     .context("Error creating a backend request")
                            //     .error(AppErrorKind::MessageBuildingFailed)?
                        } else {
                            context
                                .add_logger_tags(o!("sdp_type" => "offer", "intent" => "update"));

                            if room.rtc_sharing_policy() == db::rtc::SharingPolicy::Owned
                                && reqp.as_agent_id() != rtc.created_by()
                            {
                                return Err(anyhow!("Signaling to other's RTC with sendonly or sendrecv SDP is not allowed"))
                                    .error(AppErrorKind::AccessDenied);
                            }

                            // Authorization
                            let authz_time =
                                authorize(context, &payload, reqp, "update", &room).await?;

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

                            let jsep = serde_json::to_value(&payload.jsep)
                                .context("Error serializing JSEP")
                                .error(AppErrorKind::MessageBuildingFailed)?;
                            let agent_id = reqp.as_agent_id().to_owned();
                            let body =
                                CreateStreamRequestBody::new(payload.handle_id.rtc_id(), agent_id);

                            let transaction =
                                Transaction::CreateStream(TransactionData::new(reqp.clone()));

                            let payload = MessageRequest::new(
                                &to_base64(&transaction)
                                    .error(AppErrorKind::MessageBuildingFailed)?,
                                payload.handle_id.janus_session_id(),
                                payload.handle_id.janus_handle_id(),
                                serde_json::to_value(&body)
                                    .map_err(anyhow::Error::from)
                                    .error(AppErrorKind::MessageBuildingFailed)?,
                                Some(jsep),
                            );
                            let create_resp = context
                                .janus_http_client()
                                .create_stream(&payload)
                                .await
                                .context("Stream create error")
                                .error(AppErrorKind::MessageBuildingFailed)?;
                            return Ok(Box::new(stream::empty()));
                            // create_resp
                            //     .plugin()
                            //     .data()
                            //     .ok_or_else(|| anyhow!("Missing 'data' in the response"))
                            //     .error(AppErrorKind::MessageParsingFailed)?
                            //     .get("status")
                            //     .ok_or_else(|| anyhow!("Missing 'status' in the response"))
                            //     .error(AppErrorKind::MessageParsingFailed)
                            //     .and_then(|status| {
                            //         context.add_logger_tags(o!("status" => status.as_u64()));

                            //         if status == "200" {
                            //             Ok(())
                            //         } else {
                            //             Err(anyhow!("Received error status"))
                            //                 .error(AppErrorKind::BackendRequestFailed)
                            //         }
                            //     })
                            //     .and_then(|_| {
                            //         // Getting answer (as JSEP)
                            //         let jsep = create_resp
                            //             .jsep()
                            //             .ok_or_else(|| anyhow!("Missing 'jsep' in the response"))
                            //             .error(AppErrorKind::MessageParsingFailed)?;

                            //         let timing = ShortTermTimingProperties::until_now(
                            //             context.start_timestamp(),
                            //         );

                            //         let resp = endpoint::rtc_signal::CreateResponse::unicast(
                            //             endpoint::rtc_signal::CreateResponseData::new(Some(
                            //                 jsep.clone(),
                            //             )),
                            //             reqp.to_response(ResponseStatus::OK, timing),
                            //             reqp.as_agent_id(),
                            //             JANUS_API_VERSION,
                            //         );

                            //         let boxed_resp =
                            //             Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
                            //         Ok(boxed_resp)
                            //     })?
                            // .or_else(|err| Ok(handle_response_error(context, &reqp, err)))?
                            // context
                            //     .janus_client()
                            //     .create_stream_request(
                            //         reqp.clone(),
                            //         payload.handle_id.janus_session_id(),
                            //         payload.handle_id.janus_handle_id(),
                            //         payload.handle_id.rtc_id(),
                            //         jsep,
                            //         payload.handle_id.backend_id(),
                            //         context.start_timestamp(),
                            //         authz_time,
                            //     )
                            //     .map(|req| Box::new(req) as Box<dyn IntoPublishableMessage + Send>)
                            //     .context("Error creating a backend request")
                            //     .error(AppErrorKind::MessageBuildingFailed)?
                        }
                    }
                    JsepType::Answer => Err(anyhow!("sdp_type = 'answer' is not allowed"))
                        .error(AppErrorKind::InvalidSdpType)?,
                }
            }
            Jsep::IceCandidate(_) => {
                context.add_logger_tags(o!("sdp_type" => "ice_candidate", "intent" => "read"));

                // Authorization
                let authz_time = authorize(context, &payload, reqp, "read", &room).await?;

                let jsep = serde_json::to_value(&payload.jsep)
                    .context("Error serializing JSEP")
                    .error(AppErrorKind::MessageBuildingFailed)?;

                context
                    .janus_client()
                    .trickle_request(
                        reqp.clone(),
                        payload.handle_id.janus_session_id(),
                        payload.handle_id.janus_handle_id(),
                        jsep,
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
    let object = vec!["rooms", &room_id, "rtcs", &rtc_id];

    context
        .authz()
        .authorize(room.audience(), reqp, object, action)
        .await
        .map_err(AppError::from)
}

fn handle_response_error<C: Context>(
    context: &mut C,
    reqp: &IncomingRequestProperties,
    app_error: AppError,
) -> Box<dyn IntoPublishableMessage + Send> {
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
    boxed_resp
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
        use uuid::Uuid;

        use crate::app::handle_id::HandleId;
        use crate::backend::janus;
        use crate::db::rtc::SharingPolicy as RtcSharingPolicy;
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

        #[async_std::test]
        async fn offer() -> std::io::Result<()> {
            let db = TestDb::new();
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and rtc and an agent connection.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
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

            // Allow user to update the rtc.
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
            assert_eq!(payload.handle_id, agent_connection.handle_id());
            assert_eq!(payload.body.method, "stream.create");
            assert_eq!(payload.body.id, rtc.id());
            assert_eq!(payload.jsep.r#type, "offer");
            assert_eq!(payload.jsep.sdp, SDP_OFFER);

            // Assert rtc stream presence in the DB.
            let conn = context.get_conn().unwrap();
            let query = crate::schema::janus_rtc_stream::table.find(rtc_stream_id);

            let rtc_stream: crate::db::janus_rtc_stream::Object = query.get_result(&conn).unwrap();

            assert_eq!(rtc_stream.handle_id(), agent_connection.handle_id());
            assert_eq!(rtc_stream.rtc_id(), rtc.id());
            assert_eq!(rtc_stream.backend_id(), backend.id());
            assert_eq!(rtc_stream.label(), "whatever");
            assert_eq!(rtc_stream.sent_by(), agent.agent_id());
            Ok(())
        }

        #[async_std::test]
        async fn offer_unauthorized() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and rtc and an agent connection.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
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
                Uuid::new_v4(),
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

        #[async_std::test]
        async fn answer() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and rtc and an agent connection.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
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
                Uuid::new_v4(),
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

        #[async_std::test]
        async fn candidate() -> std::io::Result<()> {
            let db = TestDb::new();
            let mut authz = TestAuthz::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and rtc and an agent connection.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
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

            // Allow user to read the rtc.
            let room_id = rtc.room_id().to_string();
            let rtc_id = rtc.id().to_string();
            let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, authz);

            let handle_id = HandleId::new(
                Uuid::new_v4(),
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
            assert_eq!(payload.handle_id, agent_connection.handle_id());
            assert_eq!(payload.candidate.sdp_m_id, "v");
            assert_eq!(payload.candidate.sdp_m_line_index, 0);
            assert_eq!(payload.candidate.candidate, ICE_CANDIDATE);
            Ok(())
        }

        #[async_std::test]
        async fn candidate_unauthorized() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and rtc and an agent connection.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
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
                Uuid::new_v4(),
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

        #[async_std::test]
        async fn wrong_rtc_id() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and entered agent.
            let backend = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    backend
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let handle_id = HandleId::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                12345,
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

        #[async_std::test]
        async fn rtc_id_from_another_room() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend and RTC and an RTC in another room.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
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
                Uuid::new_v4(),
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

        #[async_std::test]
        async fn wrong_backend_id() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend, rtc and connected agent and another backend.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    let (_, agent_connection) = shared_helpers::insert_connected_agent(
                        &conn,
                        agent.agent_id(),
                        rtc.room_id(),
                        rtc.id(),
                    );

                    let other_backend = shared_helpers::insert_janus_backend(&conn);
                    (other_backend, rtc, agent_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let handle_id = HandleId::new(
                Uuid::new_v4(),
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

        #[async_std::test]
        async fn offline_backend() -> std::io::Result<()> {
            let db = TestDb::new();
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
                Uuid::new_v4(),
                rtc.id(),
                agent_connection.handle_id(),
                12345,
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

        #[async_std::test]
        async fn not_entered() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend, rtc and connected agent and another backend.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);
                    (backend, rtc)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let handle_id = HandleId::new(
                Uuid::new_v4(),
                rtc.id(),
                12345,
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

        #[async_std::test]
        async fn not_connected() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend, rtc and entered agent.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    (backend, rtc)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let handle_id = HandleId::new(
                Uuid::new_v4(),
                rtc.id(),
                12345,
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

        #[async_std::test]
        async fn wrong_handle_id() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert room with backend, rtc and a connect agent.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
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
                Uuid::new_v4(),
                rtc.id(),
                789,
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

        #[async_std::test]
        async fn spoof_handle_id() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);

            // Insert room with backend, rtc and 2 connect agents.
            let (backend, rtc, agent2_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    let rtc = shared_helpers::insert_rtc_with_room(&conn, &room);

                    shared_helpers::insert_connected_agent(
                        &conn,
                        agent1.agent_id(),
                        room.id(),
                        rtc.id(),
                    );

                    let agent = shared_helpers::insert_agent(&conn, agent2.agent_id(), room.id());

                    let agent2_connection =
                        factory::AgentConnection::new(*agent.id(), rtc.id(), 456).insert(&conn);

                    (backend, rtc, agent2_connection)
                })
                .unwrap();

            // Make rtc_signal.create request.
            let mut context = TestContext::new(db, TestAuthz::new());

            let handle_id = HandleId::new(
                Uuid::new_v4(),
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

        #[async_std::test]
        async fn closed_room() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            // Insert closed room with backend, rtc and connected agent.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);

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
                Uuid::new_v4(),
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

        #[async_std::test]
        async fn spoof_owned_rtc() -> std::io::Result<()> {
            let db = TestDb::new();
            let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
            let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
            let now = Utc::now();

            // Insert room with backend, rtc owned by agent2 and connect agent1.
            let (backend, rtc, agent_connection) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    let backend = shared_helpers::insert_janus_backend(&conn);

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
                Uuid::new_v4(),
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
