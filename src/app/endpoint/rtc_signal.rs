use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use failure::{err_msg, format_err, Error};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{IncomingRequest, OutgoingResponse, ResponseStatus};
use svc_agent::{Addressable, AgentId};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::endpoint;
use crate::db::{janus_rtc_stream, room, ConnectionPool};

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    handle_id: HandleId,
    jsep: JsonValue,
    label: Option<String>,
}

pub(crate) type CreateResponse = OutgoingResponse<CreateResponseData>;

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

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct State {
    authz: svc_authz::ClientMap,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(authz: svc_authz::ClientMap, db: ConnectionPool) -> Self {
        Self { authz, db }
    }
}

impl State {
    pub(crate) async fn create(
        &self,
        inreq: CreateRequest,
        start_timestamp: DateTime<Utc>,
    ) -> endpoint::Result {
        let handle_id = &inreq.payload().handle_id;
        let jsep = &inreq.payload().jsep;

        let sdp_type = match parse_sdp_type(jsep) {
            Ok(sdp_type) => sdp_type,
            Err(e) => {
                return SvcError::builder()
                    .status(ResponseStatus::BAD_REQUEST)
                    .detail(&format!("invalid jsep format, {}", &e))
                    .build()
                    .into();
            }
        };

        match sdp_type {
            SdpType::Offer => {
                if is_sdp_recvonly(jsep).map_err(|e| {
                    SvcError::builder()
                        .status(ResponseStatus::BAD_REQUEST)
                        .detail(&format!("invalid jsep format, {}", &e))
                        .build()
                })? {
                    // Authorization
                    let authz_time = self.authorize(&inreq, "read").await?;

                    let result = crate::app::janus::read_stream_request(
                        inreq.properties().clone(),
                        handle_id.janus_session_id(),
                        handle_id.janus_handle_id(),
                        handle_id.rtc_id(),
                        jsep.clone(),
                        handle_id.backend_id(),
                        start_timestamp,
                        authz_time,
                    );

                    match result {
                        Ok(req) => req.into(),
                        Err(_) => SvcError::builder()
                            .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                            .detail("error creating a backend request")
                            .build()
                            .into(),
                    }
                } else {
                    // Authorization
                    let authz_time = self.authorize(&inreq, "update").await?;

                    // Updating the Real-Time Connection state
                    {
                        let label = inreq.payload().label.as_ref().ok_or_else(|| {
                            SvcError::builder()
                                .status(ResponseStatus::BAD_REQUEST)
                                .detail("missing label")
                                .build()
                        })?;

                        let conn = self.db.get()?;
                        janus_rtc_stream::InsertQuery::new(
                            handle_id.rtc_stream_id(),
                            handle_id.janus_handle_id(),
                            handle_id.rtc_id(),
                            handle_id.backend_id(),
                            label,
                            inreq.properties().as_agent_id(),
                        )
                        .execute(&conn)?;
                    }

                    let result = crate::app::janus::create_stream_request(
                        inreq.properties().clone(),
                        handle_id.janus_session_id(),
                        handle_id.janus_handle_id(),
                        handle_id.rtc_id(),
                        jsep.clone(),
                        handle_id.backend_id(),
                        start_timestamp,
                        authz_time,
                    );

                    match result {
                        Ok(req) => req.into(),
                        Err(_) => SvcError::builder()
                            .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                            .detail("error creating a backend request")
                            .build()
                            .into(),
                    }
                }
            }
            SdpType::Answer => Err(SvcError::builder()
                .status(ResponseStatus::BAD_REQUEST)
                .detail("sdp_type = 'answer' is not allowed")
                .build())?,
            SdpType::IceCandidate => {
                // Authorization
                let authz_time = self.authorize(&inreq, "read").await?;

                let result = crate::app::janus::trickle_request(
                    inreq.properties().clone(),
                    handle_id.janus_session_id(),
                    handle_id.janus_handle_id(),
                    jsep.clone(),
                    handle_id.backend_id(),
                    start_timestamp,
                    authz_time,
                );

                match result {
                    Ok(req) => req.into(),
                    Err(_) => SvcError::builder()
                        .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                        .detail("error creating a backend request")
                        .build()
                        .into(),
                }
            }
        }
    }

    // Authorization: room's owner has to allow the action
    async fn authorize(&self, inreq: &CreateRequest, action: &str) -> Result<Duration, SvcError> {
        let rtc_id = inreq.payload().handle_id.rtc_id();

        let room = {
            let conn = self.db.get()?;
            room::FindQuery::new()
                .time(room::now())
                .rtc_id(rtc_id)
                .execute(&conn)?
                .ok_or_else(|| {
                    SvcError::builder()
                        .status(ResponseStatus::NOT_FOUND)
                        .detail(&format!("a room for the rtc = '{}' is not found", &rtc_id))
                        .build()
                })?
        };

        if room.backend() != &room::RoomBackend::Janus {
            return Err(SvcError::builder()
                .status(ResponseStatus::NOT_IMPLEMENTED)
                .detail(&format!(
                    "'rtc_signal.create' is not implemented for the backend = '{}'.",
                    room.backend()
                ))
                .build());
        }

        let room_id = room.id().to_string();
        let rtc_id = rtc_id.to_string();

        self.authz
            .authorize(
                room.audience(),
                inreq.properties(),
                vec!["rooms", &room_id, "rtcs", &rtc_id],
                action,
            )
            .map_err(|err| SvcError::from(err))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum SdpType {
    Offer,
    Answer,
    IceCandidate,
}

fn parse_sdp_type(jsep: &JsonValue) -> Result<SdpType, Error> {
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
        _ => Err(format_err!("invalid jsep = '{}'", jsep)),
    }
}

fn is_sdp_recvonly(jsep: &JsonValue) -> Result<bool, Error> {
    use webrtc_sdp::{attribute_type::SdpAttributeType, parse_sdp};

    let sdp = jsep.get("sdp").ok_or_else(|| err_msg("missing sdp"))?;
    let sdp = sdp
        .as_str()
        .ok_or_else(|| format_err!("invalid sdp = '{}'", sdp))?;
    let sdp = parse_sdp(sdp, false).map_err(|_| err_msg("invalid sdp"))?;

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

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct HandleId {
    rtc_stream_id: Uuid,
    rtc_id: Uuid,
    janus_handle_id: i64,
    janus_session_id: i64,
    backend_id: AgentId,
}

impl HandleId {
    pub(crate) fn rtc_stream_id(&self) -> Uuid {
        self.rtc_stream_id
    }

    pub(crate) fn rtc_id(&self) -> Uuid {
        self.rtc_id
    }

    pub(crate) fn janus_handle_id(&self) -> i64 {
        self.janus_handle_id
    }

    pub(crate) fn janus_session_id(&self) -> i64 {
        self.janus_session_id
    }

    pub(crate) fn backend_id(&self) -> &AgentId {
        &self.backend_id
    }
}

impl HandleId {
    pub(crate) fn new(
        rtc_stream_id: Uuid,
        rtc_id: Uuid,
        janus_handle_id: i64,
        janus_session_id: i64,
        backend_id: AgentId,
    ) -> Self {
        Self {
            rtc_stream_id,
            rtc_id,
            janus_handle_id,
            janus_session_id,
            backend_id,
        }
    }
}

impl fmt::Display for HandleId {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}.{}.{}.{}.{}",
            self.rtc_stream_id,
            self.rtc_id,
            self.janus_handle_id,
            self.janus_session_id,
            self.backend_id
        )
    }
}

impl FromStr for HandleId {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(5, '.').collect();
        match parts[..] {
            [ref rtc_stream_id, ref rtc_id, ref janus_handle_id, ref janus_session_id, ref rest] => {
                Ok(Self::new(
                    Uuid::from_str(rtc_stream_id)?,
                    Uuid::from_str(rtc_id)?,
                    janus_handle_id.parse::<i64>()?,
                    janus_session_id.parse::<i64>()?,
                    rest.parse::<AgentId>()?,
                ))
            }
            _ => Err(format_err!("invalid value for the agent id: {}", val)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

mod serde {
    use serde::{de, ser};
    use std::fmt;

    use super::HandleId;

    impl ser::Serialize for HandleId {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: ser::Serializer,
        {
            serializer.serialize_str(&self.to_string())
        }
    }

    impl<'de> de::Deserialize<'de> for HandleId {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            struct AgentIdVisitor;

            impl<'de> de::Visitor<'de> for AgentIdVisitor {
                type Value = HandleId;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("struct HandleId")
                }

                fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    use std::str::FromStr;

                    HandleId::from_str(v)
                        .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(v), &self))
                }
            }

            deserializer.deserialize_str(AgentIdVisitor)
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::Try;

    use diesel::prelude::*;
    use serde_json::json;
    use uuid::Uuid;

    use crate::test_helpers::{
        agent::TestAgent,
        authz::{no_authz, TestAuthz},
        db::TestDb,
        extract_payload,
        factory::{insert_janus_backend, insert_rtc},
    };

    use super::*;

    const AUDIENCE: &str = "dev.svc.example.org";

    ///////////////////////////////////////////////////////////////////////////

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

    #[test]
    fn create_rtc_signal_for_offer() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert a janus backend and an rtc.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    (
                        insert_janus_backend(&conn, AUDIENCE),
                        insert_rtc(&conn, AUDIENCE),
                    )
                })
                .unwrap();

            // Allow user to update the rtc.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = rtc.room_id().to_string();
            let rtc_id = rtc.id().to_string();
            let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "update");

            // Make rtc_signal.create request.
            let rtc_stream_id = Uuid::new_v4();

            let handle_id = format!(
                "{}.{}.{}.{}.{}",
                rtc_stream_id,
                rtc.id(),
                backend.handle_id(),
                backend.session_id(),
                backend.id(),
            );

            let payload = json!({
                "handle_id": handle_id,
                "jsep": {"type": "offer", "sdp": SDP_OFFER},
                "label": "whatever",
            });

            let state = State::new(authz.into(), db.connection_pool().clone());
            let method = "rtc_signal.create";
            let request: CreateRequest = agent.build_request(method, &payload).unwrap();
            let mut result = state
                .create(request, Utc::now())
                .await
                .into_result()
                .unwrap();
            let outgoing_envelope = result.remove(0);

            // Assert outgoing broker request.
            let req: RtcSignalCreateJanusRequestOffer = extract_payload(outgoing_envelope).unwrap();

            assert_eq!(req.janus, "message");
            assert_eq!(req.session_id, backend.session_id());
            assert_eq!(req.handle_id, backend.handle_id());
            assert_eq!(req.body.method, "stream.create");
            assert_eq!(req.body.id, rtc.id());
            assert_eq!(req.jsep.r#type, "offer");
            assert_eq!(req.jsep.sdp, SDP_OFFER);

            // Assert rtc stream presence in the DB.
            let conn = db.connection_pool().get().unwrap();
            let query = crate::schema::janus_rtc_stream::table.find(rtc_stream_id);
            let rtc_stream: crate::db::janus_rtc_stream::Object = query.get_result(&conn).unwrap();
            assert_eq!(rtc_stream.handle_id(), backend.handle_id());
            assert_eq!(rtc_stream.rtc_id(), rtc.id());
            assert_eq!(rtc_stream.backend_id(), backend.id());
            assert_eq!(rtc_stream.label(), "whatever");
            assert_eq!(rtc_stream.sent_by(), agent.agent_id());
        });
    }

    #[test]
    fn create_rtc_signal_for_offer_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert a janus backend and an rtc.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    (
                        insert_janus_backend(&conn, AUDIENCE),
                        insert_rtc(&conn, AUDIENCE),
                    )
                })
                .unwrap();

            // Make rtc_signal.create request.
            let rtc_stream_id = Uuid::new_v4();

            let handle_id = format!(
                "{}.{}.{}.{}.{}",
                rtc_stream_id,
                rtc.id(),
                backend.handle_id(),
                backend.session_id(),
                backend.id(),
            );

            let payload = json!({
                "handle_id": handle_id,
                "jsep": {"type": "offer", "sdp": SDP_OFFER},
                "label": "whatever",
            });

            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let state = State::new(authz.into(), db.connection_pool().clone());
            let method = "rtc_signal.create";
            let request: CreateRequest = agent.build_request(method, &payload).unwrap();
            let result = state.create(request, Utc::now()).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected rtc_signal.create to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

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
        futures::executor::block_on(async {
            let db = TestDb::new();

            // Insert a janus backend and an rtc.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    (
                        insert_janus_backend(&conn, AUDIENCE),
                        insert_rtc(&conn, AUDIENCE),
                    )
                })
                .unwrap();

            // Make rtc_signal.create request.
            let handle_id = format!(
                "{}.{}.{}.{}.{}",
                Uuid::new_v4(),
                rtc.id(),
                backend.handle_id(),
                backend.session_id(),
                backend.id(),
            );

            let payload = json!({
                "handle_id": handle_id,
                "jsep": {"type": "answer", "sdp": SDP_ANSWER},
                "label": "whatever",
            });

            let state = State::new(no_authz(AUDIENCE), db.connection_pool().clone());
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let method = "rtc_signal.create";
            let request: CreateRequest = agent.build_request(method, &payload).unwrap();
            let result = state.create(request, Utc::now()).await.into_result();

            // Expecting error 400.
            match result {
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::BAD_REQUEST),
                _ => panic!("Expected rtc_signal.create to fail"),
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////

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
        futures::executor::block_on(async {
            let db = TestDb::new();
            let mut authz = TestAuthz::new(AUDIENCE);

            // Insert a janus backend and an rtc.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    (
                        insert_janus_backend(&conn, AUDIENCE),
                        insert_rtc(&conn, AUDIENCE),
                    )
                })
                .unwrap();

            // Allow user to read the rtc.
            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let room_id = rtc.room_id().to_string();
            let rtc_id = rtc.id().to_string();
            let object = vec!["rooms", &room_id, "rtcs", &rtc_id];
            authz.allow(agent.account_id(), object, "read");

            // Make rtc_signal.create request.
            let rtc_stream_id = Uuid::new_v4();

            let handle_id = format!(
                "{}.{}.{}.{}.{}",
                rtc_stream_id,
                rtc.id(),
                backend.handle_id(),
                backend.session_id(),
                backend.id(),
            );

            let payload = json!({
                "handle_id": handle_id,
                "jsep": {"sdpMid": 0, "sdpMLineIndex": 0, "candidate": ICE_CANDIDATE},
            });

            let state = State::new(authz.into(), db.connection_pool().clone());
            let method = "rtc_signal.create";
            let request: CreateRequest = agent.build_request(method, &payload).unwrap();
            let mut result = state
                .create(request, Utc::now())
                .await
                .into_result()
                .unwrap();
            let message = result.remove(0);

            // Assert outgoing broker request.
            let req: RtcSignalCreateJanusRequestIceCandidate = extract_payload(message).unwrap();
            assert_eq!(req.janus, "trickle");
            assert_eq!(req.session_id, backend.session_id());
            assert_eq!(req.handle_id, backend.handle_id());
            assert_eq!(req.candidate.sdp_m_id, 0);
            assert_eq!(req.candidate.sdp_m_line_index, 0);
            assert_eq!(req.candidate.candidate, ICE_CANDIDATE);
        });
    }

    #[test]
    fn create_rtc_signal_for_candidate_unauthorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let authz = TestAuthz::new(AUDIENCE);

            // Insert a janus backend and an rtc.
            let (backend, rtc) = db
                .connection_pool()
                .get()
                .map(|conn| {
                    (
                        insert_janus_backend(&conn, AUDIENCE),
                        insert_rtc(&conn, AUDIENCE),
                    )
                })
                .unwrap();

            // Make rtc_signal.create request.
            let rtc_stream_id = Uuid::new_v4();

            let handle_id = format!(
                "{}.{}.{}.{}.{}",
                rtc_stream_id,
                rtc.id(),
                backend.handle_id(),
                backend.session_id(),
                backend.id(),
            );

            let payload = json!({
                "handle_id": handle_id,
                "jsep": {"sdpMid": 0, "sdpMLineIndex": 0, "candidate": ICE_CANDIDATE},
            });

            let agent = TestAgent::new("web", "user123", AUDIENCE);
            let state = State::new(authz.into(), db.connection_pool().clone());
            let method = "rtc_signal.create";
            let request: CreateRequest = agent.build_request(method, &payload).unwrap();
            let result = state.create(request, Utc::now()).await.into_result();

            // Assert 403 error response.
            match result {
                Ok(_) => panic!("Expected rtc_signal.create to fail"),
                Err(err) => assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN),
            }
        });
    }
}
