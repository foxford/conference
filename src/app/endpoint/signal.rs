use std::collections::BTreeMap;
use std::fmt;
use std::result::Result as StdResult;

use anyhow::{anyhow, Context as AnyhowContext};
use async_std::stream;
use async_trait::async_trait;
use diesel::pg::PgConnection;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::{
    mqtt::{IncomingRequestProperties, IntoPublishableMessage, OutgoingResponse},
    Addressable,
};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::db;
use crate::db::janus_backend::Object as JanusBackend;
use crate::db::room::Object as Room;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct CreateResponseData {
    jsep: JsonValue,
}

impl CreateResponseData {
    pub(crate) fn new(jsep: JsonValue) -> Self {
        Self { jsep }
    }
}

pub(crate) type CreateResponse = OutgoingResponse<CreateResponseData>;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum CreateJsep {
    Offer { sdp: String },
    Answer,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequest {
    room_id: Uuid,
    jsep: CreateJsep,
}

pub(crate) struct CreateHandler;

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        // Validate SDP type.
        let sdp = match payload.jsep {
            CreateJsep::Offer { ref sdp } => Ok(sdp),
            CreateJsep::Answer => {
                let err = anyhow!("SDP type 'answer' is invalid for signal.create method");
                Err(err).error(AppErrorKind::InvalidSdpType)
            }
        }?;

        // The room must be open, have backend = 'janus' and the agent must be entered there.
        let room =
            helpers::find_room_by_id(context, payload.room_id, helpers::RoomTimeRequirement::Open)?;

        if room.backend() != db::room::RoomBackend::Janus {
            let err = anyhow!("Room backend not supported for signal.create");
            return Err(err).error(AppErrorKind::UnsupportedBackend);
        }

        let conn = context.get_conn()?;
        helpers::check_room_presence(&room, &reqp.as_agent_id(), &conn)?;

        // Get room's backend if present or perform the balancing.
        let backend = match room.backend_id() {
            None => balance_room_to_backend(&conn, &room)?,
            Some(backend_id) => db::janus_backend::FindQuery::new()
                .id(backend_id)
                .execute(&conn)?
                .ok_or_else(|| anyhow!("No backend found for stream"))
                .error(AppErrorKind::BackendNotFound)?,
        };

        context.add_logger_tags(o!("backend_id" => backend.id().to_string()));

        // Check that the backend's capacity is not exceeded.
        let is_reader = is_sdp_recvonly(sdp)
            .context("Invalid JSEP format")
            .error(AppErrorKind::InvalidSdpType)?;

        if is_reader && db::janus_backend::free_capacity(room.id(), &conn)? == 0 {
            return Err(anyhow!(
                "Active agents number on the backend exceeded its capacity"
            ))
            .error(AppErrorKind::CapacityExceeded);
        }

        // Make janus handle creation request.
        let jsep = serde_json::to_value(&payload.jsep)
            .context("Error serializing JSEP")
            .error(AppErrorKind::MessageBuildingFailed)?;

        let janus_request_result = context.janus_client().create_agent_handle_request(
            reqp.clone(),
            payload.room_id,
            backend.session_id(),
            jsep,
            backend.id(),
            context.start_timestamp(),
        );

        match janus_request_result {
            Ok(req) => {
                let boxed_request = Box::new(req) as Box<dyn IntoPublishableMessage + Send>;
                Ok(Box::new(stream::once(boxed_request)))
            }
            Err(err) => Err(err.context("Error creating a backend request"))
                .error(AppErrorKind::MessageBuildingFailed),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct TrickleResponseData {}

impl TrickleResponseData {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

pub(crate) type TrickleResponse = OutgoingResponse<TrickleResponseData>;

////////////////////////////////////////////////////////////////////////////////

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
enum TrickleJsepItem {
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
enum TrickleJsep {
    // {"sdpMid": _, "sdpMLineIndex": _, "candidate": _}
    Single(TrickleJsepItem),
    // [{"sdpMid": _, "sdpMLineIndex": _, "candidate": _}, …, {"completed": true}]
    List(Vec<TrickleJsepItem>),
}

#[derive(Debug, Deserialize)]
pub(crate) struct TrickleRequest {
    room_id: Uuid,
    jsep: TrickleJsep,
}

pub(crate) struct TrickleHandler;

#[async_trait]
impl RequestHandler for TrickleHandler {
    type Payload = TrickleRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        // Find room.
        let room =
            helpers::find_room_by_id(context, payload.room_id, helpers::RoomTimeRequirement::Open)?;

        // Find agent connection and backend.
        let (agent_connection, backend) =
            helpers::find_agent_connection_with_backend(context, reqp.as_agent_id(), &room)?;

        // Make janus trickle request.
        let jsep = serde_json::to_value(&payload.jsep)
            .context("Error serializing JSEP")
            .error(AppErrorKind::MessageBuildingFailed)?;

        let janus_request_result = context.janus_client().trickle_request(
            reqp.clone(),
            backend.id(),
            backend.session_id(),
            agent_connection.handle_id(),
            jsep,
            context.start_timestamp(),
        );

        match janus_request_result {
            Ok(req) => {
                let boxed_request = Box::new(req) as Box<dyn IntoPublishableMessage + Send>;
                Ok(Box::new(stream::once(boxed_request)))
            }
            Err(err) => Err(err.context("Error creating a backend request"))
                .error(AppErrorKind::MessageBuildingFailed),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct UpdateResponseData {
    jsep: JsonValue,
}

impl UpdateResponseData {
    pub(crate) fn new(jsep: JsonValue) -> Self {
        Self { jsep }
    }
}

pub(crate) type UpdateResponse = OutgoingResponse<UpdateResponseData>;

////////////////////////////////////////////////////////////////////////////////

type UpdateJsep = CreateJsep;

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateRequest {
    room_id: Uuid,
    jsep: UpdateJsep,
}

pub(crate) struct UpdateHandler;

#[async_trait]
impl RequestHandler for UpdateHandler {
    type Payload = UpdateRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        // Validate SDP type.
        match payload.jsep {
            CreateJsep::Offer { sdp: _ } => Ok(()),
            CreateJsep::Answer => {
                let err = anyhow!("SDP type 'answer' is invalid for signal.update method");
                Err(err).error(AppErrorKind::InvalidSdpType)
            }
        }?;

        // Find room.
        let room =
            helpers::find_room_by_id(context, payload.room_id, helpers::RoomTimeRequirement::Open)?;

        // Find agent connection and backend.
        let (agent_connection, backend) =
            helpers::find_agent_connection_with_backend(context, reqp.as_agent_id(), &room)?;

        // Make janus `signal.update` request.
        let jsep = serde_json::to_value(&payload.jsep)
            .context("Error serializing JSEP")
            .error(AppErrorKind::MessageBuildingFailed)?;

        let janus_request_result = context.janus_client().update_signal_request(
            reqp.clone(),
            backend.id(),
            backend.session_id(),
            agent_connection.handle_id(),
            jsep,
            context.start_timestamp(),
        );

        match janus_request_result {
            Ok(req) => {
                let boxed_request = Box::new(req) as Box<dyn IntoPublishableMessage + Send>;
                Ok(Box::new(stream::once(boxed_request)))
            }
            Err(err) => Err(err.context("Error creating a backend request"))
                .error(AppErrorKind::MessageBuildingFailed),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum SdpType {
    Offer,
    Answer,
    IceCandidate,
}

impl fmt::Display for SdpType {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Offer => write!(fmt, "offer"),
            Self::Answer => write!(fmt, "answer"),
            Self::IceCandidate => write!(fmt, "ice_candidate"),
        }
    }
}

fn is_sdp_recvonly(sdp: &str) -> anyhow::Result<bool> {
    use webrtc_sdp::{attribute_type::SdpAttributeType, parse_sdp};
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

fn balance_room_to_backend(conn: &PgConnection, room: &Room) -> StdResult<JanusBackend, AppError> {
    // Greedy balancing: try to choose the most loaded backend capable to host the room's reserve.
    let backend = match db::janus_backend::most_loaded(room.id(), &conn)? {
        Some(backend) => backend,
        // Fall back to the least loaded backend with a warning.
        None => db::janus_backend::least_loaded(room.id(), &conn)?
            .map(|backend| {
                warn!(crate::LOG, "No capable backends to host the reserve; falling back to the least loaded backend: room_id = {}, backend_id = {}", room.id(), backend.id());
                notify_sentry_on_backend_fallback(&room, &backend);
                backend
            })
            .ok_or_else(|| anyhow!("No available backends"))
            .error(AppErrorKind::NoAvailableBackends)?,
    };

    db::room::UpdateQuery::new(room.id())
        .backend_id(Some(backend.id()))
        .execute(&conn)?;

    Ok(backend)
}

fn notify_sentry_on_backend_fallback(room: &Room, backend: &JanusBackend) {
    use sentry::protocol::{value::Value, Event, Level};

    let room_id = room.id().to_string();
    let backend_id = backend.id().to_string();

    let mut extra = BTreeMap::new();
    extra.insert(String::from("room_id"), Value::from(room_id));
    extra.insert(String::from("backend_id"), Value::from(backend_id));

    if let Some(reserve) = room.reserve() {
        extra.insert(String::from("reserve"), Value::from(reserve));
    }

    sentry::capture_event(Event {
        message: Some(String::from(
            "No capable backends to host the reserve; falling back to the least loaded backend",
        )),
        level: Level::Warning,
        extra,
        ..Default::default()
    });
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    const SDP_OFFER_SENDRECV: &str = r#"
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

    const SDP_OFFER_RECVONLY: &str = r#"
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
    a=recvonly
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
    a=recvonly
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

    const ICE_CANDIDATE: &str = "candidate:0 1 UDP 2113667327 198.51.100.7 49203 typ host";

    mod create {
        use std::ops::Bound;

        use chrono::{Duration, SubsecRound, Utc};
        use serde_json::json;
        use svc_agent::mqtt::ResponseStatus;

        use crate::app::API_VERSION;
        use crate::db::agent::Status as AgentStatus;
        use crate::db::room::RoomBackend;
        use crate::test_helpers::prelude::*;
        use crate::util::from_base64;

        use super::super::*;
        use super::{SDP_ANSWER, SDP_OFFER_RECVONLY, SDP_OFFER_SENDRECV};

        #[derive(Deserialize)]
        struct JanusAgentHandleCreateRequest {
            transaction: String,
            janus: String,
            session_id: i64,
        }

        #[test]
        fn create_signal() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                // Insert backend, room and enter agent there.
                let (backend, room) = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    (backend, room)
                };

                // Make `signal.create` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_SENDRECV }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Signal creation failed");

                // Assert outgoing request to Janus.
                let (payload, _reqp, topic) =
                    find_request::<JanusAgentHandleCreateRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    backend.id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(payload.janus, "attach");
                assert_eq!(payload.session_id, backend.session_id());

                let tn = from_base64::<JsonValue>(&payload.transaction)
                    .expect("Failed to parse transaction");

                let sdp = tn
                    .get("CreateAgentHandle")
                    .unwrap()
                    .get("jsep")
                    .unwrap()
                    .get("sdp")
                    .unwrap()
                    .as_str()
                    .unwrap();

                assert_eq!(sdp, SDP_OFFER_SENDRECV);

                // Assert agent status switched to `connected`.
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                let db_agents = db::agent::ListQuery::new()
                    .agent_id(agent.agent_id())
                    .room_id(room.id())
                    .execute(&conn)
                    .expect("Failed to get agents");

                assert_eq!(db_agents.len(), 1);
                assert_eq!(db_agents[0].status(), AgentStatus::Ready);
            });
        }

        #[test]
        fn create_signal_with_bounded_backend() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                // Insert backends, room and enter agent there.
                let (backend, room) = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let _backend1 = shared_helpers::insert_janus_backend(&conn);
                    let backend2 = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend2.id());
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    (backend2, room)
                };

                // Make `signal.create` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_SENDRECV }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Signal creation failed");

                // Assert outgoing request to Janus.
                let (_payload, _reqp, topic) =
                    find_request::<JanusAgentHandleCreateRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    backend.id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, expected_topic);
            });
        }

        #[test]
        fn create_signal_greedy_balancing() {
            async_std::task::block_on(async {
                let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
                let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
                let agent3 = TestAgent::new("web", "user3", USR_AUDIENCE);
                let agent4 = TestAgent::new("web", "user4", USR_AUDIENCE);
                let db = TestDb::new();

                let (backend, room) = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // The first backend is empty.
                    let backend1_id = {
                        let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                        agent.agent_id().to_owned()
                    };

                    let _backend1 = factory::JanusBackend::new(backend1_id, 1, 2)
                        .balancer_capacity(700)
                        .capacity(800)
                        .insert(&conn);

                    // The second one has some load.
                    let backend2_id = {
                        let agent = TestAgent::new("beta", "janus", SVC_AUDIENCE);
                        agent.agent_id().to_owned()
                    };

                    let backend2 = factory::JanusBackend::new(backend2_id, 3, 4)
                        .balancer_capacity(700)
                        .capacity(800)
                        .insert(&conn);

                    let room1 = shared_helpers::insert_room_with_backend_id(&conn, backend2.id());
                    shared_helpers::insert_connected_agent(&conn, agent1.agent_id(), room1.id());

                    // The third one is even more loaded but incapable to host the reserve.
                    let backend3_id = {
                        let agent = TestAgent::new("gamma", "janus", SVC_AUDIENCE);
                        agent.agent_id().to_owned()
                    };

                    let backend3 = factory::JanusBackend::new(backend3_id, 5, 6)
                        .balancer_capacity(100)
                        .capacity(150)
                        .insert(&conn);

                    let room2 = shared_helpers::insert_room_with_backend_id(&conn, backend3.id());
                    shared_helpers::insert_connected_agent(&conn, agent2.agent_id(), room2.id());
                    shared_helpers::insert_connected_agent(&conn, agent3.agent_id(), room2.id());

                    // Insert a room with a reserve to signal within. Put a ready agent there.
                    let now = Utc::now();

                    let room3 = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((
                            Bound::Included(now),
                            Bound::Excluded(now + Duration::hours(1)),
                        ))
                        .backend(RoomBackend::Janus)
                        .reserve(120)
                        .insert(&conn);

                    shared_helpers::insert_agent(&conn, agent4.agent_id(), room3.id());

                    // The greedy balancer should pick the second backend because it's the most
                    // loaded of those which are capable to host the reserve.
                    (backend2, room3)
                };

                // Make `signal.create` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_SENDRECV }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let messages = handle_request::<CreateHandler>(&mut context, &agent4, payload)
                    .await
                    .expect("Signal creation failed");

                // Assert outgoing request to Janus.
                let (_payload, _reqp, topic) =
                    find_request::<JanusAgentHandleCreateRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    backend.id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, expected_topic);
            });
        }

        #[test]
        fn create_signal_greedy_balancing_fallback() {
            async_std::task::block_on(async {
                let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
                let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
                let db = TestDb::new();

                let (backend, room) = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // The first backend is empty.
                    let backend1_id = {
                        let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                        agent.agent_id().to_owned()
                    };

                    let backend1 = factory::JanusBackend::new(backend1_id, 1, 2)
                        .balancer_capacity(100)
                        .capacity(150)
                        .insert(&conn);

                    // The second one has some load.
                    let backend2_id = {
                        let agent = TestAgent::new("beta", "janus", SVC_AUDIENCE);
                        agent.agent_id().to_owned()
                    };

                    let backend2 = factory::JanusBackend::new(backend2_id, 3, 4)
                        .balancer_capacity(100)
                        .capacity(150)
                        .insert(&conn);

                    let room1 = shared_helpers::insert_room_with_backend_id(&conn, backend2.id());
                    shared_helpers::insert_agent(&conn, agent1.agent_id(), room1.id());

                    // Insert a room with a reserve to signal within. Put a ready agent there.
                    let now = Utc::now();

                    let room2 = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((
                            Bound::Included(now),
                            Bound::Excluded(now + Duration::hours(1)),
                        ))
                        .backend(RoomBackend::Janus)
                        .reserve(200)
                        .insert(&conn);

                    shared_helpers::insert_agent(&conn, agent2.agent_id(), room2.id());

                    // No backend can host such a big reserve so the balancer should pick the
                    // least loaded one.
                    (backend1, room2)
                };

                // Make `signal.create` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_SENDRECV }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let messages = handle_request::<CreateHandler>(&mut context, &agent2, payload)
                    .await
                    .expect("Signal creation failed");

                // Assert outgoing request to Janus.
                let (_payload, _reqp, topic) =
                    find_request::<JanusAgentHandleCreateRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    backend.id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, expected_topic);
            });
        }

        #[test]
        fn create_signal_exceeded_capacity_for_reader() {
            async_std::task::block_on(async {
                let agent1 = TestAgent::new("web", "user1", USR_AUDIENCE);
                let agent2 = TestAgent::new("web", "user2", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Insert backend.
                    let backend_id = {
                        let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                        agent.agent_id().to_owned()
                    };

                    let backend = factory::JanusBackend::new(backend_id, 1, 2)
                        .capacity(1)
                        .insert(&conn);

                    // Insert room with a connected agent.
                    let room1 = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_connected_agent(&conn, agent1.agent_id(), room1.id());

                    // Insert a room to signal within and put a ready agent there.
                    let room2 = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_connected_agent(&conn, agent2.agent_id(), room2.id());
                    room2
                };

                // Make `signal.create` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_RECVONLY }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent2, payload)
                    .await
                    .expect_err("Signal created while expected capacity exceeded error");

                assert_eq!(err.status(), ResponseStatus::SERVICE_UNAVAILABLE);
                assert_eq!(err.kind(), "capacity_exceeded");
            });
        }

        #[test]
        fn create_signal_exceeded_capacity_for_writer() {
            async_std::task::block_on(async {
                let agent1 = TestAgent::new("web", "reader", USR_AUDIENCE);
                let agent2 = TestAgent::new("web", "writer", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Insert backend.
                    let backend_id = {
                        let agent = TestAgent::new("alpha", "janus", SVC_AUDIENCE);
                        agent.agent_id().to_owned()
                    };

                    let backend = factory::JanusBackend::new(backend_id, 1, 2)
                        .capacity(1)
                        .insert(&conn);

                    // Insert room with a connected agent.
                    let room1 = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_agent(&conn, agent1.agent_id(), room1.id());

                    // Insert a room to signal within and put a ready agent there.
                    let room2 = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_agent(&conn, agent2.agent_id(), room2.id());
                    room2
                };

                // Make `signal.create` request.
                // It should work for a writer even when the capacity has exceeded.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_SENDRECV }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let messages = handle_request::<CreateHandler>(&mut context, &agent2, payload)
                    .await
                    .expect("Signal creation failed");

                // Assert outgoing request to Janus.
                let (_payload, _reqp, topic) =
                    find_request::<JanusAgentHandleCreateRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    room.backend_id().unwrap(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, expected_topic);
            });
        }

        #[test]
        fn create_signal_no_backends() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                // Insert  a room with a ready agent there.
                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let now = Utc::now().trunc_subsecs(0);

                    let room = factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((
                            Bound::Included(now),
                            Bound::Excluded(now + Duration::hours(1)),
                        ))
                        .backend(RoomBackend::Janus)
                        .insert(&conn);

                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    room
                };

                // Make `signal.create` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_RECVONLY }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Signal created while expected no available backends error");

                assert_eq!(err.status(), ResponseStatus::SERVICE_UNAVAILABLE);
                assert_eq!(err.kind(), "no_available_backends");
            });
        }

        #[test]
        fn create_signal_with_invalid_jsep() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                // Insert backend, room and enter agent there.
                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room(&conn);
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    room
                };

                // Make `signal.create` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "answer", "sdp": SDP_ANSWER }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Signal created while expected invalid SDP error");

                assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
                assert_eq!(err.kind(), "invalid_sdp_type");
            });
        }

        #[test]
        fn create_signal_while_not_entered_the_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                // Insert backend and room.
                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let backend = shared_helpers::insert_janus_backend(&conn);
                    shared_helpers::insert_room_with_backend_id(&conn, backend.id())
                };

                // Make `signal.create` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_RECVONLY }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Signal created while expected agent not entered error");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "agent_not_entered_the_room");
            });
        }

        #[test]
        fn create_signal_in_closed_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                // Insert backend and a closed room with a ready agent there.
                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_closed_room(&conn);
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    room
                };

                // Make `signal.create` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_RECVONLY }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Signal created while expected closed room error");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_closed");
            });
        }

        #[test]
        fn create_signal_in_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<CreateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_RECVONLY }),
                )
                .expect("Failed to build JSEP");

                let payload = CreateRequest {
                    room_id: Uuid::new_v4(),
                    jsep,
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Signal created while expected room not found error");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }

    mod trickle {
        use serde_derive::Deserialize;
        use serde_json::json;

        use crate::app::API_VERSION;
        use crate::test_helpers::prelude::*;

        use super::super::*;
        use super::ICE_CANDIDATE;

        #[derive(Debug, PartialEq, Deserialize)]
        struct JanusTrickleRequest {
            janus: String,
            session_id: i64,
            handle_id: i64,
            transaction: String,
            candidate: JanusTrickleRequestCandidate,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct JanusTrickleRequestCandidate {
            #[serde(rename = "sdpMid")]
            sdp_m_id: String,
            #[serde(rename = "sdpMLineIndex")]
            sdp_m_line_index: usize,
            candidate: String,
        }

        #[test]
        fn trickle_signal() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                // Insert backend, room and enter agent there.
                let (backend, room) = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_connected_agent(&conn, agent.agent_id(), room.id());
                    (backend, room)
                };

                // Make `signal.trickle` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<TrickleJsep>(
                    json!({ "sdpMid": "0", "sdpMLineIndex": 0, "candidate": ICE_CANDIDATE }),
                )
                .expect("Failed to build JSEP");

                let payload = TrickleRequest {
                    room_id: room.id(),
                    jsep,
                };

                let messages = handle_request::<TrickleHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Signal trickle failed");

                // Assert outgoing request to Janus.
                let (payload, _reqp, topic) =
                    find_request::<JanusTrickleRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    backend.id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(payload.janus, "trickle");
                assert_eq!(payload.session_id, backend.session_id());
                assert_eq!(payload.handle_id, 123);
                assert_eq!(payload.candidate.sdp_m_id, "0");
                assert_eq!(payload.candidate.sdp_m_line_index, 0);
                assert_eq!(payload.candidate.candidate, ICE_CANDIDATE);
            });
        }
    }

    mod update {
        use serde_derive::{Deserialize, Serialize};
        use serde_json::json;
        use svc_agent::mqtt::ResponseStatus;

        use crate::app::API_VERSION;
        use crate::test_helpers::prelude::*;

        use super::super::*;
        use super::{SDP_ANSWER, SDP_OFFER_SENDRECV};

        #[derive(Deserialize)]
        struct JanusSignalUpdateRequest {
            janus: String,
            session_id: i64,
            handle_id: i64,
            body: JanusSignalUpdateRequestBody,
            jsep: Jsep,
        }

        #[derive(Deserialize)]
        struct JanusSignalUpdateRequestBody {
            method: String,
        }

        #[derive(Debug, PartialEq, Deserialize, Serialize)]
        struct Jsep {
            #[serde(rename = "type")]
            kind: String,
            sdp: String,
        }

        #[test]
        fn update_signal() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                // Insert backend, room and enter agent there.
                let (backend, room) = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_connected_agent(&conn, agent.agent_id(), room.id());
                    (backend, room)
                };

                // Make `signal.update` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<UpdateJsep>(
                    json!({ "type": "offer", "sdp": SDP_OFFER_SENDRECV }),
                )
                .expect("Failed to build JSEP");

                let payload = UpdateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let messages = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Signal update failed");

                // Assert outgoing request to Janus.
                let (payload, _reqp, topic) =
                    find_request::<JanusSignalUpdateRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/conference.{}",
                    backend.id(),
                    API_VERSION,
                    SVC_AUDIENCE,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(payload.janus, "message");
                assert_eq!(payload.session_id, backend.session_id());
                assert_eq!(payload.handle_id, 123);
                assert_eq!(payload.body.method, "signal.update");
                assert_eq!(payload.jsep.kind, "offer");
                assert_eq!(payload.jsep.sdp, SDP_OFFER_SENDRECV);
            });
        }

        #[test]
        fn update_signal_with_invalid_jsep() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                // Insert backend, room and enter agent there.
                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    let backend = shared_helpers::insert_janus_backend(&conn);
                    let room = shared_helpers::insert_room_with_backend_id(&conn, backend.id());
                    shared_helpers::insert_connected_agent(&conn, agent.agent_id(), room.id());
                    room
                };

                // Make `signal.update` request.
                let mut context = TestContext::new(db.clone(), TestAuthz::new());

                let jsep = serde_json::from_value::<UpdateJsep>(
                    json!({ "type": "answer", "sdp": SDP_ANSWER }),
                )
                .expect("Failed to build JSEP");

                let payload = UpdateRequest {
                    room_id: room.id(),
                    jsep,
                };

                let err = handle_request::<UpdateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Signal update succeeded while expecting invalid SDP error");

                assert_eq!(err.status(), ResponseStatus::BAD_REQUEST);
                assert_eq!(err.kind(), "invalid_sdp_type");
            });
        }
    }
}
