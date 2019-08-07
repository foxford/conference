use failure::{err_msg, format_err, Error};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fmt;
use std::str::FromStr;
use svc_agent::mqtt::compat::IntoEnvelope;
use svc_agent::mqtt::{IncomingRequest, OutgoingResponse, Publish, ResponseStatus};
use svc_agent::{Addressable, AgentId};
use svc_error::Error as SvcError;
use uuid::Uuid;

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
    pub(crate) async fn create(&self, inreq: CreateRequest) -> Result<impl Publish, SvcError> {
        let handle_id = &inreq.payload().handle_id;
        let jsep = &inreq.payload().jsep;
        let sdp_type = parse_sdp_type(jsep).map_err(|e| {
            SvcError::builder()
                .status(ResponseStatus::BAD_REQUEST)
                .detail(&format!("invalid jsep format, {}", &e))
                .build()
        })?;

        // Authorization: room's owner has to allow the action
        let authorize = |action| -> Result<(), SvcError> {
            let rtc_id = handle_id.rtc_id();
            let room = {
                let conn = self.db.get()?;
                room::FindQuery::new()
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
                .map_err(Into::into)
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
                    authorize("read")?;

                    let backreq = crate::app::janus::read_stream_request(
                        inreq.properties().clone(),
                        handle_id.janus_session_id(),
                        handle_id.janus_handle_id(),
                        handle_id.rtc_id(),
                        jsep.clone(),
                        handle_id.backend_id(),
                    )
                    .map_err(|_| {
                        SvcError::builder()
                            .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                            .detail("error creating a backend request")
                            .build()
                    })?;
                    backreq.into_envelope().map_err(Into::into)
                } else {
                    // Authorization
                    authorize("update")?;

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

                    let backreq = crate::app::janus::create_stream_request(
                        inreq.properties().clone(),
                        handle_id.janus_session_id(),
                        handle_id.janus_handle_id(),
                        handle_id.rtc_id(),
                        jsep.clone(),
                        handle_id.backend_id(),
                    )
                    .map_err(|_| {
                        SvcError::builder()
                            .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                            .detail("error creating a backend request")
                            .build()
                    })?;
                    backreq.into_envelope().map_err(Into::into)
                }
            }
            SdpType::Answer => Err(SvcError::builder()
                .status(ResponseStatus::BAD_REQUEST)
                .detail("sdp_type = 'answer' is not allowed")
                .build()),
            SdpType::IceCandidate => {
                // Authorization
                authorize("read")?;

                let backreq = crate::app::janus::trickle_request(
                    inreq.properties().clone(),
                    handle_id.janus_session_id(),
                    handle_id.janus_handle_id(),
                    jsep.clone(),
                    handle_id.backend_id(),
                )
                .map_err(|_| {
                    SvcError::builder()
                        .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                        .detail("error creating a backend request")
                        .build()
                })?;
                backreq.into_envelope().map_err(Into::into)
            }
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
