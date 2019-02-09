use crate::app::janus;
use crate::authn::Authenticable;
use crate::authz;
use crate::db::{location, rtc, ConnectionPool};
use crate::transport::mqtt::compat::IntoEnvelope;
use crate::transport::mqtt::{IncomingRequest, OutgoingResponse, Publishable};
use failure::{err_msg, format_err, Error};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

pub(crate) type CreateRequest = IncomingRequest<CreateRequestData>;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequestData {
    rtc_id: Uuid,
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
    authz: authz::ClientMap,
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(authz: authz::ClientMap, db: ConnectionPool) -> Self {
        Self { authz, db }
    }
}

impl State {
    pub(crate) fn create(&self, inreq: &CreateRequest) -> Result<impl Publishable, Error> {
        let agent_id = inreq.properties().agent_id();
        let rtc_id = &inreq.payload().rtc_id;
        let jsep = &inreq.payload().jsep;
        let sdp_type = parse_sdp_type(jsep)?;

        // Looking up for Janus Gateway Handle
        let loc = {
            let conn = self.db.get()?;
            location::FindQuery::new(&agent_id, rtc_id)
                .execute(&conn)?
                .ok_or_else(|| {
                    format_err!(
                        "the location of the rtc = '{}' for the agent = '{}' is not found",
                        rtc_id,
                        &agent_id,
                    )
                })?
        };

        // Authorization: room's owner has to allow the action
        let authorize = |action| -> Result<(), Error> {
            let room_id = loc.room_id().to_string();
            let rtc_id = loc.rtc_id().to_string();
            self.authz.authorize(
                loc.audience(),
                agent_id.account_id(),
                vec!["rooms", &room_id, "rtcs", &rtc_id],
                action,
            )
        };

        match sdp_type {
            SdpType::Offer => {
                if is_sdp_recvonly(jsep)? {
                    // Authorization
                    authorize("read")?;

                    let backreq = janus::read_stream_request(
                        inreq.properties().clone(),
                        loc.session_id(),
                        loc.handle_id(),
                        rtc_id.clone(),
                        jsep.clone(),
                        loc.location_id().clone(),
                    )?;
                    backreq.into_envelope()
                } else {
                    // Authorization
                    authorize("update")?;

                    // Updating the Real-Time Connection state
                    {
                        let conn = self.db.get()?;
                        let label = inreq
                            .payload()
                            .label
                            .as_ref()
                            .ok_or_else(|| err_msg("missing label"))?;
                        let state =
                            rtc::RtcState::new(label, Some(inreq.properties().agent_id()), None);
                        let _ = rtc::UpdateQuery::new(rtc_id).state(&state).execute(&conn)?;
                    }

                    let backreq = janus::create_stream_request(
                        inreq.properties().clone(),
                        loc.session_id(),
                        loc.handle_id(),
                        rtc_id.clone(),
                        jsep.clone(),
                        loc.location_id().clone(),
                    )?;
                    backreq.into_envelope()
                }
            }
            SdpType::Answer => Err(err_msg("sdp_type = 'answer' is not allowed")),
            SdpType::IceCandidate => {
                // Authorization
                authorize("read")?;

                let backreq = janus::trickle_request(
                    inreq.properties().clone(),
                    loc.session_id(),
                    loc.handle_id(),
                    jsep.clone(),
                    loc.location_id().clone(),
                )?;
                backreq.into_envelope()
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
