use crate::app::janus;
use crate::authn::Authenticable;
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
    db: ConnectionPool,
}

impl State {
    pub(crate) fn new(db: ConnectionPool) -> Self {
        Self { db }
    }
}

impl State {
    pub(crate) fn create(&self, inreq: &CreateRequest) -> Result<impl Publishable, Error> {
        let rtc_id = &inreq.payload().rtc_id;
        let jsep = &inreq.payload().jsep;
        let sdp_type = parse_sdp_type(jsep)?;

        let conn = self.db.get()?;
        let object =
            location::FindQuery::new(&inreq.properties().agent_id(), rtc_id).execute(&conn)?;

        match sdp_type {
            SdpType::Offer => {
                if is_sdp_recvonly(jsep)? {
                    let backreq = janus::read_stream_request(
                        inreq.properties().clone(),
                        object.session_id(),
                        object.handle_id(),
                        rtc_id.clone(),
                        jsep.clone(),
                        object.location_id().clone(),
                    )?;
                    backreq.into_envelope()
                } else {
                    let label = inreq
                        .payload()
                        .label
                        .as_ref()
                        .ok_or_else(|| err_msg("missing label"))?;
                    let state = rtc::RtcState::new(label, inreq.properties().agent_id(), None);
                    let _ = rtc::UpdateQuery::new(rtc_id).state(&state).execute(&conn)?;

                    let backreq = janus::create_stream_request(
                        inreq.properties().clone(),
                        object.session_id(),
                        object.handle_id(),
                        rtc_id.clone(),
                        jsep.clone(),
                        object.location_id().clone(),
                    )?;
                    backreq.into_envelope()
                }
            }
            SdpType::Answer => Err(err_msg("sdp_type = answer is not allowed")),
            SdpType::IceCandidate => {
                let backreq = janus::trickle_request(
                    inreq.properties().clone(),
                    object.session_id(),
                    object.handle_id(),
                    jsep.clone(),
                    object.location_id().clone(),
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
    let sdp_type = jsep.get("type");
    let candidate = jsep.get("candidate");
    match (sdp_type, candidate) {
        (Some(JsonValue::String(ref val)), None) if val == "offer" => Ok(SdpType::Offer),
        (Some(JsonValue::String(ref val)), None) if val == "answer" => Ok(SdpType::Answer),
        (None, Some(JsonValue::String(_))) => Ok(SdpType::IceCandidate),
        (None, Some(JsonValue::Object(_))) => Ok(SdpType::IceCandidate), // {"completed": true}
        (None, Some(JsonValue::Null)) => Ok(SdpType::IceCandidate),      // null
        _ => Err(format_err!("invalid jsep = {}", jsep)),
    }
}

fn is_sdp_recvonly(jsep: &JsonValue) -> Result<bool, Error> {
    use webrtc_sdp::{attribute_type::SdpAttributeType, parse_sdp};

    let sdp = jsep.get("sdp").ok_or_else(|| err_msg("missing sdp"))?;
    let sdp = sdp
        .as_str()
        .ok_or_else(|| format_err!("invalid sdp = {}", sdp))?;
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
