use crate::app::janus;
use crate::authn::Authenticable;
use crate::db::{janus_handle_shadow, ConnectionPool};
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
        let record =
            janus_handle_shadow::FindLocationQuery::new(&inreq.properties().agent_id(), rtc_id)
                .execute(&conn)?;

        match sdp_type {
            SdpType::Offer => {
                let rtc_id = &inreq.payload().rtc_id;
                let backreq = janus::create_stream_request(
                    inreq.properties().clone(),
                    record.session_id(),
                    record.handle_id(),
                    rtc_id.clone(),
                    jsep.clone(),
                    record.location_id().clone(),
                )?;
                backreq.into_envelope()
            }
            SdpType::Answer => Err(err_msg("sdp_type = answer is not currently supported")),
            SdpType::IceCandidate => {
                let backreq = janus::trickle_request(
                    inreq.properties().clone(),
                    record.session_id(),
                    record.handle_id(),
                    jsep.clone(),
                    record.location_id().clone(),
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
        _ => Err(format_err!("invalid jsep = {}", jsep)),
    }
}
