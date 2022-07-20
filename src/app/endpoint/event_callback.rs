use std::sync::Arc;

use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use svc_agent::{mqtt::ResponseStatus, Authenticable};
use svc_utils::extractors::AuthnExtractor;

use crate::app::{
    context::{AppContext, GlobalContext, MessageContext},
    error,
    service_utils::Response,
};

use super::{prelude::ErrorExt, rtc_signal::CreateResponseData, RequestResult};

#[derive(Debug, Serialize, Deserialize)]
pub struct CallbackRequest {
    response: CreateResponseData,
    id: usize,
}

impl CallbackRequest {
    pub fn new(response: CreateResponseData, id: usize) -> Self {
        Self { response, id }
    }
}

pub async fn stream(
    Extension(ctx): Extension<Arc<AppContext>>,
    AuthnExtractor(agent_id): AuthnExtractor,
    Json(payload): Json<CallbackRequest>,
) -> RequestResult {
    let context = ctx.start_message();

    match agent_id.as_account_id().label() {
        "conference" => {}
        label => {
            return Err(error::Error::new(
                error::ErrorKind::AccessDenied,
                anyhow::anyhow!("Access denied for an agent with label {}", label),
            ));
        }
    }

    ctx.janus_clients()
        .stream_waitlist()
        .fire(payload.id, Ok(payload.response))
        .error(error::ErrorKind::JanusResponseTimeout)?;

    Ok(Response::new(
        ResponseStatus::OK,
        serde_json::json!({}),
        context.start_timestamp(),
        None,
    ))
}
