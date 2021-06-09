use serde::Deserialize;

use crate::backend::janus::{
    requests::{CreateStreamRequestBody, MessageRequest},
    responses::EventResponse,
};
use serde_json::Value as JsonValue;

pub type CreateStreamRequest = MessageRequest;

pub type CreateStreamResponse = EventResponse;
