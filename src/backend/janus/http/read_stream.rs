use serde::Deserialize;

use crate::backend::janus::{
    requests::{MessageRequest, ReadStreamRequestBody},
    responses::EventResponse,
};
use serde_json::Value as JsonValue;

pub type ReadStreamRequest = MessageRequest;
pub type ReadStreamResponse = EventResponse;
