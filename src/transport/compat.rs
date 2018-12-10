use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Envelope {
    pub(crate) payload: String,
}

impl Envelope {
    pub(crate) fn new(payload: &str) -> Self {
        Self {
            payload: payload.to_owned(),
        }
    }
}
