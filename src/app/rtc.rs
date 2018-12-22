use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Queryable)]
pub(crate) struct Rtc {
}

impl Rtc {
    pub(crate) fn create(&self, _params: CreateParameters) {}
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CreateParameters {
    room_id: Uuid,
}
