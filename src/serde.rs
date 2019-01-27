use serde::ser::{Serialize, Serializer};
use serde_derive::Serialize;
use chrono::{DateTime, Utc};

#[derive(Serialize)]
#[serde(remote = "http::StatusCode")]
pub(crate) struct HttpStatusCodeRef(#[serde(getter = "http::StatusCode::as_u16")] u16);

pub fn ts_seconds_option<S>(opt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match opt {
        Some(val) => serializer.serialize_i64(val.timestamp()),
        None => serializer.serialize_none(),
    }
}
