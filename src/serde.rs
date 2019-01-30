use std::ops::Bound;

use chrono::{DateTime, Utc};
use serde::ser::{SerializeTuple, Serializer};
use serde_derive::Serialize;

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

pub(crate) fn ts_seconds_bound_tuple<S>(
    range: &(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let (start, end) = range;
    let mut tup = serializer.serialize_tuple(2)?;

    match start {
        Bound::Included(start) | Bound::Excluded(start) => {
            tup.serialize_element(&start.timestamp())?;
        }

        b @ Bound::Unbounded => {
            tup.serialize_element(b)?;
        }
    }

    match end {
        Bound::Included(end) | Bound::Excluded(end) => {
            tup.serialize_element(&end.timestamp())?;
        }
        b @ Bound::Unbounded => {
            tup.serialize_element(b)?;
        }
    }

    tup.end()
}
