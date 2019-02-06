use chrono::{DateTime, Utc};
use serde::ser::Serializer;
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

pub(crate) mod ts_seconds_bound_tuple {
    use std::fmt;
    use std::ops::Bound;

    use serde::{
        de::{self, Error},
        ser::SerializeTuple,
    };

    use super::{DateTime, Serializer, Utc};

    pub(crate) fn serialize<S>(
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

    pub fn deserialize<'de, D>(
        d: D,
    ) -> Result<(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>), D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let (start, end) = d.deserialize_tuple(2, TupleSecondsTimestampVisitor)?;
        Ok((Bound::Included(start), Bound::Included(end)))
    }

    struct TupleSecondsTimestampVisitor;

    impl<'de> de::Visitor<'de> for TupleSecondsTimestampVisitor {
        type Value = (DateTime<Utc>, DateTime<Utc>);

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter
                .write_str("a tuple (or list in some formats) with 2 unix timestamps in seconds")
        }

        /// Deserialize a tuple of two Bounded DateTime<Utc>
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let start = seq.next_element()?;
            let end = seq.next_element()?;

            match (start, end) {
                (Some(start), Some(end)) => Ok((start, end)),
                _ => Err(A::Error::custom("failed to deserialize tuple of Bounds")),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::Bound;

    use chrono::{DateTime, Utc};
    use serde_derive::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestData {
        #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
        time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    }

    #[test]
    fn ser_de() {
        let now = Utc::now();
        let val = json!({
            "time": (now, now),
        });

        let data: TestData = serde_json::from_value(val).unwrap();

        println!("{:?}", data);

        let (start, end) = data.time;

        assert_eq!(start, Bound::Included(now));
        assert_eq!(end, Bound::Included(now));

        let data = serde_json::to_value(data).unwrap();

        let arr = data
            .get("time")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap());
        let now = now.timestamp();

        for val in arr {
            assert_eq!(val, now);
        }
    }
}
