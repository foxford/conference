use chrono::{DateTime, Utc};
use serde::ser;

////////////////////////////////////////////////////////////////////////////////

pub fn ts_seconds_option<S>(opt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: ser::Serializer,
{
    match opt {
        Some(val) => serializer.serialize_i64(val.timestamp()),
        None => serializer.serialize_none(),
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) mod ts_seconds_bound_tuple {
    use std::fmt;
    use std::ops::Bound;

    use serde::{
        de::{self, Error},
        ser::SerializeTuple,
    };

    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::ser::Serializer;

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
        let start = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(start, 0), Utc);
        let end = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(end, 0), Utc);
        Ok((Bound::Included(start), Bound::Included(end)))
    }

    struct TupleSecondsTimestampVisitor;

    impl<'de> de::Visitor<'de> for TupleSecondsTimestampVisitor {
        type Value = (i64, i64);

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

pub(crate) mod ts_seconds_option_bound_tuple {
    use std::fmt;
    use std::ops::Bound;

    use serde::de::{self, Error};

    use chrono::{DateTime, NaiveDateTime, Utc};

    pub fn deserialize<'de, D>(
        d: D,
    ) -> Result<Option<(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>)>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_option(TupleSecondsTimestampVisitor)
    }

    pub struct TupleSecondsTimestampVisitor;

    impl<'de> de::Visitor<'de> for TupleSecondsTimestampVisitor {
        type Value = Option<(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>)>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter
                .write_str("a tuple (or list in some formats) with 2 unix timestamps in seconds")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            let interval = super::ts_seconds_bound_tuple::deserialize(d)?;
            Ok(Some(interval))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use std::ops::Bound;

    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde_derive::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestData {
        #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
        time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    }

    #[derive(Debug, Deserialize)]
    struct TestOptionData {
        #[serde(default)]
        #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
        time: Option<(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>)>,
    }

    #[test]
    fn ts_seconds_bound_tuple() {
        let now = Utc::now();
        let now = NaiveDateTime::from_timestamp(now.timestamp(), 0);
        let now = DateTime::from_utc(now, Utc);

        let val = json!({
            "time": (now.timestamp(), now.timestamp()),
        });

        let data: TestData = dbg!(serde_json::from_value(val).unwrap());

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

    #[test]
    fn ts_seconds_option_bound_tuple() {
        let now = Utc::now();
        let now = NaiveDateTime::from_timestamp(now.timestamp(), 0);
        let now = DateTime::from_utc(now, Utc);

        let val = json!({
            "time": (now.timestamp(), now.timestamp()),
        });

        let data: TestOptionData = dbg!(serde_json::from_value(val).unwrap());

        let (start, end) = data.time.unwrap();

        assert_eq!(start, Bound::Included(now));
        assert_eq!(end, Bound::Included(now));

        let val = json!({});

        let data: TestOptionData = dbg!(serde_json::from_value(val).unwrap());

        assert!(data.time.is_none());
    }
}
