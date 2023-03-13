use std::ops::Bound;

use chrono::{DateTime, Utc};

pub type Time = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

////////////////////////////////////////////////////////////////////////////////

pub mod ts_seconds_option {
    use chrono::{DateTime, Utc};
    use serde::{de, ser};
    use std::fmt;

    pub fn serialize<S>(opt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match opt {
            Some(dt) => chrono::serde::ts_seconds::serialize(dt, serializer),
            None => serializer.serialize_none(),
        }
    }

    #[cfg(test)]
    pub fn deserialize<'de, D>(d: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_option(SecondsTimestampOptionVisitor)
    }

    pub struct SecondsTimestampOptionVisitor;

    impl<'de> de::Visitor<'de> for SecondsTimestampOptionVisitor {
        type Value = Option<DateTime<Utc>>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("none or unix time (seconds)")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            let dt = chrono::serde::ts_seconds::deserialize(d)?;
            Ok(Some(dt))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub mod ts_seconds_bound_tuple {
    use super::Time;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{de, ser};
    use std::{fmt, ops::Bound};

    pub fn serialize<S>(value: &Time, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        use ser::SerializeTuple;

        let (lt, rt) = value;
        let mut tup = serializer.serialize_tuple(2)?;

        match lt {
            Bound::Included(lt) => {
                let val = lt.timestamp();
                tup.serialize_element(&val)?;
            }
            Bound::Excluded(lt) => {
                // Adjusting the range to '[lt, rt)'
                let val = lt.timestamp() + 1;
                tup.serialize_element(&val)?;
            }
            Bound::Unbounded => {
                let val: Option<i64> = None;
                tup.serialize_element(&val)?;
            }
        }

        match rt {
            Bound::Included(rt) => {
                // Adjusting the range to '[lt, rt)'
                let val = rt.timestamp() - 1;
                tup.serialize_element(&val)?;
            }
            Bound::Excluded(rt) => {
                let val = rt.timestamp();
                tup.serialize_element(&val)?;
            }
            Bound::Unbounded => {
                let val: Option<i64> = None;
                tup.serialize_element(&val)?;
            }
        }

        tup.end()
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Time, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_tuple(2, TupleSecondsTimestampVisitor)
    }

    struct TupleSecondsTimestampVisitor;

    impl<'de> de::Visitor<'de> for TupleSecondsTimestampVisitor {
        type Value = Time;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a [lt, rt) range of unix time (seconds) or null (unbounded)")
        }

        /// Deserialize a tuple of two Bounded DateTime<Utc>
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let lt = match seq.next_element()? {
                Some(Some(val)) => {
                    let dt = DateTime::<Utc>::from_utc(
                        NaiveDateTime::from_timestamp_opt(val, 0).unwrap_or_default(),
                        Utc,
                    );
                    Bound::Included(dt)
                }
                Some(None) => Bound::Unbounded,
                None => return Err(de::Error::invalid_length(1, &self)),
            };

            let rt = match seq.next_element()? {
                Some(Some(val)) => {
                    let dt = DateTime::<Utc>::from_utc(
                        NaiveDateTime::from_timestamp_opt(val, 0).unwrap_or_default(),
                        Utc,
                    );
                    Bound::Excluded(dt)
                }
                Some(None) => Bound::Unbounded,
                None => return Err(de::Error::invalid_length(2, &self)),
            };

            Ok((lt, rt))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub mod ts_seconds_option_bound_tuple {
    use super::Time;
    use serde::{de, ser};
    use std::fmt;

    pub fn serialize<S>(option: &Option<Time>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match option {
            Some(value) => super::ts_seconds_bound_tuple::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<Time>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_option(TupleSecondsTimestampVisitor)
    }

    pub struct TupleSecondsTimestampVisitor;

    impl<'de> de::Visitor<'de> for TupleSecondsTimestampVisitor {
        type Value = Option<Time>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter
                .write_str("none or a [lt, rt) range of unix time (seconds) or null (unbounded)")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
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

    use chrono::{DateTime, Duration, NaiveDateTime, Utc};
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::Time;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestData {
        #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
        time: Time,
    }

    #[derive(Debug, Deserialize)]
    struct TestOptionData {
        #[serde(default)]
        #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
        time: Option<Time>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TestSecondsStartedAtOptionData {
        #[serde(with = "crate::serde::ts_seconds_option")]
        started_at: Option<DateTime<Utc>>,
    }

    #[test]
    fn ts_seconds_bound_tuple() {
        let now = now();

        let val = json!({
            "time": (now.timestamp(), now.timestamp()),
        });

        let data: TestData = dbg!(serde_json::from_value(val).unwrap());

        let (start, end) = data.time;

        assert_eq!(start, Bound::Included(now));
        assert_eq!(end, Bound::Excluded(now));

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
        let now = now();

        let val = json!({
            "time": (now.timestamp(), now.timestamp()),
        });

        let data: TestOptionData = dbg!(serde_json::from_value(val).unwrap());

        let (start, end) = data.time.unwrap();

        assert_eq!(start, Bound::Included(now));
        assert_eq!(end, Bound::Excluded(now));

        let val = json!({});

        let data: TestOptionData = dbg!(serde_json::from_value(val).unwrap());

        assert!(data.time.is_none());
    }

    #[test]
    fn ts_seconds_option() {
        let now = now();
        let data = TestSecondsStartedAtOptionData {
            started_at: Some(now),
        };
        let data = serde_json::to_value(data).unwrap();
        let result = data.get("started_at").unwrap().as_i64().unwrap();
        assert_eq!(result, now.timestamp());

        let val = json!({ "started_at": now.timestamp() });
        let data: TestSecondsStartedAtOptionData = dbg!(serde_json::from_value(val).unwrap());
        assert_eq!(data.started_at, Some(now));
    }

    fn now() -> DateTime<Utc> {
        let now = Utc::now();
        let now = NaiveDateTime::from_timestamp_opt(now.timestamp(), 0).unwrap_or_default();
        DateTime::from_utc(now, Utc)
    }
}
