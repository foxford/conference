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
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{de, ser};
    use std::fmt;
    use std::ops::Bound;

    pub(crate) fn serialize<S>(
        value: &(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
        serializer: S,
    ) -> Result<S::Ok, S::Error>
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
                let val = lt.timestamp() +1;
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
                let val = rt.timestamp() -1;
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

    pub fn deserialize<'de, D>(
        d: D,
    ) -> Result<(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>), D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_tuple(2, TupleSecondsTimestampVisitor)
    }

    struct TupleSecondsTimestampVisitor;

    impl<'de> de::Visitor<'de> for TupleSecondsTimestampVisitor {
        type Value = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

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
                    let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(val, 0), Utc);
                    Bound::Included(dt)
                }
                Some(None) => Bound::Unbounded,
                None => return Err(de::Error::invalid_length(1, &self)),
            };

            let rt = match seq.next_element()? {
                Some(Some(val)) => {
                    let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(val, 0), Utc);
                    Bound::Excluded(dt)
                }
                Some(None) => Bound::Unbounded,
                None => return Err(de::Error::invalid_length(2, &self)),
            };

            return Ok((lt, rt));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) mod ts_seconds_option_bound_tuple {
    use chrono::{DateTime, Utc};
    use serde::{de, ser};
    use std::fmt;
    use std::ops::Bound;

    pub(crate) fn serialize<S>(
        option: &Option<(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>)>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match option {
            Some(value) => super::ts_seconds_bound_tuple::serialize(value, serializer),
            None => serializer.serialize_none()
        }
    }

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
        let now = Utc::now();
        let now = NaiveDateTime::from_timestamp(now.timestamp(), 0);
        let now = DateTime::from_utc(now, Utc);

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
}
