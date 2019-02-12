use chrono::{DateTime, Utc};
use serde::{de, ser};
use serde_derive::Serialize;
use std::fmt;

use crate::transport::{mqtt::AuthnProperties, AccountId, Addressable, AgentId, Authenticable};

////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize)]
#[serde(remote = "http::StatusCode")]
pub(crate) struct HttpStatusCodeRef(#[serde(getter = "http::StatusCode::as_u16")] u16);

////////////////////////////////////////////////////////////////////////////////

impl ser::Serialize for AuthnProperties {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        use ser::SerializeStruct;

        let mut state = serializer.serialize_struct("AuthnProperties", 3)?;
        state.serialize_field("agent_label", self.agent_id().label())?;
        state.serialize_field("account_label", self.account_id().label())?;
        state.serialize_field("audience", self.account_id().audience())?;
        state.end()
    }
}

impl<'de> de::Deserialize<'de> for AuthnProperties {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        enum Field {
            AgentLabel,
            AccountLabel,
            Audience,
        };

        impl<'de> de::Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: de::Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> de::Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`agent_label` or `account_label` or `audience`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "agent_label" => Ok(Field::AgentLabel),
                            "account_label" => Ok(Field::AccountLabel),
                            "audience" => Ok(Field::Audience),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct AuthnPropertiesVisitor;

        impl<'de> de::Visitor<'de> for AuthnPropertiesVisitor {
            type Value = AuthnProperties;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct AuthnProperties")
            }

            fn visit_map<V>(self, mut map: V) -> Result<AuthnProperties, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut agent_label = None;
                let mut account_label = None;
                let mut audience = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::AgentLabel => {
                            if agent_label.is_some() {
                                return Err(de::Error::duplicate_field("agent_label"));
                            }
                            agent_label = Some(map.next_value()?);
                        }
                        Field::AccountLabel => {
                            if account_label.is_some() {
                                return Err(de::Error::duplicate_field("account_label"));
                            }
                            account_label = Some(map.next_value()?);
                        }
                        Field::Audience => {
                            if audience.is_some() {
                                return Err(de::Error::duplicate_field("audience"));
                            }
                            audience = Some(map.next_value()?);
                        }
                    }
                }
                let agent_label =
                    agent_label.ok_or_else(|| de::Error::missing_field("agent_label"))?;
                let account_label =
                    account_label.ok_or_else(|| de::Error::missing_field("account_label"))?;
                let audience = audience.ok_or_else(|| de::Error::missing_field("audience"))?;

                let account_id = AccountId::new(account_label, audience);
                let agent_id = AgentId::new(agent_label, account_id);
                Ok(AuthnProperties::from(agent_id))
            }
        }

        const FIELDS: &'static [&'static str] = &["agent_label", "account_label", "audience"];
        deserializer.deserialize_struct("AuthnProperties", FIELDS, AuthnPropertiesVisitor)
    }
}

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

    #[test]
    fn ser_de() {
        let now = Utc::now();
        let now = NaiveDateTime::from_timestamp(now.timestamp(), 0);
        let now = DateTime::from_utc(now, Utc);

        let val = json!({
            "time": (now.timestamp(), now.timestamp()),
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
