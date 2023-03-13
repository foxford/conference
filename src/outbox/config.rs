use serde::Deserialize;

#[derive(Copy, Clone, Debug, Deserialize)]
pub struct Config {
    pub messages_per_try: i64,
    #[serde(with = "crate::outbox::config::duration_seconds")]
    pub try_wake_interval: chrono::Duration,
    #[serde(with = "crate::outbox::config::duration_seconds")]
    pub max_delivery_interval: chrono::Duration,
}

pub(crate) mod duration_seconds {
    use std::fmt;

    use chrono::Duration;
    use serde::de;

    pub fn deserialize<'de, D>(d: D) -> Result<Duration, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_u64(SecondsDurationVisitor)
    }

    pub struct SecondsDurationVisitor;

    impl<'de> de::Visitor<'de> for SecondsDurationVisitor {
        type Value = Duration;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("duration (seconds)")
        }

        fn visit_u64<E>(self, seconds: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Duration::seconds(seconds as i64))
        }
    }
}
