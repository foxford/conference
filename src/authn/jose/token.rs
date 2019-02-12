use chrono::{DateTime, Duration, Utc};
use failure::{err_msg, format_err, Error};
use jsonwebtoken::{encode, Algorithm, Header};
use serde_derive::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Claims {
    iss: String,
    aud: String,
    sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    exp: Option<u64>,
}

impl Claims {
    pub(crate) fn issuer(&self) -> &str {
        &self.iss
    }

    pub(crate) fn audience(&self) -> &str {
        &self.aud
    }

    pub(crate) fn subject(&self) -> &str {
        &self.sub
    }

    pub(crate) fn expiration_time(&self) -> Option<u64> {
        self.exp
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct TokenBuilder<'a> {
    issuer: Option<&'a str>,
    audience: Option<&'a str>,
    subject: Option<&'a str>,
    expiration_time: Option<DateTime<Utc>>,
    algorithm: Option<&'a Algorithm>,
    key: Option<&'a [u8]>,
}

impl<'a> TokenBuilder<'a> {
    pub(crate) fn new() -> Self {
        Self {
            issuer: None,
            audience: None,
            subject: None,
            expiration_time: None,
            algorithm: None,
            key: None,
        }
    }

    pub(crate) fn issuer(self, issuer: &'a str) -> Self {
        Self {
            issuer: Some(issuer),
            audience: self.audience,
            subject: self.subject,
            expiration_time: self.expiration_time,
            algorithm: self.algorithm,
            key: self.key,
        }
    }

    pub(crate) fn audience(self, audience: &'a str) -> Self {
        Self {
            issuer: self.issuer,
            audience: Some(audience),
            subject: self.subject,
            expiration_time: self.expiration_time,
            algorithm: self.algorithm,
            key: self.key,
        }
    }

    pub(crate) fn subject(self, subject: &'a str) -> Self {
        Self {
            issuer: self.issuer,
            audience: self.audience,
            subject: Some(subject),
            expiration_time: self.expiration_time,
            algorithm: self.algorithm,
            key: self.key,
        }
    }

    pub(crate) fn expires_in(self, expires_in: i64) -> Self {
        let expiration_time = Utc::now() + Duration::seconds(expires_in);
        Self {
            issuer: self.issuer,
            audience: self.audience,
            subject: self.subject,
            expiration_time: Some(expiration_time),
            algorithm: self.algorithm,
            key: self.key,
        }
    }

    pub(crate) fn key(self, algorithm: &'a Algorithm, key: &'a [u8]) -> Self {
        Self {
            issuer: self.issuer,
            audience: self.audience,
            subject: self.subject,
            expiration_time: self.expiration_time,
            algorithm: Some(algorithm),
            key: Some(key),
        }
    }

    pub(crate) fn build(self) -> Result<String, Error> {
        let issuer = self.issuer.ok_or_else(|| err_msg("invalid issuer"))?;
        let audience = self.audience.ok_or_else(|| err_msg("missing audience"))?;
        let subject = self.subject.ok_or_else(|| err_msg("missing subject"))?;
        let algorithm = self.algorithm.ok_or_else(|| err_msg("missing algorithm"))?;
        let key = self.key.ok_or_else(|| err_msg("missing key"))?;

        let claims = Claims {
            iss: issuer.to_owned(),
            aud: audience.to_owned(),
            sub: subject.to_owned(),
            exp: self.expiration_time.map(|val| val.timestamp() as u64),
        };

        let mut header = Header::default();
        header.alg = algorithm.clone();

        encode(&header, &claims, key).map_err(|err| format_err!("encoding error – {}", err))
    }
}
