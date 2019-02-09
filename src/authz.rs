use crate::authn::jose::token::TokenBuilder;
use crate::authn::Authenticable;
use failure::{format_err, Error};
use jsonwebtoken::Algorithm;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait Authorize: Sync + Send {
    fn authorize(&self, intent: &Intent) -> Result<(), Error>;
    fn box_clone(&self) -> Box<dyn Authorize>;
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) type ConfigMap = HashMap<String, Config>;

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub(crate) enum Config {
    Trusted(TrustedConfig),
    Http(HttpConfig),
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) type Client = Box<dyn Authorize>;

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Client")
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ClientMap {
    object_ns: String,
    inner: HashMap<String, Client>,
}

impl ClientMap {
    pub(crate) fn from_config<A>(me: &A, m: ConfigMap) -> Result<Self, Error>
    where
        A: Authenticable,
    {
        let mut inner: HashMap<String, Client> = HashMap::new();
        for (audience, config) in m {
            match config {
                Config::Trusted(config) => {
                    let client = config.into_client(me, &audience)?;
                    inner.insert(audience, client);
                }
                Config::Http(config) => {
                    let client = config.into_client(me, &audience)?;
                    inner.insert(audience, client);
                }
            }
        }

        Ok(Self {
            object_ns: me.audience().to_owned(),
            inner,
        })
    }

    pub(crate) fn authorize<A>(
        &self,
        audience: &str,
        subject: &A,
        object: Vec<&str>,
        action: &str,
    ) -> Result<(), Error>
    where
        A: Authenticable,
    {
        let client = self
            .inner
            .get(audience)
            .ok_or_else(|| format_err!("no authz configuration for the audience = {}", audience))?;

        let intent = Intent::new(
            Entity::new(
                subject.audience(),
                vec!["accounts", subject.account_label()],
            ),
            Entity::new(&self.object_ns, object),
            action,
        );

        client.authorize(&intent)
    }
}

////////////////////////////////////////////////////////////////////////////////

trait IntoClient {
    fn into_client<A>(self, me: &A, audience: &str) -> Result<Client, Error>
    where
        A: Authenticable;
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct Entity<'a> {
    namespace: &'a str,
    value: Vec<&'a str>,
}

impl<'a> Entity<'a> {
    pub(crate) fn new(namespace: &'a str, value: Vec<&'a str>) -> Self {
        Self { namespace, value }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct Intent<'a> {
    subject: Entity<'a>,
    object: Entity<'a>,
    action: &'a str,
}

impl<'a> Intent<'a> {
    pub(crate) fn new(subject: Entity<'a>, object: Entity<'a>, action: &'a str) -> Self {
        Self {
            subject,
            object,
            action,
        }
    }

    pub(crate) fn action(&self) -> &str {
        self.action
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TrustedConfig {}

impl Authorize for TrustedConfig {
    fn authorize(&self, _intent: &Intent) -> Result<(), Error> {
        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Authorize> {
        Box::new(self.clone())
    }
}

impl IntoClient for TrustedConfig {
    fn into_client<A>(self, _me: &A, _audience: &str) -> Result<Client, Error>
    where
        A: Authenticable,
    {
        Ok(Box::new(self))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct HttpConfig {
    uri: String,
    #[serde(deserialize_with = "crate::serde::algorithm")]
    algorithm: Algorithm,
    #[serde(deserialize_with = "crate::serde::file")]
    key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct HttpClient {
    uri: String,
    token: String,
}

impl Authorize for HttpClient {
    fn authorize(&self, intent: &Intent) -> Result<(), Error> {
        use reqwest;

        let client = reqwest::Client::new();
        let resp: Vec<String> = client
            .post(&self.uri)
            .bearer_auth(&self.token)
            .json(intent)
            .send()
            .map_err(|err| format_err!("error sending the authorization request, {}", &err))?
            .json()
            .map_err(|_| {
                format_err!(
                    "invalid format of the authorization response, intent = '{}'",
                    serde_json::to_string(&intent).unwrap_or_else(|_| format!("{:?}", &intent)),
                )
            })?;

        if !resp.contains(&intent.action().to_owned()) {
            return Err(format_err!("action = {} is not allowed", &intent.action()));
        }

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Authorize> {
        Box::new(self.clone())
    }
}

impl IntoClient for HttpConfig {
    fn into_client<A>(self, me: &A, audience: &str) -> Result<Client, Error>
    where
        A: Authenticable,
    {
        let token = TokenBuilder::new()
            .issuer(me.audience())
            .subject(me.account_label())
            .audience(&format!("{}:{}", me.audience(), audience))
            .key(&self.algorithm, &self.key)
            .build()
            .map_err(|err| {
                format_err!(
                    "error converting authz config for audience = '{}' into client – {}",
                    audience,
                    &err,
                )
            })?;

        Ok(Box::new(HttpClient {
            uri: self.uri,
            token,
        }))
    }
}
