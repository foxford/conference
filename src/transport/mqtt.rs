use crate::transport::{
    AccountId, Addressable, AgentId, Authenticable, Destination, SharedGroup, Source,
};
use failure::{err_msg, format_err, Error};
use serde_derive::{Deserialize, Serialize};
use std::fmt;

////////////////////////////////////////////////////////////////////////////////

pub(crate) use rumqtt::QoS;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) enum ConnectionMode {
    Agent,
    Bridge,
}

impl fmt::Display for ConnectionMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ConnectionMode::Agent => "agents",
                ConnectionMode::Bridge => "bridge-agents",
            }
        )
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct AgentConfig {
    pub(crate) uri: String,
}

#[derive(Debug)]
pub(crate) struct AgentBuilder {
    agent_id: AgentId,
    version: String,
    mode: ConnectionMode,
}

impl AgentBuilder {
    pub(crate) fn new(agent_id: AgentId) -> Self {
        Self {
            agent_id,
            version: String::from("v1.mqtt3"),
            mode: ConnectionMode::Agent,
        }
    }

    pub(crate) fn version(self, version: &str) -> Self {
        Self {
            agent_id: self.agent_id,
            version: version.to_owned(),
            mode: self.mode,
        }
    }

    pub(crate) fn mode(self, mode: ConnectionMode) -> Self {
        Self {
            agent_id: self.agent_id,
            version: self.version,
            mode,
        }
    }

    pub(crate) fn start(
        self,
        config: &AgentConfig,
    ) -> Result<(Agent, rumqtt::Receiver<rumqtt::Notification>), Error> {
        let options = Self::mqtt_options(&self.mqtt_client_id(), &config)?;
        let (tx, rx) = rumqtt::MqttClient::start(options)?;

        let agent = Agent::new(self.agent_id, tx);
        Ok((agent, rx))
    }

    fn mqtt_client_id(&self) -> String {
        format!(
            "{version}/{mode}/{agent_id}",
            version = self.version,
            mode = self.mode,
            agent_id = self.agent_id,
        )
    }

    fn mqtt_options(client_id: &str, config: &AgentConfig) -> Result<rumqtt::MqttOptions, Error> {
        let uri = config.uri.parse::<http::Uri>()?;
        let host = uri.host().ok_or_else(|| err_msg("missing MQTT host"))?;
        let port = uri
            .port_part()
            .ok_or_else(|| err_msg("missing MQTT port"))?;

        Ok(rumqtt::MqttOptions::new(client_id, host, port.as_u16())
            .set_keep_alive(30)
            .set_reconnect_opts(rumqtt::ReconnectOptions::AfterFirstSuccess(5)))
    }
}

pub(crate) struct Agent {
    id: AgentId,
    tx: rumqtt::MqttClient,
}

impl Agent {
    fn new(id: AgentId, tx: rumqtt::MqttClient) -> Self {
        Self { id, tx }
    }

    pub(crate) fn id(&self) -> &AgentId {
        &self.id
    }

    pub(crate) fn publish<M>(&mut self, message: &M) -> Result<(), Error>
    where
        M: Publishable,
    {
        let topic = message.destination_topic(&self.id)?;
        let bytes = message.to_bytes()?;

        self.tx
            .publish(topic, QoS::AtLeastOnce, false, bytes)
            .map_err(|_| err_msg("Error publishing an MQTT message"))
    }

    pub(crate) fn subscribe<S>(
        &mut self,
        subscription: &S,
        qos: QoS,
        maybe_group: Option<&SharedGroup>,
    ) -> Result<(), Error>
    where
        S: SubscriptionTopic,
    {
        let mut topic = subscription.subscription_topic(&self.id)?;
        if let Some(ref group) = maybe_group {
            topic = format!("$share/{group}/{topic}", group = group, topic = topic);
        };

        self.tx.subscribe(topic, qos)?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct AuthnProperties {
    agent_label: String,
    account_label: String,
    audience: String,
}

impl Authenticable for AuthnProperties {
    fn account_label(&self) -> &str {
        &self.account_label
    }

    fn audience(&self) -> &str {
        &self.audience
    }
}

impl Addressable for AuthnProperties {
    fn agent_label(&self) -> &str {
        &self.agent_label
    }
}

impl From<&AuthnProperties> for AgentId {
    fn from(value: &AuthnProperties) -> Self {
        AgentId::new(
            value.agent_label(),
            AccountId::new(value.account_label(), value.audience()),
        )
    }
}

impl From<&AuthnProperties> for AccountId {
    fn from(value: &AuthnProperties) -> Self {
        AccountId::new(value.account_label(), value.audience())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct IncomingEventProperties {
    #[serde(flatten)]
    authn: AuthnProperties,
}

impl Authenticable for IncomingEventProperties {
    fn account_label(&self) -> &str {
        self.authn.account_label()
    }

    fn audience(&self) -> &str {
        self.authn.audience()
    }
}

impl Addressable for IncomingEventProperties {
    fn agent_label(&self) -> &str {
        self.authn.agent_label()
    }
}

impl From<&IncomingEventProperties> for AgentId {
    fn from(value: &IncomingEventProperties) -> Self {
        (&value.authn).into()
    }
}

impl From<&IncomingEventProperties> for AccountId {
    fn from(value: &IncomingEventProperties) -> Self {
        (&value.authn).into()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct IncomingRequestProperties {
    method: String,
    correlation_data: String,
    response_topic: String,
    #[serde(flatten)]
    authn: AuthnProperties,
}

impl IncomingRequestProperties {
    pub(crate) fn method(&self) -> &str {
        &self.method
    }

    pub(crate) fn to_response(
        &self,
        status: &'static OutgoingResponseStatus,
    ) -> OutgoingResponseProperties {
        OutgoingResponseProperties::new(status, &self.correlation_data, Some(&self.response_topic))
    }
}

impl Authenticable for IncomingRequestProperties {
    fn account_label(&self) -> &str {
        self.authn.account_label()
    }

    fn audience(&self) -> &str {
        self.authn.audience()
    }
}

impl Addressable for IncomingRequestProperties {
    fn agent_label(&self) -> &str {
        self.authn.agent_label()
    }
}

impl From<&IncomingRequestProperties> for AgentId {
    fn from(value: &IncomingRequestProperties) -> Self {
        (&value.authn).into()
    }
}

impl From<&IncomingRequestProperties> for AccountId {
    fn from(value: &IncomingRequestProperties) -> Self {
        (&value.authn).into()
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct IncomingResponseProperties {
    correlation_data: String,
    #[serde(flatten)]
    authn: AuthnProperties,
}

impl Authenticable for IncomingResponseProperties {
    fn account_label(&self) -> &str {
        self.authn.account_label()
    }

    fn audience(&self) -> &str {
        self.authn.audience()
    }
}

impl Addressable for IncomingResponseProperties {
    fn agent_label(&self) -> &str {
        self.authn.agent_label()
    }
}

impl From<IncomingResponseProperties> for AgentId {
    fn from(value: IncomingResponseProperties) -> Self {
        (&value.authn).into()
    }
}

impl From<IncomingResponseProperties> for AccountId {
    fn from(value: IncomingResponseProperties) -> Self {
        (&value.authn).into()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct IncomingMessage<T, P>
where
    P: Addressable,
{
    payload: T,
    properties: P,
}

impl<T, P> IncomingMessage<T, P>
where
    P: Addressable,
{
    pub(crate) fn new(payload: T, properties: P) -> Self {
        Self {
            payload,
            properties,
        }
    }

    pub(crate) fn payload(&self) -> &T {
        &self.payload
    }

    pub(crate) fn properties(&self) -> &P {
        &self.properties
    }
}

impl<T> IncomingRequest<T> {
    pub(crate) fn to_response<R>(
        &self,
        data: R,
        status: &'static OutgoingResponseStatus,
    ) -> OutgoingResponse<R>
    where
        R: serde::Serialize,
    {
        OutgoingMessage::new(
            data,
            self.properties.to_response(status),
            Destination::Unicast(self.properties().into()),
        )
    }
}

pub(crate) type IncomingEvent<T> = IncomingMessage<T, IncomingEventProperties>;
pub(crate) type IncomingRequest<T> = IncomingMessage<T, IncomingRequestProperties>;
pub(crate) type IncomingResponse<T> = IncomingMessage<T, IncomingResponseProperties>;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct OutgoingEventProperties {
    label: &'static str,
}

impl OutgoingEventProperties {
    pub(crate) fn new(label: &'static str) -> Self {
        Self { label }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct OutgoingRequestProperties {
    method: &'static str,
}

impl OutgoingRequestProperties {
    pub(crate) fn new(method: &'static str) -> Self {
        Self { method }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct OutgoingResponseProperties {
    #[serde(with = "crate::serde::HttpStatusCodeRef")]
    status: &'static OutgoingResponseStatus,
    correlation_data: String,
    #[serde(skip)]
    response_topic: Option<String>,
}

impl OutgoingResponseProperties {
    pub(crate) fn new(
        status: &'static OutgoingResponseStatus,
        correlation_data: &str,
        response_topic: Option<&str>,
    ) -> Self {
        Self {
            status,
            correlation_data: correlation_data.to_owned(),
            response_topic: response_topic.map(|val| val.to_owned()),
        }
    }
}

pub(crate) type OutgoingResponseStatus = http::StatusCode;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct OutgoingMessage<T, P>
where
    T: serde::Serialize,
{
    payload: T,
    properties: P,
    destination: Destination,
}

impl<T, P> OutgoingMessage<T, P>
where
    T: serde::Serialize,
{
    pub(crate) fn new(payload: T, properties: P, destination: Destination) -> Self {
        Self {
            payload,
            properties,
            destination,
        }
    }
}

pub(crate) type OutgoingEvent<T> = OutgoingMessage<T, OutgoingEventProperties>;
pub(crate) type OutgoingRequest<T> = OutgoingMessage<T, OutgoingRequestProperties>;
pub(crate) type OutgoingResponse<T> = OutgoingMessage<T, OutgoingResponseProperties>;

impl<T> compat::IntoEnvelope for OutgoingEvent<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<compat::OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)?;
        let envelope = compat::OutgoingEnvelope::new(
            &payload,
            compat::OutgoingEnvelopeProperties::Event(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

impl<T> compat::IntoEnvelope for OutgoingRequest<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<compat::OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)?;
        let envelope = compat::OutgoingEnvelope::new(
            &payload,
            compat::OutgoingEnvelopeProperties::Request(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

impl<T> compat::IntoEnvelope for OutgoingResponse<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<compat::OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)?;
        let envelope = compat::OutgoingEnvelope::new(
            &payload,
            compat::OutgoingEnvelopeProperties::Response(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait Publishable {
    fn destination_topic(&self, agent_id: &AgentId) -> Result<String, Error>;
    fn to_bytes(&self) -> Result<String, Error>;
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait Publish<'a> {
    fn publish(&'a self, tx: &mut Agent) -> Result<(), Error>;
}

impl<'a, T> Publish<'a> for T
where
    T: Publishable,
{
    fn publish(&'a self, tx: &mut Agent) -> Result<(), Error> {
        tx.publish(self)?;
        Ok(())
    }
}

impl<'a, T1, T2> Publish<'a> for (T1, T2)
where
    T1: Publishable,
    T2: Publishable,
{
    fn publish(&'a self, tx: &mut Agent) -> Result<(), Error> {
        tx.publish(&self.0)?;
        tx.publish(&self.1)?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////

trait DestinationTopic {
    fn destination_topic(&self, agent_id: &AgentId, dest: &Destination) -> Result<String, Error>;
}

impl DestinationTopic for OutgoingEventProperties {
    fn destination_topic(&self, me: &AgentId, dest: &Destination) -> Result<String, Error> {
        match dest {
            Destination::Broadcast(ref uri) => Ok(format!(
                "apps/{app}/api/v1/{uri}",
                app = me.account_id(),
                uri = uri,
            )),
            _ => Err(format_err!(
                "destination = '{:?}' is incompatible with event message type",
                dest,
            )),
        }
    }
}

impl DestinationTopic for OutgoingRequestProperties {
    fn destination_topic(&self, me: &AgentId, dest: &Destination) -> Result<String, Error> {
        match dest {
            Destination::Unicast(ref agent_id) => Ok(format!(
                "agents/{agent_id}/api/v1/in/{app}",
                agent_id = agent_id,
                app = me.account_id(),
            )),
            Destination::Multicast(ref account_id) => Ok(format!(
                "agents/{agent_id}/api/v1/out/{app}",
                agent_id = me,
                app = account_id,
            )),
            _ => Err(format_err!(
                "destination = '{:?}' is incompatible with request message type",
                dest,
            )),
        }
    }
}

impl DestinationTopic for OutgoingResponseProperties {
    fn destination_topic(&self, me: &AgentId, dest: &Destination) -> Result<String, Error> {
        match &self.response_topic {
            Some(ref val) => Ok(val.to_owned()),
            None => match dest {
                Destination::Unicast(ref agent_id) => Ok(format!(
                    "agents/{agent_id}/api/v1/in/{app}",
                    agent_id = agent_id,
                    app = me.account_id(),
                )),
                _ => Err(format_err!(
                    "destination = '{:?}' is incompatible with response message type",
                    dest,
                )),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait SubscriptionTopic {
    fn subscription_topic(&self, agent_id: &AgentId) -> Result<String, Error>;
}

pub(crate) struct EventSubscription<'a> {
    source: Source<'a>,
}

impl<'a> EventSubscription<'a> {
    pub(crate) fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

impl<'a> SubscriptionTopic for EventSubscription<'a> {
    fn subscription_topic(&self, _me: &AgentId) -> Result<String, Error> {
        match self.source {
            Source::Broadcast(ref account_id, ref uri) => Ok(format!(
                "apps/{app}/api/v1/{uri}",
                app = account_id,
                uri = uri,
            )),
            _ => Err(format_err!(
                "source = '{:?}' is incompatible with event subscription",
                self.source,
            )),
        }
    }
}

pub(crate) struct RequestSubscription<'a> {
    source: Source<'a>,
}

impl<'a> RequestSubscription<'a> {
    pub(crate) fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

impl<'a> SubscriptionTopic for RequestSubscription<'a> {
    fn subscription_topic(&self, me: &AgentId) -> Result<String, Error> {
        match self.source {
            Source::Unicast(Some(ref account_id)) => Ok(format!(
                "agents/{agent_id}/api/v1/in/{app}",
                agent_id = me,
                app = account_id,
            )),
            Source::Unicast(None) => Ok(format!("agents/{agent_id}/api/v1/in/+", agent_id = me)),
            Source::Multicast => Ok(format!("agents/+/api/v1/out/{app}", app = me.account_id())),
            _ => Err(format_err!(
                "source = '{:?}' is incompatible with request subscription",
                self.source,
            )),
        }
    }
}

pub(crate) struct ResponseSubscription<'a> {
    source: Source<'a>,
}

impl<'a> ResponseSubscription<'a> {
    pub(crate) fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

impl<'a> SubscriptionTopic for ResponseSubscription<'a> {
    fn subscription_topic(&self, me: &AgentId) -> Result<String, Error> {
        match self.source {
            Source::Unicast(Some(ref account_id)) => Ok(format!(
                "agents/{agent_id}/api/v1/in/{app}",
                agent_id = me,
                app = account_id,
            )),
            Source::Unicast(None) => Ok(format!("agents/{agent_id}/api/v1/in/+", agent_id = me)),
            _ => Err(format_err!(
                "source = '{:?}' is incompatible with response subscription",
                self.source,
            )),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub mod compat {

    use super::{
        Destination, DestinationTopic, IncomingEvent, IncomingEventProperties, IncomingMessage,
        IncomingRequest, IncomingRequestProperties, IncomingResponse, IncomingResponseProperties,
        OutgoingEventProperties, OutgoingRequestProperties, OutgoingResponseProperties,
        Publishable,
    };
    use crate::transport::AgentId;
    use failure::{err_msg, format_err, Error};
    use serde_derive::{Deserialize, Serialize};

    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "lowercase")]
    #[serde(tag = "type")]
    pub(crate) enum IncomingEnvelopeProperties {
        Event(IncomingEventProperties),
        Request(IncomingRequestProperties),
        Response(IncomingResponseProperties),
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct IncomingEnvelope {
        payload: String,
        properties: IncomingEnvelopeProperties,
    }

    impl IncomingEnvelope {
        pub(crate) fn properties(&self) -> &IncomingEnvelopeProperties {
            &self.properties
        }

        pub(crate) fn payload<T>(&self) -> Result<T, Error>
        where
            T: serde::de::DeserializeOwned,
        {
            let payload = serde_json::from_str::<T>(&self.payload)?;
            Ok(payload)
        }
    }

    pub(crate) fn into_event<T>(envelope: IncomingEnvelope) -> Result<IncomingEvent<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            IncomingEnvelopeProperties::Event(props) => Ok(IncomingMessage::new(payload, props)),
            val => Err(format_err!("error converting into event = {:?}", val)),
        }
    }

    pub(crate) fn into_request<T>(envelope: IncomingEnvelope) -> Result<IncomingRequest<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            IncomingEnvelopeProperties::Request(props) => Ok(IncomingMessage::new(payload, props)),
            _ => Err(err_msg("Error converting into request")),
        }
    }

    pub(crate) fn into_response<T>(envelope: IncomingEnvelope) -> Result<IncomingResponse<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            IncomingEnvelopeProperties::Response(props) => Ok(IncomingMessage::new(payload, props)),
            _ => Err(err_msg("error converting into response")),
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "lowercase")]
    #[serde(tag = "type")]
    pub(crate) enum OutgoingEnvelopeProperties {
        Event(OutgoingEventProperties),
        Request(OutgoingRequestProperties),
        Response(OutgoingResponseProperties),
    }

    #[derive(Debug, Serialize)]
    pub(crate) struct OutgoingEnvelope {
        payload: String,
        properties: OutgoingEnvelopeProperties,
        #[serde(skip)]
        destination: Destination,
    }

    impl OutgoingEnvelope {
        pub(crate) fn new(
            payload: &str,
            properties: OutgoingEnvelopeProperties,
            destination: Destination,
        ) -> Self {
            Self {
                payload: payload.to_owned(),
                properties,
                destination,
            }
        }
    }

    impl DestinationTopic for OutgoingEnvelopeProperties {
        fn destination_topic(
            &self,
            agent_id: &AgentId,
            dest: &Destination,
        ) -> Result<String, Error> {
            match self {
                OutgoingEnvelopeProperties::Event(val) => val.destination_topic(agent_id, dest),
                OutgoingEnvelopeProperties::Request(val) => val.destination_topic(agent_id, dest),
                OutgoingEnvelopeProperties::Response(val) => val.destination_topic(agent_id, dest),
            }
        }
    }

    impl<'a> Publishable for OutgoingEnvelope {
        fn destination_topic(&self, agent_id: &AgentId) -> Result<String, Error> {
            self.properties
                .destination_topic(agent_id, &self.destination)
        }

        fn to_bytes(&self) -> Result<String, Error> {
            Ok(serde_json::to_string(&self)?)
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    pub(crate) trait IntoEnvelope {
        fn into_envelope(self) -> Result<OutgoingEnvelope, Error>;
    }

}
