use super::{AuthnProperties, Destination, SharedGroup, Source};
use crate::authn::{AccountId, AgentId, Authenticable};
use failure::{err_msg, format_err, Error};
use serde_derive::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////

pub(crate) use rumqtt::QoS;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct AgentOptions {
    pub(crate) uri: String,
}

#[derive(Debug)]
pub(crate) struct AgentBuilder {
    agent_id: AgentId,
}

impl AgentBuilder {
    pub(crate) fn new(agent_id: AgentId) -> Self {
        Self { agent_id }
    }

    pub(crate) fn start(
        self,
        config: &AgentOptions,
    ) -> Result<(Agent, crossbeam_channel::Receiver<rumqtt::Notification>), Error> {
        let client_id = Self::mqtt_client_id(&self.agent_id);
        let options = Self::mqtt_options(&client_id, &config)?;
        let (tx, rx) = rumqtt::MqttClient::start(options)?;

        let mut agent = Agent::new(self.agent_id, tx);
        Ok((agent, rx))
    }

    fn mqtt_client_id(agent_id: &AgentId) -> String {
        format!("v1.mqtt3/agents/{agent_id}", agent_id = agent_id)
    }

    fn mqtt_options(client_id: &str, config: &AgentOptions) -> Result<rumqtt::MqttOptions, Error> {
        let uri = config.uri.parse::<http::Uri>()?;
        let host = uri.host().ok_or_else(|| err_msg("missing MQTT host"))?;
        let port = uri
            .port_part()
            .ok_or_else(|| err_msg("missing MQTT port"))?;

        Ok(rumqtt::MqttOptions::new(client_id, host, port.as_u16()).set_keep_alive(30))
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
        group: &SharedGroup,
    ) -> Result<(), Error>
    where
        S: SubscriptionTopic,
    {
        let topic = subscription.subscription_topic(&self.id, group)?;
        self.tx.subscribe(topic, qos)?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct IncomingEventProperties {
    #[serde(flatten)]
    authn: AuthnProperties,
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

#[derive(Debug, Deserialize)]
pub(crate) struct IncomingResponseProperties {
    correlation_data: String,
    #[serde(flatten)]
    authn: AuthnProperties,
}

impl Authenticable for IncomingEventProperties {
    fn account_id(&self) -> AccountId {
        AccountId::from(&self.authn)
    }

    fn agent_id(&self) -> AgentId {
        AgentId::from(&self.authn)
    }
}

impl Authenticable for IncomingRequestProperties {
    fn account_id(&self) -> AccountId {
        AccountId::from(&self.authn)
    }

    fn agent_id(&self) -> AgentId {
        AgentId::from(&self.authn)
    }
}

impl Authenticable for IncomingResponseProperties {
    fn account_id(&self) -> AccountId {
        AccountId::from(&self.authn)
    }

    fn agent_id(&self) -> AgentId {
        AgentId::from(&self.authn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct IncomingMessage<T, P>
where
    P: Authenticable,
{
    payload: T,
    properties: P,
}

impl<T, P> IncomingMessage<T, P>
where
    P: Authenticable,
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
            Destination::Unicast(self.properties.agent_id()),
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
    fn destination_topic(&self, agent_id: &AgentId, dest: &Destination) -> Result<String, Error> {
        match dest {
            Destination::Broadcast(ref dest_uri) => Ok(format!(
                "apps/{app_name}/api/v1/{uri}",
                app_name = &agent_id.account_id(),
                uri = dest_uri,
            )),
            _ => Err(format_err!(
                "destination = '{:?}' is incompatible with event message type",
                dest,
            )),
        }
    }
}

impl DestinationTopic for OutgoingRequestProperties {
    fn destination_topic(&self, agent_id: &AgentId, dest: &Destination) -> Result<String, Error> {
        match dest {
            Destination::Unicast(ref dest_agent_id) => Ok(format!(
                "agents/{agent_id}/api/v1/in/{app_name}",
                agent_id = dest_agent_id,
                app_name = &agent_id.account_id(),
            )),
            Destination::Multicast(ref dest_account_id) => Ok(format!(
                "agents/{agent_id}/api/v1/out/{app_name}",
                agent_id = agent_id,
                app_name = dest_account_id,
            )),
            _ => Err(format_err!(
                "destination = '{:?}' is incompatible with request message type",
                dest,
            )),
        }
    }
}

impl DestinationTopic for OutgoingResponseProperties {
    fn destination_topic(&self, agent_id: &AgentId, dest: &Destination) -> Result<String, Error> {
        match &self.response_topic {
            Some(ref val) => Ok(val.to_owned()),
            None => match dest {
                Destination::Unicast(ref dest_agent_id) => Ok(format!(
                    "agents/{agent_id}/api/v1/in/{app_name}",
                    agent_id = dest_agent_id,
                    app_name = &agent_id.account_id(),
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
    fn subscription_topic(&self, agent_id: &AgentId, group: &SharedGroup) -> Result<String, Error>;
}

pub(crate) struct EventSubscription {
    source: Source,
}

impl EventSubscription {
    pub(crate) fn new(source: Source) -> Self {
        Self { source }
    }
}

impl SubscriptionTopic for EventSubscription {
    fn subscription_topic(&self, _: &AgentId, group: &SharedGroup) -> Result<String, Error> {
        match self.source {
            Source::Broadcast(ref source_account_id, ref source_uri) => Ok(format!(
                "$share/{group}/apps/{app_name}/api/v1/{uri}",
                group = group,
                app_name = source_account_id,
                uri = source_uri,
            )),
            _ => Err(format_err!(
                "source = '{:?}' is incompatible with event subscription",
                self.source,
            )),
        }
    }
}

pub(crate) struct RequestSubscription {
    source: Source,
}

impl RequestSubscription {
    pub(crate) fn new(source: Source) -> Self {
        Self { source }
    }
}

impl SubscriptionTopic for RequestSubscription {
    fn subscription_topic(&self, agent_id: &AgentId, group: &SharedGroup) -> Result<String, Error> {
        match self.source {
            Source::Unicast(ref source_account_id) => Ok(format!(
                "$share/{group}/agents/{agent_id}/api/v1/in/{app_name}",
                group = group,
                agent_id = agent_id,
                app_name = source_account_id,
            )),
            Source::Multicast => Ok(format!(
                "$share/{group}/agents/+/api/v1/out/{app_name}",
                group = group,
                app_name = agent_id.account_id(),
            )),
            _ => Err(format_err!(
                "source = '{:?}' is incompatible with request subscription",
                self.source,
            )),
        }
    }
}

pub(crate) struct ResponseSubscription {
    source: Source,
}

impl ResponseSubscription {
    pub(crate) fn new(source: Source) -> Self {
        Self { source }
    }
}

impl SubscriptionTopic for ResponseSubscription {
    fn subscription_topic(&self, agent_id: &AgentId, group: &SharedGroup) -> Result<String, Error> {
        match self.source {
            Source::Unicast(ref source_account_id) => Ok(format!(
                "$share/{group}/agents/{agent_id}/api/v1/in/{app_name}",
                group = group,
                agent_id = agent_id,
                app_name = source_account_id,
            )),
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
    use crate::authn::AgentId;
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
