use super::{AccountId, AgentId, Authenticable, AuthnProperties, Destination, SharedGroup};
use failure::{err_msg, format_err, Error};
use rumqtt::{MqttClient, MqttOptions, QoS};
use serde_derive::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct AgentOptions {
    pub(crate) uri: String,
}

#[derive(Debug)]
pub(crate) struct AgentBuilder {
    agent_id: AgentId,
    backend_account_id: AccountId,
}

impl AgentBuilder {
    pub(crate) fn new(agent_id: AgentId, backend_account_id: AccountId) -> Self {
        Self {
            agent_id,
            backend_account_id,
        }
    }

    pub(crate) fn start(
        self,
        config: &AgentOptions,
    ) -> Result<(Agent, crossbeam_channel::Receiver<rumqtt::Notification>), Error> {
        let client_id = Self::mqtt_client_id(&self.agent_id);
        let options = Self::mqtt_options(&client_id, &config)?;
        let (tx, rx) = MqttClient::start(options)?;

        let group = SharedGroup::new("loadbalancer", self.agent_id.account_id().clone());
        let mut agent = Agent::new(self.agent_id, self.backend_account_id, tx);
        agent.tx.subscribe(
            agent.backend_responses_subscription(&group),
            QoS::AtLeastOnce,
        )?;
        agent
            .tx
            .subscribe(agent.anyone_output_subscription(&group), QoS::AtLeastOnce)?;

        Ok((agent, rx))
    }

    fn mqtt_client_id(agent_id: &AgentId) -> String {
        format!("v1.mqtt3/agents/{agent_id}", agent_id = agent_id)
    }

    fn mqtt_options(client_id: &str, config: &AgentOptions) -> Result<MqttOptions, Error> {
        let uri = config.uri.parse::<http::Uri>()?;
        let host = uri.host().ok_or_else(|| err_msg("missing MQTT host"))?;
        let port = uri
            .port_part()
            .ok_or_else(|| err_msg("missing MQTT port"))?;

        Ok(MqttOptions::new(client_id, host, port.as_u16()).set_keep_alive(30))
    }
}

pub(crate) struct Agent {
    id: AgentId,
    backend_account_id: AccountId,
    tx: rumqtt::MqttClient,
}

impl Agent {
    fn new(id: AgentId, backend_account_id: AccountId, tx: MqttClient) -> Self {
        Self {
            id,
            backend_account_id,
            tx,
        }
    }

    pub(crate) fn publish<M>(&mut self, message: &M) -> Result<(), Error>
    where
        M: Publishable,
    {
        let topic = message.destination_topic(&self.id)?;
        let bytes = message.to_bytes()?;

        self.tx
            .publish(topic, QoS::AtLeastOnce, bytes)
            .map_err(|_| err_msg("Error publishing an MQTT message"))
    }

    // TODO: SubscriptionTopicBuilder -- Destination::Broadcast("responses")
    fn backend_responses_subscription(&self, group: &SharedGroup) -> String {
        format!(
            "$share/{group}/apps/{backend_name}/api/v1/responses",
            group = group,
            backend_name = &self.backend_account_id,
        )
    }

    // TODO: SubscriptionTopicBuilder -- Destination::Multicast(me)
    fn anyone_output_subscription(&self, group: &SharedGroup) -> String {
        format!(
            "$share/{group}/agents/+/api/v1/out/{name}",
            group = group,
            name = &self.id.account_id(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct IncomingEventProperties {
    #[serde(flatten)]
    authn: AuthnProperties,
}

#[derive(Debug, Deserialize, Serialize)]
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

    pub(crate) fn to_response(&self, status: OutgoingResponseStatus) -> OutgoingResponseProperties {
        OutgoingResponseProperties::new(status, &self.correlation_data, Some(&self.response_topic))
    }
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
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

impl<T> IncomingMessage<T, IncomingRequestProperties> {
    pub(crate) fn to_response<R>(
        &self,
        data: R,
        status: OutgoingResponseStatus,
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
pub(crate) struct OutgoingEventProperties {}

#[derive(Debug, Serialize)]
pub(crate) struct OutgoingRequestProperties {
    method: String,
}

impl OutgoingRequestProperties {
    pub(crate) fn new(method: &str) -> Self {
        Self {
            method: method.to_owned(),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct OutgoingResponseProperties {
    status: OutgoingResponseStatus,
    correlation_data: String,
    #[serde(skip)]
    response_topic: Option<String>,
}

impl OutgoingResponseProperties {
    pub(crate) fn new(
        status: OutgoingResponseStatus,
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OutgoingResponseStatus {
    Success,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
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

    pub(crate) fn payload(&self) -> &T {
        &self.payload
    }

    pub(crate) fn properties(&self) -> &P {
        &self.properties
    }

    pub(crate) fn destination(&self) -> &Destination {
        &self.destination
    }
}

pub(crate) type OutgoingEvent<T> = OutgoingMessage<T, OutgoingEventProperties>;
pub(crate) type OutgoingRequest<T> = OutgoingMessage<T, OutgoingRequestProperties>;
pub(crate) type OutgoingResponse<T> = OutgoingMessage<T, OutgoingResponseProperties>;

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait Publishable {
    fn destination_topic(&self, agent_id: &AgentId) -> Result<String, Error>;
    fn to_bytes(&self) -> Result<String, Error>;
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait Publish<'a> {
    fn publish(&'a self, tx: &mut Agent) -> Result<(), Error>;
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
                "Destination {:?} incompatible with event message type",
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
                "Destination {:?} incompatible with request message type",
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
                    "Destination {:?} incompatible with response message type",
                    dest,
                )),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub mod compat {

    use super::{
        Agent, Destination, DestinationTopic, IncomingEvent, IncomingEventProperties,
        IncomingMessage, IncomingRequest, IncomingRequestProperties, IncomingResponse,
        IncomingResponseProperties, OutgoingEvent, OutgoingEventProperties, OutgoingRequest,
        OutgoingRequestProperties, OutgoingResponse, OutgoingResponseProperties, Publish,
        Publishable,
    };
    use crate::transport::AgentId;
    use failure::{err_msg, format_err, Error};
    use serde_derive::{Deserialize, Serialize};

    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug, Deserialize, Serialize)]
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
            val => Err(format_err!("Error converting into event = {:?}", val)),
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
            _ => Err(err_msg("Error converting into response")),
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "lowercase")]
    #[serde(tag = "type")]
    pub(crate) enum OutgoingEnvelopeProperties<'a> {
        Event(&'a OutgoingEventProperties),
        Request(&'a OutgoingRequestProperties),
        Response(&'a OutgoingResponseProperties),
    }

    #[derive(Debug, Serialize)]
    pub(crate) struct OutgoingEnvelope<'a> {
        payload: String,
        properties: OutgoingEnvelopeProperties<'a>,
        #[serde(skip)]
        destination: &'a Destination,
    }

    pub(crate) trait ToEnvelope<'a> {
        fn to_envelope(&'a self) -> Result<OutgoingEnvelope<'a>, Error>;
    }

    impl<'a, T> ToEnvelope<'a> for OutgoingEvent<T>
    where
        T: serde::Serialize,
    {
        fn to_envelope(&'a self) -> Result<OutgoingEnvelope<'a>, Error> {
            let payload = serde_json::to_string(self.payload())?;
            let envelope = OutgoingEnvelope {
                payload,
                properties: OutgoingEnvelopeProperties::Event(self.properties()),
                destination: self.destination(),
            };
            Ok(envelope)
        }
    }

    impl<'a, T> ToEnvelope<'a> for OutgoingRequest<T>
    where
        T: serde::Serialize,
    {
        fn to_envelope(&'a self) -> Result<OutgoingEnvelope<'a>, Error> {
            let payload = serde_json::to_string(self.payload())?;
            let envelope = OutgoingEnvelope {
                payload,
                properties: OutgoingEnvelopeProperties::Request(self.properties()),
                destination: self.destination(),
            };
            Ok(envelope)
        }
    }

    impl<'a, T> ToEnvelope<'a> for OutgoingResponse<T>
    where
        T: serde::Serialize,
    {
        fn to_envelope(&'a self) -> Result<OutgoingEnvelope<'a>, Error> {
            let payload = serde_json::to_string(self.payload())?;
            let envelope = OutgoingEnvelope {
                payload,
                properties: OutgoingEnvelopeProperties::Response(self.properties()),
                destination: self.destination(),
            };
            Ok(envelope)
        }
    }

    impl<'a> DestinationTopic for OutgoingEnvelopeProperties<'a> {
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

    impl<'a> Publishable for OutgoingEnvelope<'a> {
        fn destination_topic(&self, agent_id: &AgentId) -> Result<String, Error> {
            let dest = self.destination;
            self.properties.destination_topic(agent_id, dest)
        }

        fn to_bytes(&self) -> Result<String, Error> {
            Ok(serde_json::to_string(&self)?)
        }
    }

    impl<'a, T> Publish<'a> for T
    where
        T: ToEnvelope<'a>,
    {
        fn publish(&'a self, tx: &mut Agent) -> Result<(), Error> {
            let envelope = self.to_envelope()?;
            tx.publish(&envelope)?;
            Ok(())
        }
    }

    impl<'a, T1, T2> Publish<'a> for (T1, T2)
    where
        T1: ToEnvelope<'a>,
        T2: ToEnvelope<'a>,
    {
        fn publish(&'a self, tx: &mut Agent) -> Result<(), Error> {
            tx.publish(&self.0.to_envelope()?)?;
            tx.publish(&self.1.to_envelope()?)?;
            Ok(())
        }
    }

}
