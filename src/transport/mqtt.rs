use super::{AccountId, AgentId, Authenticable, AuthnMessageProperties, Destination, SharedGroup};
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
pub(crate) struct EventMessageProperties {
    #[serde(flatten)]
    authn: AuthnMessageProperties,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct RequestMessageProperties {
    method: String,
    correlation_data: String,
    response_topic: String,
    #[serde(flatten)]
    authn: AuthnMessageProperties,
}

impl RequestMessageProperties {
    pub(crate) fn method(&self) -> &str {
        &self.method
    }

    pub(crate) fn to_response(
        &self,
        status: LocalResponseMessageStatus,
    ) -> LocalResponseMessageProperties {
        LocalResponseMessageProperties::new(
            status,
            &self.correlation_data,
            Some(&self.response_topic),
        )
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ResponseMessageProperties {
    correlation_data: String,
    #[serde(flatten)]
    authn: AuthnMessageProperties,
}

impl Authenticable for EventMessageProperties {
    fn account_id(&self) -> AccountId {
        AccountId::from(&self.authn)
    }

    fn agent_id(&self) -> AgentId {
        AgentId::from(&self.authn)
    }
}

impl Authenticable for RequestMessageProperties {
    fn account_id(&self) -> AccountId {
        AccountId::from(&self.authn)
    }

    fn agent_id(&self) -> AgentId {
        AgentId::from(&self.authn)
    }
}

impl Authenticable for ResponseMessageProperties {
    fn account_id(&self) -> AccountId {
        AccountId::from(&self.authn)
    }

    fn agent_id(&self) -> AgentId {
        AgentId::from(&self.authn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Message<T, P>
where
    P: Authenticable,
{
    payload: T,
    properties: P,
}

impl<T, P> Message<T, P>
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

impl<T> Message<T, RequestMessageProperties> {
    pub(crate) fn to_response<R>(
        &self,
        data: R,
        status: LocalResponseMessageStatus,
    ) -> LocalResponse<R>
    where
        R: serde::Serialize,
    {
        LocalMessage::new(
            data,
            self.properties.to_response(status),
            Destination::Unicast(self.properties.agent_id()),
        )
    }
}

pub(crate) type Event<T> = Message<T, EventMessageProperties>;
pub(crate) type Request<T> = Message<T, RequestMessageProperties>;
pub(crate) type Response<T> = Message<T, ResponseMessageProperties>;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct LocalEventMessageProperties {}

#[derive(Debug, Serialize)]
pub(crate) struct LocalRequestMessageProperties {
    method: String,
}

impl LocalRequestMessageProperties {
    pub(crate) fn new(method: &str) -> Self {
        Self {
            method: method.to_owned(),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct LocalResponseMessageProperties {
    status: LocalResponseMessageStatus,
    correlation_data: String,
    #[serde(skip)]
    response_topic: Option<String>,
}

impl LocalResponseMessageProperties {
    pub(crate) fn new(
        status: LocalResponseMessageStatus,
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
pub(crate) enum LocalResponseMessageStatus {
    Success,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
pub(crate) struct LocalMessage<T, P>
where
    T: serde::Serialize,
{
    payload: T,
    properties: P,
    destination: Destination,
}

impl<T, P> LocalMessage<T, P>
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

pub(crate) type LocalEvent<T> = LocalMessage<T, LocalEventMessageProperties>;
pub(crate) type LocalRequest<T> = LocalMessage<T, LocalRequestMessageProperties>;
pub(crate) type LocalResponse<T> = LocalMessage<T, LocalResponseMessageProperties>;

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

impl DestinationTopic for LocalEventMessageProperties {
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

impl DestinationTopic for LocalRequestMessageProperties {
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

impl DestinationTopic for LocalResponseMessageProperties {
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
        Agent, Destination, DestinationTopic, Event, EventMessageProperties, LocalEvent,
        LocalEventMessageProperties, LocalRequest, LocalRequestMessageProperties, LocalResponse,
        LocalResponseMessageProperties, Message, Publish, Publishable, Request,
        RequestMessageProperties, Response, ResponseMessageProperties,
    };
    use crate::transport::AgentId;
    use failure::{err_msg, format_err, Error};
    use serde_derive::{Deserialize, Serialize};

    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug, Deserialize, Serialize)]
    #[serde(rename_all = "lowercase")]
    #[serde(tag = "type")]
    pub(crate) enum EnvelopeMessageProperties {
        Event(EventMessageProperties),
        Request(RequestMessageProperties),
        Response(ResponseMessageProperties),
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct Envelope {
        payload: String,
        properties: EnvelopeMessageProperties,
    }

    impl Envelope {
        pub(crate) fn properties(&self) -> &EnvelopeMessageProperties {
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

    pub(crate) fn into_event<T>(envelope: Envelope) -> Result<Event<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            EnvelopeMessageProperties::Event(props) => Ok(Message::new(payload, props)),
            val => Err(format_err!("Error converting into event = {:?}", val)),
        }
    }

    pub(crate) fn into_request<T>(envelope: Envelope) -> Result<Request<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            EnvelopeMessageProperties::Request(props) => Ok(Message::new(payload, props)),
            _ => Err(err_msg("Error converting into request")),
        }
    }

    pub(crate) fn into_response<T>(envelope: Envelope) -> Result<Response<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            EnvelopeMessageProperties::Response(props) => Ok(Message::new(payload, props)),
            _ => Err(err_msg("Error converting into response")),
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "lowercase")]
    #[serde(tag = "type")]
    pub(crate) enum LocalEnvelopeMessageProperties<'a> {
        Event(&'a LocalEventMessageProperties),
        Request(&'a LocalRequestMessageProperties),
        Response(&'a LocalResponseMessageProperties),
    }

    #[derive(Debug, Serialize)]
    pub(crate) struct LocalEnvelope<'a> {
        payload: String,
        properties: LocalEnvelopeMessageProperties<'a>,
        #[serde(skip)]
        destination: &'a Destination,
    }

    pub(crate) trait ToEnvelope<'a> {
        fn to_envelope(&'a self) -> Result<LocalEnvelope<'a>, Error>;
    }

    impl<'a, T> ToEnvelope<'a> for LocalEvent<T>
    where
        T: serde::Serialize,
    {
        fn to_envelope(&'a self) -> Result<LocalEnvelope<'a>, Error> {
            let payload = serde_json::to_string(self.payload())?;
            let envelope = LocalEnvelope {
                payload,
                properties: LocalEnvelopeMessageProperties::Event(self.properties()),
                destination: self.destination(),
            };
            Ok(envelope)
        }
    }

    impl<'a, T> ToEnvelope<'a> for LocalRequest<T>
    where
        T: serde::Serialize,
    {
        fn to_envelope(&'a self) -> Result<LocalEnvelope<'a>, Error> {
            let payload = serde_json::to_string(self.payload())?;
            let envelope = LocalEnvelope {
                payload,
                properties: LocalEnvelopeMessageProperties::Request(self.properties()),
                destination: self.destination(),
            };
            Ok(envelope)
        }
    }

    impl<'a, T> ToEnvelope<'a> for LocalResponse<T>
    where
        T: serde::Serialize,
    {
        fn to_envelope(&'a self) -> Result<LocalEnvelope<'a>, Error> {
            let payload = serde_json::to_string(self.payload())?;
            let envelope = LocalEnvelope {
                payload,
                properties: LocalEnvelopeMessageProperties::Response(self.properties()),
                destination: self.destination(),
            };
            Ok(envelope)
        }
    }

    impl<'a> DestinationTopic for LocalEnvelopeMessageProperties<'a> {
        fn destination_topic(
            &self,
            agent_id: &AgentId,
            dest: &Destination,
        ) -> Result<String, Error> {
            match self {
                LocalEnvelopeMessageProperties::Event(inner) => {
                    inner.destination_topic(agent_id, dest)
                }
                LocalEnvelopeMessageProperties::Request(inner) => {
                    inner.destination_topic(agent_id, dest)
                }
                LocalEnvelopeMessageProperties::Response(inner) => {
                    inner.destination_topic(agent_id, dest)
                }
            }
        }
    }

    impl<'a> Publishable for LocalEnvelope<'a> {
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
