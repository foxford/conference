use super::{AccountId, AgentId, AuthnMessageProperties, Destination, Publishable, SharedGroup};
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
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub(crate) enum MessageProperties {
    Event(EventMessageProperties),
    Request(RequestMessageProperties),
    Response(ResponseMessageProperties),
}

impl MessageProperties {
    pub(crate) fn authn(&self) -> &AuthnMessageProperties {
        match self {
            MessageProperties::Event(ref props) => &props.authn,
            MessageProperties::Request(ref props) => &props.authn,
            MessageProperties::Response(ref props) => &props.authn,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct EventMessageProperties {
    #[serde(flatten)]
    authn: AuthnMessageProperties,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct RequestMessageProperties {
    method: String,
    response_topic: String,
    correlation_data: String,
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
        LocalResponseMessageProperties::new(status, &self.correlation_data)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ResponseMessageProperties {
    correlation_data: String,
    #[serde(flatten)]
    authn: AuthnMessageProperties,
}

impl From<&MessageProperties> for AccountId {
    fn from(props: &MessageProperties) -> Self {
        AccountId::from(props.authn())
    }
}

impl From<&MessageProperties> for AgentId {
    fn from(props: &MessageProperties) -> Self {
        AgentId::from(props.authn())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub(crate) enum LocalMessageProperties {
    Event(LocalEventMessageProperties),
    Request(LocalRequestMessageProperties),
    Response(LocalResponseMessageProperties),
}

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
}

impl LocalResponseMessageProperties {
    pub(crate) fn new(status: LocalResponseMessageStatus, correlation_data: &str) -> Self {
        Self {
            status,
            correlation_data: correlation_data.to_owned(),
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
pub(crate) struct LocalMessage<T>
where
    T: serde::Serialize,
{
    payload: T,
    properties: LocalMessageProperties,
    destination: Destination,
}

impl<T> LocalMessage<T>
where
    T: serde::Serialize,
{
    pub(crate) fn new(
        payload: T,
        properties: LocalMessageProperties,
        destination: Destination,
    ) -> Self {
        Self {
            payload,
            properties,
            destination,
        }
    }

    pub(crate) fn payload(&self) -> &T {
        &self.payload
    }

    pub(crate) fn properties(&self) -> &LocalMessageProperties {
        &self.properties
    }
}

impl<T> Publishable for LocalMessage<T>
where
    T: serde::Serialize,
{
    fn destination_topic(&self, agent_id: &AgentId) -> Result<String, Error> {
        DestinationTopicBuilder::build(agent_id, &self.properties, &self.destination)
    }

    fn to_bytes(&self) -> Result<String, Error> {
        let envelope = self::compat::from_message(self)?;
        let payload = serde_json::to_string(&envelope)?;
        Ok(payload)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Message<T> {
    payload: T,
    properties: MessageProperties,
}

impl<T> Message<T> {
    pub(crate) fn new(payload: T, properties: MessageProperties) -> Self {
        Self {
            payload,
            properties,
        }
    }

    pub(crate) fn subject(&self) -> AccountId {
        AccountId::from(&self.properties)
    }

    pub(crate) fn agent_id(&self) -> AgentId {
        AgentId::from(&self.properties)
    }

    pub(crate) fn payload(&self) -> &T {
        &self.payload
    }

    pub(crate) fn properties(&self) -> &MessageProperties {
        &self.properties
    }

    pub(crate) fn to_response<R>(
        &self,
        data: R,
        status: LocalResponseMessageStatus,
    ) -> Result<LocalMessage<R>, Error>
    where
        R: serde::Serialize,
    {
        match self.properties() {
            MessageProperties::Request(req_props) => Ok(LocalMessage::new(
                data,
                LocalMessageProperties::Response(req_props.to_response(status)),
                Destination::Unicast(self.agent_id()),
            )),
            _ => Err(err_msg("Error converting request to response")),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

struct DestinationTopicBuilder;

impl DestinationTopicBuilder {
    fn build(
        agent_id: &AgentId,
        props: &LocalMessageProperties,
        dest: &Destination,
    ) -> Result<String, Error> {
        match props {
            LocalMessageProperties::Event(_) => Self::build_event_topic(agent_id, dest),
            LocalMessageProperties::Request(_) => Self::build_request_topic(agent_id, dest),
            LocalMessageProperties::Response(_) => Self::build_response_topic(agent_id, dest),
        }
    }

    // TODO: api version
    fn build_event_topic(agent_id: &AgentId, dest: &Destination) -> Result<String, Error> {
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

    // TODO: api version
    fn build_request_topic(agent_id: &AgentId, dest: &Destination) -> Result<String, Error> {
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

    // TODO: api version
    fn build_response_topic(agent_id: &AgentId, dest: &Destination) -> Result<String, Error> {
        match dest {
            Destination::Unicast(ref dest_agent_id) => Ok(format!(
                "agents/{agent_id}/api/v1/in/{app_name}",
                agent_id = dest_agent_id,
                app_name = &agent_id.account_id(),
            )),
            _ => Err(format_err!(
                "Destination {:?} incompatible with response message type",
                dest,
            )),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait Publish {
    fn publish(&self, tx: &mut Agent) -> Result<(), Error>;
}

impl<T> Publish for T
where
    T: Publishable,
{
    fn publish(&self, tx: &mut Agent) -> Result<(), Error> {
        tx.publish(self)?;
        Ok(())
    }
}

impl<T1, T2> Publish for (T1, T2)
where
    T1: Publishable,
    T2: Publishable,
{
    fn publish(&self, tx: &mut Agent) -> Result<(), Error> {
        tx.publish(&self.0)?;
        tx.publish(&self.1)?;
        Ok(())
    }
}

pub mod compat {

    use super::{LocalMessage, LocalMessageProperties, Message, MessageProperties};
    use failure::Error;
    use serde_derive::{Deserialize, Serialize};

    #[derive(Debug, Serialize)]
    pub(crate) struct LocalEnvelope<'a> {
        payload: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        properties: Option<&'a LocalMessageProperties>,
    }

    pub(crate) fn from_message<'a, T>(
        message: &'a LocalMessage<T>,
    ) -> Result<LocalEnvelope<'a>, Error>
    where
        T: serde::Serialize,
    {
        let payload = serde_json::to_string(message.payload())?;
        let envelope = LocalEnvelope {
            payload,
            properties: Some(message.properties()),
        };
        Ok(envelope)
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct Envelope {
        payload: String,
        properties: MessageProperties,
    }

    impl Envelope {
        pub(crate) fn properties(&self) -> &MessageProperties {
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

    pub(crate) fn into_message<T>(envelope: Envelope) -> Result<Message<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        let properties = envelope.properties;
        Ok(Message::new(payload, properties))
    }

}
