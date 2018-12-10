use crate::transport;
use failure::{err_msg, Error};
use log::{error, info, warn};
use rumqtt::{MqttClient, MqttOptions, QoS};

mod config;

pub(crate) fn run() {
    // Config
    let config = config::load().expect("Failed to load config");
    info!("App config: {:?}", config);

    // Ids
    let agent_label = "a";
    let agent_id = transport::AgentId::new(
        &agent_label,
        &config.identity.account_id,
        &config.identity.audience,
    );
    let name = format!(
        "{label}.{audience}",
        label = config.identity.label,
        audience = config.identity.audience
    );
    let group = format!("loadbalancer.{name}", name = name);

    // MQTT client
    let mqtt_client_id = format!("v1.mqtt3/agents/{agent_id}", agent_id = agent_id);
    let mqtt_options =
        mqtt_options(&mqtt_client_id, &config.mqtt).expect("Failed to initialize a MQTT client");
    let (mut mqtt_tx, mqtt_rx) =
        MqttClient::start(mqtt_options).expect("Failed to start a MQTT client");

    // MQTT subscriptions
    let sub_responses_backend = format!(
        "$share/{group}/apps/{backend_name}/api/v1/responses",
        group = group,
        backend_name = config.backend.name
    );
//    let sub_events_backend = format!(
//        "$share/{group}/apps/{backend_name}/api/v1/events/+",
//        group = group,
//        backend_name = config.backend.name
//    );
//    let sub_out_others = format!(
//        "$share/{group}/agents/+/api/v1/out/{name}",
//        group = group,
//        name = name
//    );
    mqtt_tx
        .subscribe(sub_responses_backend, QoS::AtLeastOnce)
        .expect("Failed to subscribe to backend responses topic");
//    mqtt_tx
//        .subscribe(sub_events_backend, QoS::AtLeastOnce)
//        .expect("Failed to subscribe to backend events topic");
//    mqtt_tx
//        .subscribe(sub_out_others, QoS::AtLeastOnce)
//        .expect("Failed to subscribe to incoming messages topic");

    use crate::backend::janus;
    use crate::transport::compat::Envelope;

    let message = {
        let transaction = janus::Transaction::new("foobar", agent_id);

        let payload =
            serde_json::to_string(&janus::SessionCreateRequest::new(transaction)).unwrap();
        let envelope = Envelope::new(&payload);
        serde_json::to_string(&envelope).unwrap()
    };
    info!("req: {:?}", message);

    // TODO: derive a backend agent id from a status message
    let pub_in_backend = format!(
        "agents/{backend_agent_id}/api/v1/in/{name}",
        backend_agent_id = "a.00000000-0000-1071-a000-000000000000.example.org",
        name = name
    );
    mqtt_tx
        .publish(pub_in_backend, QoS::AtLeastOnce, message)
        .unwrap();

    for message in mqtt_rx {
        match message {
            rumqtt::client::Notification::Publish(message) => {
                match serde_json::from_slice::<Envelope>(message.payload.as_slice()) {
                    Ok(envelope) => {
                        match serde_json::from_str::<janus::Response>(&envelope.payload) {
                            Ok(resp) => info!("{:?}", resp),
                            _ => error!("bad janus message format = {:?}", envelope.payload),
                        }
                    }
                    _ => error!("bad envelope format = {:?}", message),
                }
            }
            _ => warn!("unknown message = {:?}", message),
        }
    }
}

pub(crate) fn mqtt_options(client_id: &str, config: &config::Mqtt) -> Result<MqttOptions, Error> {
    let uri = config.uri.parse::<http::Uri>()?;
    let host = uri.host().ok_or_else(|| err_msg("missing MQTT host"))?;
    let port = uri
        .port_part()
        .ok_or_else(|| err_msg("missing MQTT port"))?;

    Ok(MqttOptions::new(client_id, host, port.as_u16()).set_keep_alive(30))
}
