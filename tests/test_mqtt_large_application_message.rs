use std::time::Duration;

use tokio::sync::mpsc::unbounded_channel;
use tokio::time::{sleep, timeout};

use lorawan_multiplexer_converter::packets::GatewayId;
use lorawan_multiplexer_converter::{config, mqtt};

mod common;

const WAIT: Duration = Duration::from_secs(5);

/// An `application/#` message larger than 10 KiB (the default packet size
/// limit of the MQTT client) must be forwarded intact, without dropping the
/// connection to either broker.
#[tokio::test(flavor = "multi_thread")]
async fn test() {
    let topic = "application/1/device/0102030405060708/event/up";
    let payload: Vec<u8> = (0..100 * 1024).map(|i| (i % 251) as u8).collect();

    // The source broker sends the message as soon as the multiplexer
    // subscribes to application/#.
    let source = common::FakeBroker::start(vec![common::OnSubscribe {
        filter: "application/#".into(),
        topic: topic.into(),
        payload: payload.clone(),
    }]);
    let mut analyzer = common::FakeBroker::start(vec![]);

    let conf = config::MqttConfig {
        inputs: vec![],
        outputs: vec![
            config::MqttOutput {
                server: source.url(),
                subscribe_application: true,
                ..Default::default()
            },
            config::MqttOutput {
                server: analyzer.url(),
                analyzer: true,
                forward_application: true,
                ..Default::default()
            },
        ],
    };

    let (downlink_tx, _downlink_rx) = unbounded_channel::<(GatewayId, Vec<u8>, Option<u32>)>();
    let (uplink_tx, _uplink_rx) =
        unbounded_channel::<(GatewayId, Vec<u8>, String, Option<String>, String)>();
    mqtt::setup(&conf, downlink_tx, uplink_tx).await.unwrap();

    // Expect the message, intact, at the analyzer broker.
    let (received_topic, received_payload) = timeout(WAIT, analyzer.published.recv())
        .await
        .expect("application message was not forwarded")
        .unwrap();
    assert_eq!(topic, received_topic);
    assert!(payload == received_payload, "payload was altered");

    // Expect both connections to still be the first ones (an oversized packet
    // used to be a connection error, followed by a reconnect).
    sleep(Duration::from_millis(1500)).await;
    assert_eq!(1, source.connection_count());
    assert_eq!(1, analyzer.connection_count());
}
