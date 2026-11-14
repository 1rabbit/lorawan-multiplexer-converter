use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout};

use lorawan_multiplexer_converter::{config, forwarder, listener, mqtt};

mod common;

// More than the capacity of the MQTT request queue (1000), so that the queue
// of the unreachable broker fills up during the test.
const UPLINKS: usize = 1200;
const WAIT: Duration = Duration::from_secs(5);

// PHYPayload (base64) of the uplinks sent while the broker is down, and of the
// uplinks sent once it is back (raw bytes of the latter below).
const UPLINK: &str = "QAQDAgE=";
const UPLINK_AFTER_RECOVERY: &str = "QEQzIhE=";
const UPLINK_AFTER_RECOVERY_BYTES: [u8; 5] = [0x40, 0x44, 0x33, 0x22, 0x11];

/// Builds a PUSH_DATA datagram for gateway 0102030405060708 with one uplink.
fn push_data(token: u16, phy_payload: &str) -> Vec<u8> {
    let t = token.to_be_bytes();
    let mut b = vec![
        0x02, t[0], t[1], 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    ];
    b.extend_from_slice(
        format!(
            r#"{{"rxpk":[{{"tmst":1,"freq":868.1,"chan":0,"rfch":0,"stat":1,"modu":"LORA","datr":"SF7BW125","codr":"4/5","rssi":-50,"lsnr":7.0,"size":5,"data":"{phy_payload}"}}]}}"#
        )
        .as_bytes(),
    );
    b
}

/// An unreachable MQTT broker must not hold back the other outputs: with one
/// MQTT output down, every uplink still reaches the UDP server and the other
/// MQTT broker. Once the broker is back, it receives uplinks again.
#[tokio::test(flavor = "multi_thread")]
async fn test() {
    let input_port = common::free_udp_port();
    let server_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut broker = common::FakeBroker::start(vec![]);
    let down_port = common::free_tcp_port();

    let conf = config::Configuration {
        gwmp: config::Gwmp {
            inputs: vec![config::GwmpInput {
                bind: format!("127.0.0.1:{input_port}"),
                topic_prefix: "eu868".into(),
                ..Default::default()
            }],
            outputs: vec![config::GwmpOutput {
                server: server_sock.local_addr().unwrap().to_string(),
                ..Default::default()
            }],
        },
        mqtt: config::MqttConfig {
            inputs: vec![],
            outputs: vec![
                // Nothing listens on this port. It comes first, so that it is
                // served before the reachable broker.
                config::MqttOutput {
                    server: format!("tcp://127.0.0.1:{down_port}"),
                    ..Default::default()
                },
                config::MqttOutput {
                    server: broker.url(),
                    ..Default::default()
                },
            ],
        },
        ..Default::default()
    };

    let (downlink_tx, uplink_rx, uplink_tx) = listener::setup(&conf.gwmp.inputs).await.unwrap();
    forwarder::setup(downlink_tx.clone(), uplink_rx, conf.gwmp.outputs.clone())
        .await
        .unwrap();
    mqtt::setup(&conf.mqtt, downlink_tx, uplink_tx).await.unwrap();

    // Count the PUSH_DATA datagrams arriving at the UDP server.
    let server = tokio::spawn(async move {
        let mut buffer: [u8; 65535] = [0; 65535];
        for received in 0..UPLINKS {
            if timeout(WAIT, server_sock.recv(&mut buffer)).await.is_err() {
                return received;
            }
        }
        UPLINKS
    });

    // Gateway socket.
    let gw_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    gw_sock
        .connect(format!("127.0.0.1:{input_port}"))
        .await
        .unwrap();
    let mut buffer: [u8; 65535] = [0; 65535];

    for i in 0..UPLINKS {
        gw_sock.send(&push_data(i as u16, UPLINK)).await.unwrap();

        // Expect PUSH_ACK.
        let size = timeout(WAIT, gw_sock.recv(&mut buffer))
            .await
            .expect("no PUSH_ACK")
            .unwrap();
        assert_eq!(4, size);
        assert_eq!(0x01, buffer[3]);
    }

    // Expect every uplink at the UDP server.
    assert_eq!(UPLINKS, server.await.unwrap());

    // Expect every uplink at the reachable MQTT broker.
    for received in 0..UPLINKS {
        let (topic, _) = timeout(WAIT, broker.published.recv())
            .await
            .unwrap_or_else(|_| panic!("reachable broker received {received} of {UPLINKS} uplinks"))
            .unwrap();
        assert_eq!("eu868/gateway/0102030405060708/event/up", topic);
    }

    // Bring the unreachable broker up. Uplinks sent from now on must reach it,
    // after the ones that were still queued for it.
    let mut recovered = common::FakeBroker::start_on(down_port, vec![]);
    let deadline = Instant::now() + 2 * WAIT;
    let mut token = UPLINKS as u16;

    'recovery: loop {
        assert!(
            Instant::now() < deadline,
            "recovered broker did not receive new uplinks"
        );

        gw_sock
            .send(&push_data(token, UPLINK_AFTER_RECOVERY))
            .await
            .unwrap();
        token += 1;

        while let Ok(Some((_, payload))) =
            timeout(Duration::from_millis(100), recovered.published.recv()).await
        {
            if payload
                .windows(UPLINK_AFTER_RECOVERY_BYTES.len())
                .any(|w| w == UPLINK_AFTER_RECOVERY_BYTES)
            {
                break 'recovery;
            }
        }
    }
}
