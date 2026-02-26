use std::str::FromStr;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tracing_subscriber::prelude::*;

use lorawan_multiplexer_converter::{config, forwarder, listener};
use lrwn_filters::EuiPrefix;

#[tokio::test]
async fn test() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .init();

    let conf = config::Configuration {
        gwmp: config::Gwmp {
            inputs: vec![config::GwmpInput {
                bind: "0.0.0.0:1710".into(),
                ..Default::default()
            }],
            outputs: vec![
                config::GwmpOutput {
                    server: "localhost:1711".into(),
                    ..Default::default()
                },
                config::GwmpOutput {
                    server: "localhost:1712".into(),
                    gateway_id_prefixes: vec![EuiPrefix::from_str("0101000000000000/16").unwrap()],
                    ..Default::default()
                },
            ],
        },
        ..Default::default()
    };

    let (downlink_tx, uplink_rx, _uplink_tx) = listener::setup(&conf.gwmp.inputs).await.unwrap();
    forwarder::setup(downlink_tx, uplink_rx, conf.gwmp.outputs.clone())
        .await
        .unwrap();
    let mut buffer: [u8; 65535] = [0; 65535];

    // Server sockets.
    let server1_sock = UdpSocket::bind("0.0.0.0:1711").await.unwrap();
    let server2_sock = UdpSocket::bind("0.0.0.0:1701").await.unwrap();

    // Gateway socket.
    let gw_sock = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    gw_sock.connect("localhost:1710").await.unwrap();

    // Send PUSH_DATA (unconfirmed uplink with DevAddr 01020304).
    gw_sock
        .send(&[
            0x02, 0x01, 0x02, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x7b, 0x22,
            0x72, 0x78, 0x70, 0x6b, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x64, 0x61, 0x74, 0x61, 0x22,
            0x3a, 0x22, 0x51, 0x41, 0x51, 0x44, 0x41, 0x67, 0x45, 0x3d, 0x22, 0x7d, 0x5d, 0x7d,
        ])
        .await
        .unwrap();

    // Expect PUSH_DATA forwarded to server 1.
    let size = server1_sock.recv(&mut buffer).await.unwrap();
    assert_eq!(
        &[
            0x02, 0x01, 0x02, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x7b, 0x22,
            0x72, 0x78, 0x70, 0x6b, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x64, 0x61, 0x74, 0x61, 0x22,
            0x3a, 0x22, 0x51, 0x41, 0x51, 0x44, 0x41, 0x67, 0x45, 0x3d, 0x22, 0x7d, 0x5d, 0x7d,
        ],
        &buffer[..size]
    );

    // Expect PUSH_DATA not forwarded to server 2.
    let resp = timeout(Duration::from_millis(100), server2_sock.recv(&mut buffer)).await;
    assert!(resp.is_err());
}
