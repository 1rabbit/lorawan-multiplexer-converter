use std::str::FromStr;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;

use lorawan_multiplexer_converter::{config, forwarder, listener};

mod common;

const WAIT: Duration = Duration::from_secs(2);

/// Builds a PUSH_DATA datagram for gateway 0102030405060708.
fn push_data(json: &str) -> Vec<u8> {
    let mut b = vec![
        0x02, 0x01, 0x02, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    ];
    b.extend_from_slice(json.as_bytes());
    b
}

/// Gateway statistics must reach the UDP outputs, both when the PUSH_DATA
/// carries only `stat` and when it also carries uplinks that an output filters
/// out.
#[tokio::test]
async fn test() {
    let input_port = common::free_udp_port();

    // Server sockets. The second output only accepts DevAddr 02xxxxxx, which
    // does not match the uplink used below (DevAddr 01020304).
    let unfiltered_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let filtered_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let conf = config::Configuration {
        gwmp: config::Gwmp {
            inputs: vec![config::GwmpInput {
                bind: format!("127.0.0.1:{input_port}"),
                ..Default::default()
            }],
            outputs: vec![
                config::GwmpOutput {
                    server: unfiltered_sock.local_addr().unwrap().to_string(),
                    ..Default::default()
                },
                config::GwmpOutput {
                    server: filtered_sock.local_addr().unwrap().to_string(),
                    filters: config::Filters {
                        dev_addr_prefixes: vec![
                            lrwn_filters::DevAddrPrefix::from_str("02000000/8").unwrap(),
                        ],
                        ..Default::default()
                    },
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

    // Gateway socket.
    let gw_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    gw_sock
        .connect(format!("127.0.0.1:{input_port}"))
        .await
        .unwrap();

    // PUSH_DATA with only gateway statistics.
    let stat_only = push_data(
        r#"{"stat":{"time":"2014-01-12 08:59:28 GMT","lati":46.24,"long":3.2523,"alti":145,"rxnb":2,"rxok":2,"rxfw":2,"ackr":100.0,"dwnb":2,"txnb":2}}"#,
    );
    gw_sock.send(&stat_only).await.unwrap();

    // Expect PUSH_ACK.
    let size = gw_sock.recv(&mut buffer).await.unwrap();
    assert_eq!(&[0x02, 0x01, 0x02, 0x01], &buffer[..size]);

    // Expect the unmodified PUSH_DATA at both servers.
    for sock in [&unfiltered_sock, &filtered_sock] {
        let size = timeout(WAIT, sock.recv(&mut buffer))
            .await
            .expect("stat-only PUSH_DATA was not forwarded")
            .unwrap();
        assert_eq!(&stat_only[..], &buffer[..size]);
    }

    // PUSH_DATA with an uplink (DevAddr 01020304) and gateway statistics.
    let uplink_and_stat =
        push_data(r#"{"rxpk":[{"data":"QAQDAgE="}],"stat":{"rxnb":2,"rxok":2,"rxfw":2,"pfrm":"test"}}"#);
    gw_sock.send(&uplink_and_stat).await.unwrap();

    // Expect PUSH_ACK.
    let size = gw_sock.recv(&mut buffer).await.unwrap();
    assert_eq!(&[0x02, 0x01, 0x02, 0x01], &buffer[..size]);

    // Expect the unmodified PUSH_DATA at the unfiltered server.
    let size = timeout(WAIT, unfiltered_sock.recv(&mut buffer))
        .await
        .expect("PUSH_DATA was not forwarded to the unfiltered server")
        .unwrap();
    assert_eq!(&uplink_and_stat[..], &buffer[..size]);

    // Expect the statistics, without the uplink, at the filtered server.
    let size = timeout(WAIT, filtered_sock.recv(&mut buffer))
        .await
        .expect("stats were not forwarded to the filtered server")
        .unwrap();
    assert_eq!(&uplink_and_stat[..12], &buffer[..12]);
    let json: serde_json::Value = serde_json::from_slice(&buffer[12..size]).unwrap();
    assert_eq!(
        serde_json::json!({"rxnb":2,"rxok":2,"rxfw":2,"pfrm":"test"}),
        json["stat"]
    );
    assert_eq!(0, json["rxpk"].as_array().map(|v| v.len()).unwrap_or(0));

    // PUSH_DATA with only the (non-matching) uplink.
    let uplink_only = push_data(r#"{"rxpk":[{"data":"QAQDAgE="}]}"#);
    gw_sock.send(&uplink_only).await.unwrap();

    // Expect PUSH_ACK.
    let size = gw_sock.recv(&mut buffer).await.unwrap();
    assert_eq!(&[0x02, 0x01, 0x02, 0x01], &buffer[..size]);

    // Expect the PUSH_DATA at the unfiltered server only.
    let size = timeout(WAIT, unfiltered_sock.recv(&mut buffer))
        .await
        .expect("PUSH_DATA was not forwarded to the unfiltered server")
        .unwrap();
    assert_eq!(&uplink_only[..], &buffer[..size]);

    let resp = timeout(Duration::from_millis(200), filtered_sock.recv(&mut buffer)).await;
    assert!(resp.is_err());
}
