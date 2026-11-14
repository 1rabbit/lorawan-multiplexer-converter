//! Helpers shared by the integration tests.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// Returns a loopback UDP port that is free right now.
pub fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Returns a loopback TCP port that is free right now (nothing listens on it).
pub fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A message the fake broker sends to a client right after that client
/// subscribes to `filter`.
#[derive(Clone)]
pub struct OnSubscribe {
    pub filter: String,
    pub topic: String,
    pub payload: Vec<u8>,
}

/// Minimal MQTT 3.1.1 broker for tests. It accepts any CONNECT, acknowledges
/// SUBSCRIBE and PINGREQ, and reports every PUBLISH it receives on
/// `published`. It does not route messages between clients.
pub struct FakeBroker {
    pub addr: SocketAddr,
    /// Number of client connections accepted so far.
    pub connections: Arc<AtomicUsize>,
    /// (topic, payload) of every PUBLISH received from any client.
    pub published: UnboundedReceiver<(String, Vec<u8>)>,
}

impl FakeBroker {
    /// Starts a broker on a free loopback port.
    pub fn start(on_subscribe: Vec<OnSubscribe>) -> FakeBroker {
        Self::start_on(0, on_subscribe)
    }

    /// Starts a broker on the given loopback port.
    pub fn start_on(port: u16, on_subscribe: Vec<OnSubscribe>) -> FakeBroker {
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        let addr = listener.local_addr().unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = unbounded_channel();

        let accepted = connections.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                accepted.fetch_add(1, Ordering::SeqCst);
                let tx = tx.clone();
                let on_subscribe = on_subscribe.clone();
                std::thread::spawn(move || {
                    // An error only means that the client went away.
                    let _ = serve_client(stream, tx, on_subscribe);
                });
            }
        });

        FakeBroker {
            addr,
            connections,
            published: rx,
        }
    }

    /// The `tcp://` URL to use as `server` in the MQTT configuration.
    pub fn url(&self) -> String {
        format!("tcp://{}", self.addr)
    }

    pub fn connection_count(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

fn serve_client(
    mut stream: TcpStream,
    published: UnboundedSender<(String, Vec<u8>)>,
    on_subscribe: Vec<OnSubscribe>,
) -> std::io::Result<()> {
    stream.set_nodelay(true)?;

    loop {
        let (header, body) = read_packet(&mut stream)?;

        match header >> 4 {
            // CONNECT -> CONNACK (session present = 0, return code = accepted)
            1 => stream.write_all(&[0x20, 0x02, 0x00, 0x00])?,
            // PUBLISH
            3 => {
                let qos = (header >> 1) & 0x03;
                let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
                let topic = String::from_utf8_lossy(&body[2..2 + topic_len]).to_string();
                let mut pos = 2 + topic_len;
                if qos > 0 {
                    // PUBACK (the tests do not use QoS 2)
                    stream.write_all(&[0x40, 0x02, body[pos], body[pos + 1]])?;
                    pos += 2;
                }
                let _ = published.send((topic, body[pos..].to_vec()));
            }
            // SUBSCRIBE -> SUBACK (QoS 0 granted for every filter)
            8 => {
                let mut filters = Vec::new();
                let mut pos = 2;
                while pos < body.len() {
                    let len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
                    filters.push(String::from_utf8_lossy(&body[pos + 2..pos + 2 + len]).to_string());
                    pos += 2 + len + 1; // filter + requested QoS
                }

                let mut suback = vec![0x90, (2 + filters.len()) as u8, body[0], body[1]];
                suback.extend(std::iter::repeat_n(0x00, filters.len()));
                stream.write_all(&suback)?;

                for msg in on_subscribe.iter().filter(|m| filters.contains(&m.filter)) {
                    stream.write_all(&encode_publish(&msg.topic, &msg.payload))?;
                }
            }
            // PINGREQ -> PINGRESP
            12 => stream.write_all(&[0xd0, 0x00])?,
            // DISCONNECT
            14 => return Ok(()),
            _ => {}
        }
    }
}

/// Reads one MQTT control packet, returns (first header byte, body).
fn read_packet(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut byte = [0u8; 1];
    stream.read_exact(&mut byte)?;
    let header = byte[0];

    let mut remaining_len = 0usize;
    let mut shift = 0;
    loop {
        stream.read_exact(&mut byte)?;
        remaining_len |= ((byte[0] & 0x7f) as usize) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    let mut body = vec![0u8; remaining_len];
    stream.read_exact(&mut body)?;
    Ok((header, body))
}

/// Encodes a QoS 0 PUBLISH packet.
fn encode_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0x30];

    let mut remaining_len = 2 + topic.len() + payload.len();
    loop {
        let mut byte = (remaining_len % 128) as u8;
        remaining_len /= 128;
        if remaining_len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if remaining_len == 0 {
            break;
        }
    }

    out.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    out.extend_from_slice(topic.as_bytes());
    out.extend_from_slice(payload);
    out
}
