use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{OnceCell, RwLock};
use tracing::{Instrument, debug, error, info, trace, warn};

use crate::basicstation;
use crate::config;
use crate::monitoring::{inc_gateway_udp_received_count, inc_gateway_udp_sent_count};
use crate::mqtt;
use crate::packets::{GatewayId, PacketType, PushData, TxAck, get_random_token};
use crate::traits::PrintFullError;

static GATEWAYS: OnceCell<RwLock<HashMap<GatewayId, Gateway>>> = OnceCell::const_new();

/// Track pending downlink IDs by (gateway_id, token) -> downlink_id
static PENDING_DOWNLINKS: OnceCell<RwLock<HashMap<(GatewayId, u16), u32>>> = OnceCell::const_new();

struct Gateway {
    addr: SocketAddr,
    socket: Arc<UdpSocket>,
    last_seen: SystemTime,
}

pub async fn setup(
    inputs: &[config::GwmpInput],
) -> Result<(
    UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,  // downlink_tx (gateway_id, data, downlink_id)
    UnboundedReceiver<(GatewayId, Vec<u8>, String, Option<String>)>, // uplink_rx (gateway_id, data, region)
    UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,  // uplink_tx (for MQTT-in)
)> {
    let (uplink_tx, uplink_rx) = unbounded_channel::<(GatewayId, Vec<u8>, String, Option<String>)>();
    let (downlink_tx, downlink_rx) = unbounded_channel::<(GatewayId, Vec<u8>, Option<u32>)>();

    for input in inputs {
        info!(bind = %input.bind, topic_prefix = %input.topic_prefix, "Setting up listener");

        let sock = UdpSocket::bind(&input.bind).await.context("Bind socket")?;
        let sock = Arc::new(sock);

        let topic_prefix = input.topic_prefix.clone();
        tokio::spawn(handle_uplink(sock.clone(), uplink_tx.clone(), topic_prefix));
    }

    tokio::spawn(handle_downlink(downlink_rx));
    tokio::spawn(cleanup_gateways());

    Ok((downlink_tx, uplink_rx, uplink_tx))
}

async fn handle_uplink(socket: Arc<UdpSocket>, uplink_tx: UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>, topic_prefix: String) {
    let mut buffer: [u8; 65535] = [0; 65535];
    loop {
        let (size, addr) = match socket.recv_from(&mut buffer).await {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Receive error");
                continue;
            }
        };

        if size < 4 {
            warn!(addr = %addr, received_bytes = size, "At least 4 bytes are expected");
            continue;
        }

        if let Err(e) = handle_uplink_packet(&socket, &uplink_tx, addr, &buffer[..size], &topic_prefix)
            .instrument(tracing::info_span!("", addr = %addr))
            .await
        {
            error!(error = %e.full(), "Handle uplink packet error");
        }
    }
}

async fn handle_uplink_packet(
    socket: &Arc<UdpSocket>,
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    addr: SocketAddr,
    data: &[u8],
    topic_prefix: &str,
) -> Result<()> {
    let packet_type = PacketType::try_from(data)?;
    let gateway_id = GatewayId::try_from(data)?;
    let _token = get_random_token(data)?;

    // For PushData, extract and log DevAddr info from rxpk entries
    let frame_info = if packet_type == PacketType::PushData {
        PushData::from_slice(data)
            .ok()
            .map(|pd| {
                pd.payload.rxpk.iter()
                    .map(|rxpk| rxpk.get_frame_info())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .filter(|s| !s.is_empty())
    } else {
        None
    };

    if let Some(ref frames) = frame_info {
        info!(
            gateway_id = %gateway_id,
            frames = %frames,
            "Received UDP uplink",
        );
    } else {
        info!(
            packet_type = %packet_type,
            gateway_id = %gateway_id,
            "Received UDP packet",
        );
    }

    inc_gateway_udp_received_count(gateway_id, packet_type).await;

    match packet_type {
        PacketType::PushData => handle_push_data(socket, uplink_tx, addr, gateway_id, data, topic_prefix).await?,
        PacketType::PullData => {
            set_gateway(gateway_id, addr, socket.clone()).await?;
            handle_pull_data(socket, uplink_tx, addr, gateway_id, data, topic_prefix).await?;
        }
        PacketType::TxAck => handle_tx_ack(uplink_tx, gateway_id, data, topic_prefix).await?,
        _ => warn!(packet_type = %packet_type, "Unexpected packet-type"),
    }

    Ok(())
}

async fn handle_downlink(
    mut downlink_rx: UnboundedReceiver<(GatewayId, Vec<u8>, Option<u32>)>,
) {
    while let Some((gateway_id, data, downlink_id)) = downlink_rx.recv().await {
        if let Err(e) = handle_downlink_packet(gateway_id, &data, downlink_id).await {
            error!(error = %e.full(), "Handle downlink packet error");
        }
    }
}

async fn handle_downlink_packet(
    gateway_id: GatewayId,
    data: &[u8],
    downlink_id: Option<u32>,
) -> Result<()> {
    let packet_type = PacketType::try_from(data)?;

    // Check if this is a Basic Station gateway first
    if basicstation::is_bs_gateway(gateway_id).await {
        basicstation::send_downlink_frame(gateway_id, data).await?;
        return Ok(());
    }

    // Check if this is an MQTT gateway (includes virtual relay gateways)
    if mqtt::is_mqtt_gateway(gateway_id).await {
        // Route via MQTT (send_downlink_frame handles relay→border resolution)
        mqtt::send_downlink_frame(gateway_id, data).await?;
        return Ok(());
    }

    // Fallback: check if this is a virtual relay gateway whose border gateway is an MQTT gateway
    if let Some(border_gw) = mqtt::resolve_relay_gateway(gateway_id).await {
        if mqtt::is_mqtt_gateway(border_gw).await {
            info!(
                virtual_gateway = %gateway_id,
                border_gateway = %border_gw,
                "Routing relay downlink to border gateway (fallback)"
            );
            mqtt::send_downlink_frame(gateway_id, data).await?;
            return Ok(());
        }
    }

    // For PULL_RESP, extract the token and store the mapping to the real downlink_id
    if packet_type == PacketType::PullResp && data.len() >= 4 {
        let token = u16::from_le_bytes([data[1], data[2]]);
        // Use the provided downlink_id if available, otherwise fall back to token
        let real_downlink_id = downlink_id.unwrap_or(token as u32);
        store_pending_downlink(gateway_id, token, real_downlink_id).await;
    }

    // Route via UDP — look up the gateway's socket and address
    let (addr, socket) = get_gateway_socket(gateway_id).await?;
    let span = tracing::info_span!("", addr = %addr);

    async move {
        info!(packet_type = %packet_type, gateway_id = %gateway_id, "Sending UDP packet");

        socket
            .send_to(data, addr)
            .await
            .context("Socket send")
            .map(|_| ())
    }
    .instrument(span)
    .await?;

    inc_gateway_udp_sent_count(gateway_id, packet_type).await;

    Ok(())
}

async fn handle_push_data(
    socket: &Arc<UdpSocket>,
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    addr: SocketAddr,
    gateway_id: GatewayId,
    data: &[u8],
    topic_prefix: &str,
) -> Result<()> {
    if data.len() < 12 {
        return Err(anyhow!("At least 12 bytes are expected"));
    }

    info!(packet_type = %PacketType::PushAck, "Sending UDP packet");

    let b: [u8; 4] = [data[0], data[1], data[2], PacketType::PushAck.into()];
    socket.send_to(&b, addr).await.context("Socket send")?;
    inc_gateway_udp_sent_count(gateway_id, PacketType::PushAck).await;

    debug!("Sending received data to uplink channel");
    uplink_tx
        .send((gateway_id, data.to_vec(), topic_prefix.to_string(), None))
        .context("Uplink channel send")?;

    Ok(())
}

async fn handle_tx_ack(
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    gateway_id: GatewayId,
    data: &[u8],
    topic_prefix: &str,
) -> Result<()> {
    // Forward to UDP servers via uplink channel
    uplink_tx
        .send((gateway_id, data.to_vec(), topic_prefix.to_string(), None))
        .context("Uplink channel send")?;

    // Forward to MQTT backends if enabled
    let mqtt_enabled = mqtt::is_enabled().await;
    let is_mqtt_gw = mqtt::is_mqtt_gateway(gateway_id).await;

    debug!(
        gateway_id = %gateway_id,
        mqtt_enabled = mqtt_enabled,
        is_mqtt_gateway = is_mqtt_gw,
        "TX_ACK received, checking MQTT forwarding"
    );

    if mqtt_enabled && !is_mqtt_gw {
        match TxAck::from_slice(data) {
            Ok(tx_ack) => {
                let token = tx_ack.random_token;
                // Look up the downlink_id from our pending map
                let downlink_id = get_pending_downlink(gateway_id, token).await.unwrap_or(token as u32);
                let ack = tx_ack.to_downlink_tx_ack(&gateway_id, downlink_id);

                info!(
                    gateway_id = %gateway_id,
                    downlink_id = downlink_id,
                    "Forwarding TX_ACK to MQTT"
                );

                if let Err(e) = mqtt::send_tx_ack(gateway_id, &ack, topic_prefix).await {
                    error!(error = %e, "MQTT send TX_ACK error");
                }
            }
            Err(e) => {
                warn!(gateway_id = %gateway_id, error = %e, "Failed to parse TX_ACK");
            }
        }
    } else {
        debug!(
            gateway_id = %gateway_id,
            mqtt_enabled = mqtt_enabled,
            is_mqtt_gateway = is_mqtt_gw,
            "TX_ACK not forwarded to MQTT (mqtt_enabled={}, is_mqtt_gateway={})",
            mqtt_enabled,
            is_mqtt_gw
        );
    }

    Ok(())
}

async fn handle_pull_data(
    socket: &Arc<UdpSocket>,
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    addr: SocketAddr,
    gateway_id: GatewayId,
    data: &[u8],
    topic_prefix: &str,
) -> Result<()> {
    if data.len() < 12 {
        return Err(anyhow!("At least 12 bytes are expected"));
    }

    info!(packet_type = %PacketType::PullAck, "Sending UDP packet");

    let b: [u8; 4] = [data[0], data[1], data[2], PacketType::PullAck.into()];
    socket.send_to(&b, addr).await.context("Socket send")?;
    inc_gateway_udp_sent_count(gateway_id, PacketType::PullAck).await;

    uplink_tx
        .send((gateway_id, data.to_vec(), topic_prefix.to_string(), None))
        .context("Uplink channel send")?;

    Ok(())
}

async fn set_gateway(gateway_id: GatewayId, addr: SocketAddr, socket: Arc<UdpSocket>) -> Result<()> {
    trace!(gateway_id = %gateway_id, addr = %addr, "Setting / updating Gateway ID to addr mapping");

    let gateways = GATEWAYS
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;

    let mut gateways = gateways.write().await;
    let _ = gateways.insert(
        gateway_id,
        Gateway {
            addr,
            socket,
            last_seen: SystemTime::now(),
        },
    );

    Ok(())
}

async fn get_gateway_socket(gateway_id: GatewayId) -> Result<(SocketAddr, Arc<UdpSocket>)> {
    trace!(gateway_id = %gateway_id, "Getting addr and socket for Gateway ID");

    let gateways = GATEWAYS
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;

    let gateways = gateways.read().await;
    gateways
        .get(&gateway_id)
        .map(|v| (v.addr, v.socket.clone()))
        .ok_or_else(|| anyhow!("Unknown Gateway ID: {}", gateway_id))
}

async fn cleanup_gateways() {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

        trace!("Cleaning up inactive Gateway ID to addr mappings");

        let gateways = GATEWAYS
            .get_or_init(|| async { RwLock::new(HashMap::new()) })
            .await;
        let mut gateways = gateways.write().await;
        gateways.retain(|k, v| {
            if let Ok(duration) = SystemTime::now().duration_since(v.last_seen) {
                if duration < Duration::from_secs(60) {
                    true
                } else {
                    warn!(gateway_id = %k, addr = %v.addr, "Cleaning up inactive mapping");
                    false
                }
            } else {
                warn!(gateway_id = %k, addr = %v.addr, "Cleaning up inactive mapping");
                false
            }
        })
    }
}

/// Store a pending downlink ID for later TX_ACK handling.
async fn store_pending_downlink(gateway_id: GatewayId, token: u16, downlink_id: u32) {
    debug!(gateway_id = %gateway_id, token = token, downlink_id = downlink_id, "Storing pending downlink for TX_ACK correlation");
    let pending = PENDING_DOWNLINKS
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;
    let mut pending = pending.write().await;
    pending.insert((gateway_id, token), downlink_id);
}

/// Get and remove a pending downlink ID.
async fn get_pending_downlink(gateway_id: GatewayId, token: u16) -> Option<u32> {
    let pending = PENDING_DOWNLINKS
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;
    let mut pending = pending.write().await;
    let result = pending.remove(&(gateway_id, token));
    debug!(gateway_id = %gateway_id, token = token, downlink_id = ?result, "Retrieved pending downlink for TX_ACK");
    result
}
