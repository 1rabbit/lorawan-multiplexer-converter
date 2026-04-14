use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{OnceCell, RwLock, oneshot};
use tracing::{Instrument, debug, error, info, trace, warn};

use crate::basicstation;
use crate::config;
use crate::filters::{AllowDenyFilters, GatewayIdFilters};
use crate::monitoring::{inc_server_udp_received_count, inc_server_udp_sent_count};
use crate::mqtt;
use crate::packets::{GatewayId, PacketType, PullData, PushData, TxAck, get_random_token};
use crate::traits::PrintFullError;

static SERVERS: OnceCell<RwLock<Vec<Server>>> = OnceCell::const_new();

struct Server {
    server: String,
    uplink_only: bool,
    gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    gateway_id_filters: GatewayIdFilters,
    filters: lrwn_filters::Filters,
    allow_deny_filters: AllowDenyFilters,
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    sockets: HashMap<GatewayId, ServerSocket>,
    /// Mesh relay virtual gateway prefix (8 hex chars). Empty = disabled.
    relay_gateway_id_prefix: String,
}

impl Server {
    fn match_gateway_id(&self, gateway_id: GatewayId) -> bool {
        let gw_id_le = gateway_id.as_bytes_le();

        // First check the new allow/deny filters
        if !self.gateway_id_filters.allow.is_empty() || !self.gateway_id_filters.deny.is_empty() {
            return self.gateway_id_filters.matches(gw_id_le);
        }

        // Fall back to legacy prefix-based filtering
        if self.gateway_id_prefixes.is_empty() {
            return true;
        }

        for prefix in &self.gateway_id_prefixes {
            if prefix.is_match(gw_id_le) {
                return true;
            }
        }

        false
    }

    async fn get_server_socket(&mut self, gateway_id: GatewayId) -> Result<&mut ServerSocket> {
        // Check if we already have a socket for the given Gateway ID to the
        // server and if not, we create it.
        if let std::collections::hash_map::Entry::Vacant(e) = self.sockets.entry(gateway_id) {
            info!(gateway_id = %gateway_id, server = %self.server, "Initializing forwarder to server");

            let socket = UdpSocket::bind("0.0.0.0:0")
                .await
                .context("UDP socket bind")?;
            socket
                .connect(&self.server)
                .await
                .context("UDP socket connect")?;

            let socket = Arc::new(socket);
            let (stop_tx, stop_rx) = oneshot::channel::<()>();

            tokio::spawn(handle_downlink(
                self.server.clone(),
                stop_rx,
                self.uplink_only,
                socket.clone(),
                self.downlink_tx.clone(),
                gateway_id,
            ));

            e.insert(ServerSocket {
                last_uplink: SystemTime::now(),
                push_data_token: None,
                pull_data_token: None,
                pull_resp_token: None,
                _stop_tx: stop_tx,
                socket,
            });
        }

        // This should never error since we check the existence of the GatewayId key above.
        let socket = self
            .sockets
            .get_mut(&gateway_id)
            .ok_or_else(|| anyhow!("Gateway ID not found"))?;

        Ok(socket)
    }
}

struct ServerSocket {
    last_uplink: SystemTime,
    _stop_tx: oneshot::Sender<()>,
    socket: Arc<UdpSocket>,
    pull_data_token: Option<u16>,
    push_data_token: Option<u16>,
    pull_resp_token: Option<u16>,
}

pub async fn setup(
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    uplink_rx: UnboundedReceiver<(GatewayId, Vec<u8>, String, Option<String>)>,
    outputs: Vec<config::GwmpOutput>,
) -> Result<()> {
    info!("Setting up forwarder");

    for output in outputs {
        let gateway_id_filters = GatewayIdFilters {
            allow: output.gateway_id_prefixes.clone(),
            deny: output.gateway_id_deny.clone(),
        };
        let allow_deny_filters = AllowDenyFilters {
            dev_addr_prefixes: output.filters.dev_addr_prefixes.clone(),
            dev_addr_deny: output.filters.dev_addr_deny.clone(),
            join_eui_prefixes: output.filters.join_eui_prefixes.clone(),
            join_eui_deny: output.filters.join_eui_deny.clone(),
        };

        // Validate relay prefix
        let relay_prefix = if !output.relay_gateway_id_prefix.is_empty() {
            if output.relay_gateway_id_prefix.len() != 8
                || !output.relay_gateway_id_prefix.chars().all(|c| c.is_ascii_hexdigit())
            {
                error!(
                    server = %output.server,
                    prefix = %output.relay_gateway_id_prefix,
                    "Invalid relay_gateway_id_prefix (must be exactly 8 hex chars), disabling"
                );
                String::new()
            } else {
                info!(
                    server = %output.server,
                    prefix = %output.relay_gateway_id_prefix,
                    "Mesh relay gateway virtualization enabled"
                );
                output.relay_gateway_id_prefix.clone()
            }
        } else {
            String::new()
        };

        add_server(
            output.server.clone(),
            output.uplink_only,
            output.gateway_id_prefixes.clone(),
            gateway_id_filters,
            lrwn_filters::Filters {
                dev_addr_prefixes: output.filters.dev_addr_prefixes.clone(),
                join_eui_prefixes: output.filters.join_eui_prefixes.clone(),
                lorawan_only: false,
            },
            allow_deny_filters,
            downlink_tx.clone(),
            relay_prefix,
        )
        .await?;
    }

    tokio::spawn(handle_uplink(uplink_rx));
    tokio::spawn(cleanup_sockets());

    Ok(())
}

async fn handle_uplink(mut uplink_rx: UnboundedReceiver<(GatewayId, Vec<u8>, String, Option<String>)>) {
    while let Some((gateway_id, data, region, relay_id)) = uplink_rx.recv().await {
        if let Err(e) = handle_uplink_packet(gateway_id, &data, &region, relay_id.as_deref()).await {
            error!(error = %e.full(), "Handle uplink error");
        }
    }
}

async fn handle_uplink_packet(gateway_id: GatewayId, data: &[u8], region: &str, relay_id: Option<&str>) -> Result<()> {
    let packet_type = PacketType::try_from(data)?;
    let random_token = get_random_token(data)?;

    // Parse PushData once up front, shared across MQTT/BS/GWMP outputs and logging.
    let parsed_push_data = if packet_type == PacketType::PushData {
        match PushData::from_slice(data) {
            Ok(pd) => Some(pd),
            Err(e) => {
                error!("Decode PushData payload error: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Extract dev_addr and fport once for logging (shared across all server iterations).
    let (dev_addr_str, fport_str) = parsed_push_data
        .as_ref()
        .and_then(|pd| pd.payload.rxpk.first())
        .map(|rxpk| {
            if rxpk.data.len() >= 5 {
                let mhdr = rxpk.data[0];
                let mtype = mhdr >> 5;
                if mtype == 2 || mtype == 4 {
                    let dev_addr = format!(
                        "{:02x}{:02x}{:02x}{:02x}",
                        rxpk.data[4], rxpk.data[3], rxpk.data[2], rxpk.data[1]
                    );
                    let fport = if rxpk.data.len() >= 13 {
                        rxpk.data[8].to_string()
                    } else {
                        "-".to_string()
                    };
                    (dev_addr, fport)
                } else {
                    ("-".to_string(), "-".to_string())
                }
            } else {
                ("-".to_string(), "-".to_string())
            }
        })
        .unwrap_or_else(|| ("-".to_string(), "-".to_string()));

    // Send to MQTT backend (if enabled) - do this once before the server loop
    // as MQTT has its own filters
    // Skip if this is an MQTT gateway - uplinks from MQTT-in are already forwarded directly
    if let Some(push_data) = &parsed_push_data {
        if mqtt::is_enabled().await && !mqtt::is_mqtt_gateway(gateway_id).await {
            if let Err(e) = mqtt::send_uplink_frame(gateway_id, push_data, region, relay_id).await {
                error!(error = %e, "MQTT send uplink error");
            }
            if let Some(stats) = push_data.to_gateway_stats() {
                if let Err(e) = mqtt::send_gateway_stats(gateway_id, &stats, region).await {
                    error!(error = %e, "MQTT send stats error");
                }
            }
        }
    }

    // Send to Basic Station outputs (if enabled)
    // Skip if this is a BS gateway - uplinks from BS-in are already forwarded
    if let Some(push_data) = &parsed_push_data {
        if basicstation::is_enabled().await && !basicstation::is_bs_gateway(gateway_id).await {
            if let Err(e) = basicstation::send_uplink_frame(gateway_id, push_data, region).await {
                error!(error = %e, "BS send uplink error");
            }
        }
    }

    let servers = SERVERS
        .get_or_init(|| async { RwLock::new(Vec::new()) })
        .await;
    let mut servers = servers.write().await;

    for server in servers.iter_mut() {
        // Determine effective gateway_id for this server (relay rewriting)
        let (effective_gw, is_relay) = if let Some(rid) = relay_id {
            if !server.relay_gateway_id_prefix.is_empty() {
                let virtual_hex = format!("{}{}", server.relay_gateway_id_prefix, rid);
                match GatewayId::from_hex(&virtual_hex) {
                    Ok(virtual_gw) => {
                        // Register mapping for downlink routing (with signal quality for best-gateway selection)
                        let (rssi, snr) = parsed_push_data.as_ref()
                            .and_then(|pd| pd.payload.rxpk.first())
                            .map(|r| (r.rssi.unwrap_or(0), r.lsnr.unwrap_or(0.0) as f32))
                            .unwrap_or((0, 0.0));
                        mqtt::register_relay_gateway(virtual_gw, gateway_id, rssi, snr).await;
                        // Register virtual gateway in MQTT_GATEWAYS for downlink routing back
                        if let Some((srv, rgn, ctx)) = mqtt::get_mqtt_gateway_info(gateway_id).await {
                            mqtt::register_mqtt_gateway_pub(virtual_gw, srv, rgn, ctx).await;
                        }
                        info!(
                            server = %server.server,
                            border_gateway = %gateway_id,
                            virtual_gateway = %virtual_gw,
                            relay_id = %rid,
                            "Rewriting gateway_id for mesh relay (UDP output)"
                        );
                        (virtual_gw, true)
                    }
                    Err(e) => {
                        warn!(relay_id = %rid, error = %e, "Invalid virtual gateway ID, using original");
                        (gateway_id, false)
                    }
                }
            } else {
                (gateway_id, false)
            }
        } else {
            (gateway_id, false)
        };

        if !server.match_gateway_id(effective_gw) {
            continue;
        }

        // Clone filters before mutable borrow of socket.
        let filters = server.filters.clone();
        let allow_deny_filters = server.allow_deny_filters.clone();
        let server_name = server.server.clone();

        let socket = server.get_server_socket(effective_gw).await?;
        socket.last_uplink = SystemTime::now();

        let span = tracing::info_span!("", addr = %socket.socket.peer_addr().unwrap());
        let _enter = span.enter();

        match packet_type {
            PacketType::PushData => {
                if let Some(push_data) = &parsed_push_data {
                    let has_payload_filters = !allow_deny_filters.dev_addr_deny.is_empty()
                        || !allow_deny_filters.join_eui_deny.is_empty()
                        || !allow_deny_filters.dev_addr_prefixes.is_empty()
                        || !allow_deny_filters.join_eui_prefixes.is_empty()
                        || !filters.dev_addr_prefixes.is_empty()
                        || !filters.join_eui_prefixes.is_empty();

                    if push_data.payload.rxpk.is_empty() {
                        // Stat-only packet (no rxpk) — forward raw bytes regardless of filters.
                        if !push_data.payload.is_empty() {
                            if is_relay {
                                // Must re-serialize with virtual gateway_id
                                let mut rewritten = push_data.clone();
                                rewritten.gateway_id = *effective_gw.as_bytes();
                                let rewritten_data = rewritten.to_bytes();
                                info!(gateway_id = %effective_gw, "Forwarding UDP stats (relay)");
                                socket.push_data_token = Some(random_token);
                                socket.socket.send(&rewritten_data).await.context("Send UDP packet")?;
                            } else {
                                info!(gateway_id = %effective_gw, "Forwarding UDP stats");
                                socket.push_data_token = Some(random_token);
                                socket.socket.send(data).await.context("Send UDP packet")?;
                            }
                            inc_server_udp_sent_count(&server_name, packet_type).await;
                        } else {
                            debug!("Nothing to send, UDP packet is empty");
                        }
                    } else if !has_payload_filters && !is_relay {
                        // No payload filters and no relay rewrite — raw passthrough.
                        info!(gateway_id = %effective_gw, dev_addr = %dev_addr_str, f_port = %fport_str, "Forwarding UDP uplink");
                        socket.push_data_token = Some(random_token);
                        socket.socket.send(data).await.context("Send UDP packet")?;
                        inc_server_udp_sent_count(&server_name, packet_type).await;
                    } else {
                        // Filters configured or relay rewrite needed — must process.
                        let original_count = push_data.payload.rxpk.len();
                        let mut filtered_push_data = push_data.clone();

                        // Apply relay gateway_id rewrite and strip relay metadata
                        if is_relay {
                            filtered_push_data.gateway_id = *effective_gw.as_bytes();
                            for rxpk in &mut filtered_push_data.payload.rxpk {
                                if let Some(ref mut meta) = rxpk.meta {
                                    meta.remove("relay_id");
                                    meta.remove("hop_count");
                                    if meta.is_empty() {
                                        rxpk.meta = None;
                                    }
                                }
                            }
                            debug!(server = %server_name, "Stripped relay metadata from uplink");
                        }

                        if !allow_deny_filters.dev_addr_deny.is_empty()
                            || !allow_deny_filters.join_eui_deny.is_empty()
                        {
                            filtered_push_data
                                .payload
                                .filter_rxpk_allow_deny(&allow_deny_filters);
                        } else if !filters.dev_addr_prefixes.is_empty()
                            || !filters.join_eui_prefixes.is_empty()
                        {
                            filtered_push_data
                                .payload
                                .filter_rxpk(&filters);
                        }

                        let remaining_count = filtered_push_data.payload.rxpk.len();

                        if remaining_count == 0 {
                            // All rxpk removed — drop.
                            debug!("Nothing to send, UDP packet does not match filters");
                        } else if remaining_count == original_count && !is_relay {
                            // All rxpk passed and no relay — send original raw bytes.
                            info!(gateway_id = %effective_gw, dev_addr = %dev_addr_str, f_port = %fport_str, "Forwarding UDP uplink");
                            socket.push_data_token = Some(random_token);
                            socket.socket.send(data).await.context("Send UDP packet")?;
                            inc_server_udp_sent_count(&server_name, packet_type).await;
                        } else {
                            // Re-serialize (filtered or relay-rewritten).
                            let filtered_data = filtered_push_data.to_bytes();
                            info!(gateway_id = %effective_gw, dev_addr = %dev_addr_str, f_port = %fport_str, "Forwarding UDP uplink (filtered)");
                            socket.push_data_token = Some(random_token);
                            socket.socket.send(&filtered_data).await.context("Send UDP packet")?;
                            inc_server_udp_sent_count(&server_name, packet_type).await;
                        }
                    }
                }
            }
            PacketType::PullData => {
                if is_relay {
                    // Send PULL_DATA with virtual gateway_id
                    let pull_data = PullData::new(&effective_gw);
                    let pd_data = pull_data.to_bytes();
                    info!(packet_type = %packet_type, gateway_id = %effective_gw, "Sending UDP packet (relay)");
                    socket.pull_data_token = Some(pull_data.random_token);
                    socket.socket.send(&pd_data).await.context("Send UDP packet")?;
                } else {
                    info!(packet_type = %packet_type, "Sending UDP packet");
                    socket.pull_data_token = Some(random_token);
                    socket.socket.send(data).await.context("Send UDP packet")?;
                }
                inc_server_udp_sent_count(&server_name, packet_type).await;
            }
            PacketType::TxAck => {
                if let Some(pull_resp_token) = socket.pull_resp_token
                    && pull_resp_token == random_token
                {
                    info!(packet_type = %packet_type, "Sending UDP packet");
                    socket.pull_resp_token = None;
                    socket.socket.send(data).await.context("Send UDP packet")?;
                    inc_server_udp_sent_count(&server_name, packet_type).await;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

async fn handle_downlink(
    server: String,
    mut stop_rx: oneshot::Receiver<()>,
    uplink_only: bool,
    socket: Arc<UdpSocket>,
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    gateway_id: GatewayId,
) {
    let mut buffer: [u8; 65535] = [0; 65535];

    loop {
        let (size, addr) = tokio::select! {
            _ = &mut stop_rx => {
                break;
            }
           v = socket.recv_from(&mut buffer) =>
                match v  {
                    Ok(v) => v,
                    Err(e) => {
                        error!(error = %e, "UDP socket receive error");
                        break;
                    },
                },
            else => {
                break;
            }
        };

        if size < 4 {
            warn!(addr = %addr, received_bytes = size, "At least 4 bytes are expected");
            continue;
        }

        if let Err(e) = handle_downlink_packet(
            &server,
            uplink_only,
            &downlink_tx,
            gateway_id,
            &buffer[..size],
        )
        .instrument(tracing::info_span!("", addr = %addr, gateway_id = %gateway_id))
        .await
        {
            error!(error = %e.full(), "Handle downlink packet error");
        }
    }

    debug!("Downlink loop has ended");
}

async fn handle_downlink_packet(
    server: &str,
    uplink_only: bool,
    downlink_tx: &UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    gateway_id: GatewayId,
    data: &[u8],
) -> Result<()> {
    let packet_type = PacketType::try_from(data)?;
    let token = get_random_token(data)?;

    info!(packet_type = %packet_type, "Received UDP packet");

    inc_server_udp_received_count(server, packet_type).await;

    match packet_type {
        PacketType::PullResp => {
            if uplink_only {
                warn!("Dropping downlink, server is configured as uplink-only");
            } else {
                set_pull_resp_token(server, gateway_id, token).await?;
                handle_pull_resp(downlink_tx, gateway_id, data).await?;
            }
        }
        PacketType::PullAck => {
            // TODO: keep ack stats
        }
        PacketType::PushAck => {
            // TODO: keep ack stats
        }

        _ => {}
    }

    Ok(())
}

async fn handle_pull_resp(
    downlink_tx: &UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    gateway_id: GatewayId,
    data: &[u8],
) -> Result<()> {
    debug!("Sending received data to downlink channel");
    // PULL_RESP from UDP server doesn't have a separate downlink_id, use None
    downlink_tx
        .send((gateway_id, data.to_vec(), None))
        .context("Downlink channel send")?;

    Ok(())
}

async fn add_server(
    server: String,
    uplink_only: bool,
    gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    gateway_id_filters: GatewayIdFilters,
    filters: lrwn_filters::Filters,
    allow_deny_filters: AllowDenyFilters,
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    relay_gateway_id_prefix: String,
) -> Result<()> {
    info!(
        server = server,
        uplink_only = uplink_only,
        gateway_id_prefixes = ?gateway_id_prefixes,
        relay_gateway_id_prefix = %relay_gateway_id_prefix,
        "Adding server"
    );

    let servers = SERVERS
        .get_or_init(|| async { RwLock::new(Vec::new()) })
        .await;

    let mut servers = servers.write().await;
    servers.push(Server {
        server,
        uplink_only,
        gateway_id_prefixes,
        gateway_id_filters,
        filters,
        allow_deny_filters,
        downlink_tx,
        sockets: HashMap::new(),
        relay_gateway_id_prefix,
    });

    Ok(())
}

async fn cleanup_sockets() {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

        trace!("Cleaning up inactive sockets");

        let servers = SERVERS
            .get_or_init(|| async { RwLock::new(Vec::new()) })
            .await;
        let mut servers = servers.write().await;

        for server in servers.iter_mut() {
            server.sockets.retain(|k, v| {
                if let Ok(duration) = SystemTime::now().duration_since(v.last_uplink) {
                    if duration < Duration::from_secs(60) {
                        true
                    } else {
                        warn!(server = server.server, gateway_id = %k, "Cleaning up inactive socket");
                        false
                    }
                } else {
                    warn!(server = server.server, gateway_id = %k, "Cleaning up inactive socket");
                    false
                }
            });
        }
    }
}

async fn set_pull_resp_token(srv: &str, gateway_id: GatewayId, token: u16) -> Result<()> {
    let servers = SERVERS
        .get_or_init(|| async { RwLock::new(Vec::new()) })
        .await;
    let mut servers = servers.write().await;

    for server in servers.iter_mut() {
        if server.server.eq(srv)
            && let Some(v) = server.sockets.get_mut(&gateway_id)
        {
            v.pull_resp_token = Some(token);
        }
    }

    Ok(())
}

/// Send PULL_DATA to all UDP servers for an MQTT-in gateway.
/// This registers the gateway with the servers so they can send downlinks.
pub async fn send_pull_data_for_gateway(gateway_id: GatewayId) -> Result<()> {
    let servers = SERVERS
        .get_or_init(|| async { RwLock::new(Vec::new()) })
        .await;
    let mut servers = servers.write().await;

    for server in servers.iter_mut() {
        if !server.match_gateway_id(gateway_id) {
            continue;
        }

        // Create PULL_DATA packet
        let pull_data = PullData::new(&gateway_id);
        let data = pull_data.to_bytes();
        let token = pull_data.random_token;
        let server_name = server.server.clone();

        // Get or create socket for this gateway
        let socket = server.get_server_socket(gateway_id).await?;

        info!(
            gateway_id = %gateway_id,
            server = %server_name,
            token = token,
            "Sending synthetic PULL_DATA for MQTT-in gateway"
        );

        socket.pull_data_token = Some(token);
        socket.socket.send(&data).await.context("Send PULL_DATA")?;
        inc_server_udp_sent_count(&server_name, PacketType::PullData).await;
    }

    Ok(())
}

/// Send TX_ACK to the UDP server that sent the corresponding PULL_RESP.
/// This is used for MQTT-in gateways that send TX_ACK via MQTT.
pub async fn send_tx_ack_for_gateway(gateway_id: GatewayId, tx_ack: &TxAck) -> Result<()> {
    let servers = SERVERS
        .get_or_init(|| async { RwLock::new(Vec::new()) })
        .await;
    let mut servers = servers.write().await;

    let token = tx_ack.random_token;
    let data = tx_ack.to_bytes();

    for server in servers.iter_mut() {
        if !server.match_gateway_id(gateway_id) {
            continue;
        }

        if let Some(socket) = server.sockets.get_mut(&gateway_id) {
            // Check if this server sent a PULL_RESP with a matching token
            if let Some(pull_resp_token) = socket.pull_resp_token {
                if pull_resp_token == token {
                    info!(
                        server = %server.server,
                        gateway_id = %gateway_id,
                        "Forwarding TX_ACK to UDP"
                    );

                    socket.pull_resp_token = None;
                    socket.socket.send(&data).await.context("Send TX_ACK")?;
                    inc_server_udp_sent_count(&server.server, PacketType::TxAck).await;
                    return Ok(());
                }
            }
        }
    }

    debug!(
        gateway_id = %gateway_id,
        token = token,
        "No matching PULL_RESP token found for TX_ACK"
    );

    Ok(())
}
