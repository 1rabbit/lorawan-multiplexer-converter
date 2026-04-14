use std::fs;
use std::io::{BufReader, Cursor};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chirpstack_api::gw;
use prost::Message;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS, Transport};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::{OnceCell, RwLock};
use tracing::{debug, error, info, trace, warn};
use url::Url;

use crate::config;
use crate::filters::{AllowDenyFilters, GatewayIdFilters};
use crate::forwarder;
use crate::monitoring::{
    inc_mqtt_connected, inc_mqtt_messages_published, inc_mqtt_messages_received,
};
use crate::packets::{GatewayId, PushData, PullResp, RxPk, downlink_frame_to_pull_resp, downlink_tx_ack_to_tx_ack};

static STATES: OnceCell<RwLock<Vec<State>>> = OnceCell::const_new();

/// Track which gateways connected via MQTT-in (server URL as value)
static MQTT_GATEWAYS: OnceCell<RwLock<std::collections::HashMap<GatewayId, MqttGatewayInfo>>> = OnceCell::const_new();

/// Information about an MQTT-in gateway
struct MqttGatewayInfo {
    server: String,
    /// Region/topic prefix from the gateway's MQTT topic
    region: String,
    /// Last time PULL_DATA was sent for this gateway
    last_pull_data: std::time::Instant,
    /// Original rx_info.context from the last uplink (opaque bytes from the gateway bridge)
    last_context: Vec<u8>,
    /// Extracted tmst (concentrator counter) from the last uplink context
    last_tmst: Option<u32>,
}

/// Interval for sending PULL_DATA keepalives (5 seconds, matching typical gateway behavior)
const PULL_DATA_INTERVAL: Duration = Duration::from_secs(5);

/// Maps virtual relay gateway IDs to their real border gateway for downlink routing.
static RELAY_GATEWAYS: OnceCell<RwLock<std::collections::HashMap<GatewayId, RelayGatewayInfo>>> = OnceCell::const_new();

struct RelayGatewayInfo {
    border_gateway_id: GatewayId,
    last_seen: std::time::Instant,
}

struct State {
    client: AsyncClient,
    qos: QoS,
    json: bool,
    server: String,
    #[allow(dead_code)]
    uplink_only: bool,
    /// If true, this backend subscribes to uplinks (MQTT-in) and should not receive published uplinks
    /// to avoid feedback loops.
    subscribe_uplinks: bool,
    /// If true, this is an output-only backend for traffic analyzers (no subscriptions).
    analyzer: bool,
    /// If true, this backend receives forwarded application/# messages from subscribe_application sources.
    forward_application: bool,
    gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    gateway_id_filters: GatewayIdFilters,
    filters: lrwn_filters::Filters,
    allow_deny_filters: AllowDenyFilters,
    /// Mesh relay virtual gateway prefix (8 hex chars). Empty = disabled.
    relay_gateway_id_prefix: String,
}

impl State {
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
}

/// Setup all configured MQTT backends.
pub async fn setup(
    config: &config::MqttConfig,
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    uplink_tx: UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
) -> Result<()> {
    // Initialize the states vector
    STATES
        .get_or_init(|| async { RwLock::new(Vec::new()) })
        .await;

    for conf in &config.inputs {
        setup_input(conf, downlink_tx.clone(), uplink_tx.clone()).await?;
    }

    for conf in &config.outputs {
        setup_output(conf, downlink_tx.clone()).await?;
    }

    // Spawn relay gateway cleanup task
    tokio::spawn(cleanup_relay_gateways());

    Ok(())
}

/// Setup a single MQTT input (subscribes to uplinks).
async fn setup_input(
    conf: &config::MqttInput,
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    uplink_tx: UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
) -> Result<()> {
    info!(server = %conf.server, "Setting up MQTT input");

    let qos = parse_qos(conf.qos)?;
    let (connect_tx, connect_rx) = unbounded_channel::<()>();
    let (client, eventloop) = create_mqtt_client(
        &conf.server, &conf.client_id, conf.keep_alive_interval, conf.clean_session,
        &conf.username, &conf.password, &conf.ca_cert, &conf.tls_cert, &conf.tls_key,
    )?;

    let filters = lrwn_filters::Filters {
        dev_addr_prefixes: conf.filters.dev_addr_prefixes.clone(),
        join_eui_prefixes: conf.filters.join_eui_prefixes.clone(),
        lorawan_only: false,
    };
    let allow_deny_filters = AllowDenyFilters {
        dev_addr_prefixes: conf.filters.dev_addr_prefixes.clone(),
        dev_addr_deny: conf.filters.dev_addr_deny.clone(),
        join_eui_prefixes: conf.filters.join_eui_prefixes.clone(),
        join_eui_deny: conf.filters.join_eui_deny.clone(),
    };
    let gateway_id_filters = GatewayIdFilters {
        allow: conf.gateway_id_prefixes.clone(),
        deny: conf.gateway_id_deny.clone(),
    };

    let state = State {
        client,
        qos,
        json: conf.json,
        server: conf.server.clone(),
        uplink_only: false,
        subscribe_uplinks: true,
        analyzer: false,
        forward_application: false,
        gateway_id_prefixes: conf.gateway_id_prefixes.clone(),
        gateway_id_filters: gateway_id_filters.clone(),
        filters: filters.clone(),
        allow_deny_filters: allow_deny_filters.clone(),
        relay_gateway_id_prefix: String::new(), // Inputs don't do relay rewriting
    };

    {
        let states = STATES.get().ok_or_else(|| anyhow!("STATES not initialized"))?;
        states.write().await.push(state);
    }

    let server_for_subscribe = conf.server.clone();
    tokio::spawn(subscribe_loop(
        server_for_subscribe, qos,
        false,  // uplink_only
        true,   // subscribe_uplinks
        false,  // analyzer
        false,  // subscribe_application
        connect_rx,
    ));

    let server = conf.server.clone();
    let json = conf.json;
    let reconnect_interval = conf.reconnect_interval;
    tokio::spawn(event_loop(
        eventloop, connect_tx, downlink_tx, uplink_tx,
        reconnect_interval, server, true, json,
        filters, allow_deny_filters, gateway_id_filters,
    ));

    Ok(())
}

/// Setup a single MQTT output (publishes uplinks, subscribes to downlinks).
async fn setup_output(
    conf: &config::MqttOutput,
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
) -> Result<()> {
    info!(server = %conf.server, analyzer = conf.analyzer, subscribe_application = conf.subscribe_application, forward_application = conf.forward_application, "Setting up MQTT output");

    let qos = parse_qos(conf.qos)?;
    let (connect_tx, connect_rx) = unbounded_channel::<()>();
    let (client, eventloop) = create_mqtt_client(
        &conf.server, &conf.client_id, conf.keep_alive_interval, conf.clean_session,
        &conf.username, &conf.password, &conf.ca_cert, &conf.tls_cert, &conf.tls_key,
    )?;

    let filters = lrwn_filters::Filters {
        dev_addr_prefixes: conf.filters.dev_addr_prefixes.clone(),
        join_eui_prefixes: conf.filters.join_eui_prefixes.clone(),
        lorawan_only: false,
    };
    let allow_deny_filters = AllowDenyFilters {
        dev_addr_prefixes: conf.filters.dev_addr_prefixes.clone(),
        dev_addr_deny: conf.filters.dev_addr_deny.clone(),
        join_eui_prefixes: conf.filters.join_eui_prefixes.clone(),
        join_eui_deny: conf.filters.join_eui_deny.clone(),
    };
    let gateway_id_filters = GatewayIdFilters {
        allow: conf.gateway_id_prefixes.clone(),
        deny: conf.gateway_id_deny.clone(),
    };

    // Validate relay prefix if set
    let relay_prefix = if !conf.relay_gateway_id_prefix.is_empty() {
        if conf.relay_gateway_id_prefix.len() != 8
            || !conf.relay_gateway_id_prefix.chars().all(|c| c.is_ascii_hexdigit())
        {
            error!(
                server = %conf.server,
                prefix = %conf.relay_gateway_id_prefix,
                "Invalid relay_gateway_id_prefix (must be exactly 8 hex chars), disabling"
            );
            String::new()
        } else {
            info!(
                server = %conf.server,
                prefix = %conf.relay_gateway_id_prefix,
                "Mesh relay gateway virtualization enabled"
            );
            conf.relay_gateway_id_prefix.clone()
        }
    } else {
        String::new()
    };

    let state = State {
        client,
        qos,
        json: conf.json,
        server: conf.server.clone(),
        uplink_only: conf.uplink_only,
        subscribe_uplinks: false,
        analyzer: conf.analyzer,
        forward_application: conf.forward_application,
        gateway_id_prefixes: conf.gateway_id_prefixes.clone(),
        gateway_id_filters: gateway_id_filters.clone(),
        filters: filters.clone(),
        allow_deny_filters: allow_deny_filters.clone(),
        relay_gateway_id_prefix: relay_prefix,
    };

    {
        let states = STATES.get().ok_or_else(|| anyhow!("STATES not initialized"))?;
        states.write().await.push(state);
    }

    let server_for_subscribe = conf.server.clone();
    let subscribe_application = conf.subscribe_application;
    let analyzer = conf.analyzer;
    let uplink_only = conf.uplink_only;
    tokio::spawn(subscribe_loop(
        server_for_subscribe, qos,
        uplink_only,
        false, // subscribe_uplinks
        analyzer,
        subscribe_application,
        connect_rx,
    ));

    // Outputs don't produce uplinks, but event_loop still needs the channel types.
    // We create a dummy uplink_tx that is never read.
    let (dummy_uplink_tx, _) = unbounded_channel::<(GatewayId, Vec<u8>, String, Option<String>)>();
    let server = conf.server.clone();
    let json = conf.json;
    let reconnect_interval = conf.reconnect_interval;
    tokio::spawn(event_loop(
        eventloop, connect_tx, downlink_tx, dummy_uplink_tx,
        reconnect_interval, server, false, json,
        filters, allow_deny_filters, gateway_id_filters,
    ));

    Ok(())
}

fn parse_qos(qos: u8) -> Result<QoS> {
    match qos {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        _ => Err(anyhow!("Invalid QoS value: {}", qos)),
    }
}

fn create_mqtt_client(
    server: &str, client_id: &str, keep_alive: Duration, clean_session: bool,
    username: &str, password: &str,
    ca_cert: &str, tls_cert: &str, tls_key: &str,
) -> Result<(AsyncClient, EventLoop)> {
    let url = Url::parse(server).context("Parse MQTT server URL")?;
    let host = url.host_str().ok_or_else(|| anyhow!("Missing host in MQTT URL"))?;
    let port = url.port().unwrap_or(match url.scheme() {
        "ssl" | "tls" | "mqtts" | "wss" => 8883,
        _ => 1883,
    });

    let cid = if client_id.is_empty() {
        format!(
            "lorawan-multiplexer-converter-{}",
            hex::encode(rand::random::<[u8; 4]>())
        )
    } else {
        client_id.to_string()
    };

    let mut mqtt_options = MqttOptions::new(&cid, host, port);
    mqtt_options.set_keep_alive(keep_alive);
    mqtt_options.set_clean_session(clean_session);

    if !username.is_empty() {
        mqtt_options.set_credentials(username, password);
    }

    let scheme = url.scheme();
    if scheme == "ssl" || scheme == "tls" || scheme == "mqtts" || scheme == "wss" {
        let tls_config = build_tls_config(ca_cert, tls_cert, tls_key)?;
        mqtt_options.set_transport(Transport::tls_with_config(
            rumqttc::TlsConfiguration::Rustls(Arc::new(tls_config)),
        ));
    } else if scheme == "ws" {
        mqtt_options.set_transport(Transport::Ws);
    }

    Ok(AsyncClient::new(mqtt_options, 100))
}

pub fn build_tls_config(ca_cert: &str, tls_cert: &str, tls_key: &str) -> Result<rustls::ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();

    if !ca_cert.is_empty() {
        let ca_data = fs::read(ca_cert).context("Read CA certificate")?;
        let mut reader = BufReader::new(Cursor::new(ca_data));
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .context("Parse CA certificates")?;
        for cert in certs {
            root_store.add(cert).context("Add CA certificate")?;
        }
    } else {
        let native_certs = rustls_native_certs::load_native_certs();
        for cert in native_certs.certs {
            root_store.add(cert).ok();
        }
    }

    let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

    if !tls_cert.is_empty() && !tls_key.is_empty() {
        let cert = fs::read(tls_cert).context("Read TLS certificate")?;
        let key = fs::read(tls_key).context("Read TLS key")?;

        let mut cert_reader = BufReader::new(Cursor::new(cert));
        let certs = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .context("Parse TLS certificates")?;

        let mut key_reader = BufReader::new(Cursor::new(key));
        let key = rustls_pemfile::private_key(&mut key_reader)
            .context("Parse TLS key")?
            .ok_or_else(|| anyhow!("No private key found"))?;

        Ok(builder.with_client_auth_cert(certs, key)?)
    } else {
        Ok(builder.with_no_client_auth())
    }
}

async fn subscribe_loop(
    server: String,
    qos: QoS,
    uplink_only: bool,
    subscribe_uplinks: bool,
    analyzer: bool,
    subscribe_application: bool,
    mut connect_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
) {
    // Analyzer backends are output-only, no subscriptions needed
    if analyzer {
        info!(server = %server, "Analyzer mode, not subscribing to any topics");
        return;
    }

    while connect_rx.recv().await.is_some() {
        let states = match STATES.get() {
            Some(s) => s,
            None => continue,
        };

        let states = states.read().await;
        let state = match states.iter().find(|s| s.server == server) {
            Some(s) => s,
            None => continue,
        };

        // Subscribe to downlink commands (if not uplink-only, not MQTT-in, and not analyzer)
        if !uplink_only && !subscribe_uplinks && !analyzer {
            let topic = "+/gateway/+/command/down";
            info!(server = %server, topic = %topic, "Subscribing to MQTT downlink topic");

            if let Err(e) = state.client.subscribe(topic, qos).await {
                error!(error = %e, "MQTT subscribe error");
            }
        } else if uplink_only {
            info!(server = %server, "MQTT uplink-only mode, not subscribing to downlink topics");
        }

        // Subscribe to events from MQTT (MQTT-in support)
        if subscribe_uplinks {
            let events_topic = "+/gateway/+/event/+";
            info!(server = %server, topic = %events_topic, "Subscribing to MQTT event topics (MQTT-in)");

            if let Err(e) = state.client.subscribe(events_topic, qos).await {
                error!(error = %e, "MQTT subscribe error");
            }
        }

        // Subscribe to application/# for forwarding to forward_application outputs
        if subscribe_application {
            let app_topic = "application/#";
            info!(server = %server, topic = %app_topic, "Subscribing to application topics for forwarding");

            if let Err(e) = state.client.subscribe(app_topic, qos).await {
                error!(error = %e, "MQTT subscribe error");
            }
        }
    }
}

async fn event_loop(
    mut eventloop: EventLoop,
    connect_tx: tokio::sync::mpsc::UnboundedSender<()>,
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    uplink_tx: UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    reconnect_interval: Duration,
    server: String,
    subscribe_uplinks: bool,
    json: bool,
    filters: lrwn_filters::Filters,
    allow_deny_filters: AllowDenyFilters,
    gateway_id_filters: GatewayIdFilters,
) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(v))) => {
                info!(server = %server, session_present = v.session_present, "MQTT connected");
                inc_mqtt_connected(&server, true).await;
                if connect_tx.send(()).is_err() {
                    error!("Connect channel send error");
                }
            }
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                trace!(topic = %p.topic, "MQTT message received");
                let downlink_tx = downlink_tx.clone();
                let uplink_tx = uplink_tx.clone();
                let server = server.clone();
                let filters = filters.clone();
                let allow_deny_filters = allow_deny_filters.clone();
                let gateway_id_filters = gateway_id_filters.clone();
                tokio::spawn(async move {
                    if let Err(e) = message_callback(
                        &server,
                        &p.topic,
                        &p.payload,
                        &downlink_tx,
                        &uplink_tx,
                        subscribe_uplinks,
                        json,
                        &filters,
                        &allow_deny_filters,
                        &gateway_id_filters,
                    )
                    .await
                    {
                        error!(error = %e, "MQTT message callback error");
                    }
                });
            }
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                warn!(server = %server, "MQTT disconnected");
                inc_mqtt_connected(&server, false).await;
            }
            Ok(_) => {}
            Err(e) => {
                error!(server = %server, error = %e, "MQTT error");
                inc_mqtt_connected(&server, false).await;
                tokio::time::sleep(reconnect_interval).await;
            }
        }
    }
}

async fn message_callback(
    server: &str,
    original_topic: &str,
    payload: &[u8],
    downlink_tx: &UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    subscribe_uplinks: bool,
    json: bool,
    filters: &lrwn_filters::Filters,
    allow_deny_filters: &AllowDenyFilters,
    gateway_id_filters: &GatewayIdFilters,
) -> Result<()> {
    // Check if this is an application/... topic (subscribe_application)
    if original_topic.starts_with("application/") {
        handle_application_message(server, original_topic, payload).await;
        return Ok(());
    }

    // Parse topic: {prefix}/gateway/{gw_id}/{type}/{cmd}
    // Find "gateway" segment by position
    let parts: Vec<&str> = original_topic.split('/').collect();

    // Find the "gateway" segment — it may be at index 0 (prefix stripped or no prefix)
    // or index 1+ (unknown prefix from wildcard subscription)
    let gw_idx = match parts.iter().position(|&p| p == "gateway") {
        Some(idx) if parts.len() >= idx + 4 => idx,
        _ => {
            debug!(topic = %original_topic, "Ignoring message on unknown topic");
            return Ok(());
        }
    };

    let gateway_id_str = parts[gw_idx + 1];
    let msg_type = parts[gw_idx + 2];
    let msg_subtype = parts[gw_idx + 3];

    match (msg_type, msg_subtype) {
        ("command", "down") => {
            handle_downlink_message(server, gateway_id_str, payload, original_topic, downlink_tx).await
        }
        ("event", "up") if subscribe_uplinks => {
            handle_uplink_message(server, gateway_id_str, payload, original_topic, uplink_tx, json, filters, allow_deny_filters, gateway_id_filters).await
        }
        ("event", _) if subscribe_uplinks => {
            // Pass through other event types (stats, ack, exec, etc.) without filtering
            handle_event_passthrough(server, gateway_id_str, payload, original_topic, msg_subtype, gateway_id_filters, json).await
        }
        _ => {
            debug!(msg_type = %msg_type, msg_subtype = %msg_subtype, "Ignoring unknown message type");
            Ok(())
        }
    }
}

async fn handle_downlink_message(
    server: &str,
    gateway_id_str: &str,
    payload: &[u8],
    original_topic: &str,
    downlink_tx: &UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
) -> Result<()> {
    inc_mqtt_messages_received(server, "down").await;

    let gateway_id = GatewayId::from_hex(gateway_id_str)?;

    // Try to decode as protobuf first, then JSON
    let frame: gw::DownlinkFrame = if let Ok(frame) = gw::DownlinkFrame::decode(payload) {
        frame
    } else if let Ok(frame) = serde_json::from_slice(payload) {
        frame
    } else {
        return Err(anyhow!("Failed to decode downlink frame"));
    };

    info!(
        server = %server,
        gateway_id = %gateway_id,
        downlink_id = ?frame.downlink_id,
        "Received MQTT downlink"
    );

    // Check if this is an MQTT gateway (or a virtual relay gateway) - if so, forward via MQTT
    let is_mqtt = is_mqtt_gateway(gateway_id).await;
    let is_relay = !is_mqtt && resolve_relay_gateway(gateway_id).await.is_some();

    if is_mqtt || is_relay {
        send_downlink_frame_direct(gateway_id, payload, original_topic).await?;
    } else {
        // Convert to PULL_RESP for UDP gateways
        let pull_resp = downlink_frame_to_pull_resp(&frame, &gateway_id)?;

        // Send to downlink channel with the real downlink_id for TX_ACK correlation
        downlink_tx
            .send((gateway_id, pull_resp, Some(frame.downlink_id)))
            .context("Downlink channel send")?;
    }

    // Republish the command to analyzer output backends
    publish_command_to_analyzer_backends(server, gateway_id, original_topic, payload).await;

    Ok(())
}

/// Republish a downlink command to analyzer output backends.
/// Skips the originating backend (by server URL) and MQTT-in backends.
/// Publishes the exact original topic and payload without modification.
async fn publish_command_to_analyzer_backends(
    origin_server: &str,
    gateway_id: GatewayId,
    original_topic: &str,
    original_payload: &[u8],
) {
    let states = match STATES.get() {
        Some(s) => s,
        None => return,
    };

    let states = states.read().await;

    for state in states.iter() {
        // Only publish to analyzer output backends
        if !state.analyzer {
            continue;
        }

        // Skip the originating backend to avoid echoing back
        if state.server == origin_server {
            continue;
        }

        // Check gateway ID filters
        if !state.match_gateway_id(gateway_id) {
            debug!(
                gateway_id = %gateway_id,
                server = %state.server,
                "Gateway ID does not match filters for analyzer"
            );
            continue;
        }

        info!(
            server = %state.server,
            gateway_id = %gateway_id,
            topic = %original_topic,
            "Publishing downlink command to analyzer backend"
        );

        if let Err(e) = state
            .client
            .publish(original_topic, state.qos, false, original_payload)
            .await
        {
            error!(server = %state.server, error = %e, "Failed to publish downlink command");
        }

        inc_mqtt_messages_published(&state.server, "down").await;
    }
}

/// Handle an application/... message received from a broker with subscribe_application=true.
/// Republishes the payload as-is to all outputs with forward_application=true.
async fn handle_application_message(origin_server: &str, topic: &str, payload: &[u8]) {
    let states = match STATES.get() {
        Some(s) => s,
        None => return,
    };

    let states = states.read().await;

    for state in states.iter() {
        // Only forward to outputs with forward_application enabled
        if !state.forward_application {
            continue;
        }

        // Skip the originating backend
        if state.server == origin_server {
            continue;
        }

        info!(
            server = %state.server,
            topic = %topic,
            payload_len = payload.len(),
            "Forwarding application message to forward_application backend"
        );

        if let Err(e) = state.client.publish(topic, state.qos, false, payload).await {
            error!(server = %state.server, error = %e, "Failed to publish application message");
        }
    }
}

async fn handle_uplink_message(
    server: &str,
    gateway_id_str: &str,
    payload: &[u8],
    original_topic: &str,
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    _json: bool,
    filters: &lrwn_filters::Filters,
    allow_deny_filters: &AllowDenyFilters,
    gateway_id_filters: &GatewayIdFilters,
) -> Result<()> {
    inc_mqtt_messages_received(server, "up").await;

    let gateway_id = GatewayId::from_hex(gateway_id_str)?;

    // Check gateway ID filters (deny list)
    if !gateway_id_filters.allow.is_empty() || !gateway_id_filters.deny.is_empty() {
        let gw_id_le = gateway_id.as_bytes_le();
        if !gateway_id_filters.matches(gw_id_le) {
            debug!(
                gateway_id = %gateway_id,
                server = %server,
                "MQTT-in uplink filtered out by gateway ID filters"
            );
            return Ok(());
        }
    }

    // Try to decode as protobuf first, then JSON
    let frame: gw::UplinkFrame = if let Ok(frame) = gw::UplinkFrame::decode(payload) {
        frame
    } else if let Ok(frame) = serde_json::from_slice(payload) {
        frame
    } else {
        return Err(anyhow!("Failed to decode uplink frame"));
    };

    // Apply filters to the uplink frame's PHY payload
    if !frame.phy_payload.is_empty() {
        // Check against allow/deny filters
        if !allow_deny_filters.dev_addr_deny.is_empty()
            || !allow_deny_filters.join_eui_deny.is_empty()
        {
            if !allow_deny_filters.matches(&frame.phy_payload) {
                debug!(
                    gateway_id = %gateway_id,
                    server = %server,
                    "MQTT-in uplink filtered out by allow/deny filters"
                );
                return Ok(());
            }
        } else if !filters.dev_addr_prefixes.is_empty() || !filters.join_eui_prefixes.is_empty() {
            // Use legacy filters when no deny lists are configured
            if !lrwn_filters::matches(&frame.phy_payload, filters) {
                debug!(
                    gateway_id = %gateway_id,
                    server = %server,
                    "MQTT-in uplink filtered out by filters"
                );
                return Ok(());
            }
        }
    }

    // Parse phy_payload to extract mtype, dev_addr, and fport
    let phy_len = frame.phy_payload.len();
    let mhdr = if phy_len > 0 { frame.phy_payload[0] } else { 0 };
    let mtype = mhdr >> 5;

    let (dev_addr_debug, fport_debug) = if phy_len >= 5 && (mtype == 2 || mtype == 4) {
        let dev_addr = format!("{:02x}{:02x}{:02x}{:02x}",
            frame.phy_payload[4], frame.phy_payload[3], frame.phy_payload[2], frame.phy_payload[1]);
        // FPort is at byte 8 (after MHDR[1] + DevAddr[4] + FCtrl[1] + FCnt[2])
        // Minimum frame with FPort: 13 bytes (includes MIC)
        let fport = if phy_len >= 13 {
            frame.phy_payload[8].to_string()
        } else {
            "-".to_string()
        };
        (dev_addr, fport)
    } else if mtype == 0 {
        ("join-req".to_string(), "-".to_string())
    } else {
        (format!("mtype={}", mtype), "-".to_string())
    };

    info!(
        server = %server,
        gateway_id = %gateway_id,
        dev_addr = %dev_addr_debug,
        f_port = %fport_debug,
        "Received MQTT uplink"
    );

    // Extract region (topic_prefix) from original_topic: "{region}/gateway/..."
    let region = extract_region_from_topic(original_topic);

    // Extract original rx_info.context for downlink routing (preserves gateway bridge's opaque context)
    let uplink_context = frame.rx_info.as_ref().map(|r| r.context.clone()).unwrap_or_default();

    // Register the border gateway as an MQTT-in gateway for downlink routing
    register_mqtt_gateway(gateway_id, server.to_string(), region.clone(), uplink_context).await;

    // Extract relay_id from metadata (present on mesh relay uplinks from ChirpStack)
    let relay_id: Option<String> = frame.rx_info.as_ref()
        .and_then(|rx| rx.metadata.get("relay_id"))
        .cloned();

    if let Some(ref rid) = relay_id {
        info!(
            server = %server,
            gateway_id = %gateway_id,
            relay_id = %rid,
            "Detected mesh relay uplink"
        );
    }

    // For relay packets, use per-backend path so each MQTT output can apply its own relay prefix.
    // For normal packets, use raw passthrough for efficiency.
    if relay_id.is_some() {
        let rxpk = uplink_frame_to_rxpk(&frame)?;
        let push_data_obj = PushData {
            protocol_version: 0x02,
            random_token: rand::random(),
            gateway_id: *gateway_id.as_bytes(),
            payload: crate::packets::PushDataPayload::new(vec![rxpk]),
        };
        send_uplink_frame(gateway_id, &push_data_obj, &region, relay_id.as_deref()).await?;
    } else {
        send_uplink_frame_direct(gateway_id, payload, original_topic, &frame.phy_payload).await?;
    }

    // Skip UDP forwarding for downlink frames (mtype 3 = Unconfirmed Data Down, mtype 5 = Confirmed Data Down)
    // These don't make sense to convert to UDP PUSH_DATA
    if mtype == 3 || mtype == 5 {
        debug!(
            gateway_id = %gateway_id,
            mtype = mtype,
            "Skipping UDP conversion for downlink frame"
        );
        return Ok(());
    }

    // Convert UplinkFrame to synthetic PushData packet for UDP forwarding
    let push_data = uplink_frame_to_push_data(&frame, &gateway_id)?;

    // Send to uplink channel (for UDP servers), with relay_id for virtual gateway rewriting
    uplink_tx
        .send((gateway_id, push_data, region, relay_id))
        .context("Uplink channel send")?;

    Ok(())
}

/// Handle pass-through of other event types (stats, ack, exec, etc.) from MQTT-in.
/// These are forwarded as-is without filtering (except gateway_id filters).
/// TX_ACK events are also forwarded to UDP servers.
async fn handle_event_passthrough(
    server: &str,
    gateway_id_str: &str,
    payload: &[u8],
    original_topic: &str,
    event_type: &str,
    gateway_id_filters: &GatewayIdFilters,
    json: bool,
) -> Result<()> {
    inc_mqtt_messages_received(server, event_type).await;

    let gateway_id = GatewayId::from_hex(gateway_id_str)?;

    // Check gateway ID filters (deny list)
    if !gateway_id_filters.allow.is_empty() || !gateway_id_filters.deny.is_empty() {
        let gw_id_le = gateway_id.as_bytes_le();
        if !gateway_id_filters.matches(gw_id_le) {
            debug!(
                gateway_id = %gateway_id,
                event_type = %event_type,
                server = %server,
                "MQTT-in event filtered out by gateway ID filters"
            );
            return Ok(());
        }
    }

    info!(
        server = %server,
        gateway_id = %gateway_id,
        event_type = %event_type,
        "Received MQTT event"
    );

    // Extract region from topic and register this gateway as an MQTT-in gateway for downlink routing
    // Event passthrough doesn't have uplink context, pass empty (won't overwrite existing context)
    let region = extract_region_from_topic(original_topic);
    register_mqtt_gateway(gateway_id, server.to_string(), region, Vec::new()).await;

    // Forward directly to MQTT backends (pass-through)
    send_event_passthrough(gateway_id, payload, original_topic, event_type).await?;

    // For TX_ACK events, also forward to UDP servers
    if event_type == "ack" {
        handle_mqtt_tx_ack(server, gateway_id, payload, json).await?;
    }

    Ok(())
}

/// Handle TX_ACK from MQTT-in gateway and forward to UDP servers.
async fn handle_mqtt_tx_ack(server: &str, gateway_id: GatewayId, payload: &[u8], json: bool) -> Result<()> {
    // Try to decode as protobuf first, then JSON
    let ack: gw::DownlinkTxAck = if json {
        if let Ok(ack) = serde_json::from_slice(payload) {
            ack
        } else if let Ok(ack) = gw::DownlinkTxAck::decode(payload) {
            ack
        } else {
            debug!(gateway_id = %gateway_id, "Failed to decode TX_ACK as JSON or protobuf");
            return Ok(());
        }
    } else {
        if let Ok(ack) = gw::DownlinkTxAck::decode(payload) {
            ack
        } else if let Ok(ack) = serde_json::from_slice(payload) {
            ack
        } else {
            debug!(gateway_id = %gateway_id, "Failed to decode TX_ACK as protobuf or JSON");
            return Ok(());
        }
    };

    // Log TX_ACK item statuses for debugging
    for (i, item) in ack.items.iter().enumerate() {
        let status = gw::TxAckStatus::try_from(item.status)
            .unwrap_or(gw::TxAckStatus::Ignored);
        info!(
            server = %server,
            gateway_id = %gateway_id,
            downlink_id = ack.downlink_id,
            item_index = i,
            status = ?status,
            "MQTT TX_ACK item status"
        );
    }

    // Convert to UDP TX_ACK format
    // The token needs to match the PULL_RESP token - use downlink_id as the token
    let token = (ack.downlink_id & 0xFFFF) as u16;
    let tx_ack = downlink_tx_ack_to_tx_ack(&ack, &gateway_id, token);

    info!(
        server = %server,
        gateway_id = %gateway_id,
        downlink_id = ack.downlink_id,
        "Forwarding MQTT TX_ACK"
    );

    // Forward to UDP server
    if let Err(e) = forwarder::send_tx_ack_for_gateway(gateway_id, &tx_ack).await {
        error!(gateway_id = %gateway_id, error = %e, "Failed to forward TX_ACK to UDP server");
    }

    // Forward dntxed to BS outputs that originated the downlink.
    if let Err(e) = crate::basicstation::send_tx_ack(gateway_id, &ack, "").await {
        error!(gateway_id = %gateway_id, error = %e, "Failed to forward TX_ACK to BS output");
    }

    Ok(())
}

/// Send an event (stats, ack, exec, etc.) directly to MQTT backends (pass-through).
async fn send_event_passthrough(gateway_id: GatewayId, payload: &[u8], original_topic: &str, event_type: &str) -> Result<()> {
    let states = match STATES.get() {
        Some(s) => s,
        None => return Ok(()),
    };

    let states = states.read().await;
    if states.is_empty() {
        return Ok(());
    }

    for state in states.iter() {
        // Skip MQTT-in backends
        if state.subscribe_uplinks {
            continue;
        }

        // Check gateway ID filters
        if !state.match_gateway_id(gateway_id) {
            debug!(gateway_id = %gateway_id, server = %state.server, "Gateway ID does not match MQTT filters");
            continue;
        }

        info!(
            server = %state.server,
            gateway_id = %gateway_id,
            event_type = %event_type,
            "Forwarding MQTT event"
        );

        if let Err(e) = state
            .client
            .publish(original_topic, state.qos, false, payload)
            .await
        {
            error!(server = %state.server, error = %e, "Failed to publish MQTT event");
        }
    }

    Ok(())
}

/// Extract the region (topic_prefix) from an MQTT topic.
/// Topic format: "{region}/gateway/{gw_id}/{type}/{subtype}"
/// Returns the part before "/gateway/", or empty string if not found.
fn extract_region_from_topic(topic: &str) -> String {
    if let Some(idx) = topic.find("/gateway/") {
        topic[..idx].to_string()
    } else {
        String::new()
    }
}

/// Convert a ChirpStack UplinkFrame to Semtech PUSH_DATA bytes.
fn uplink_frame_to_push_data(frame: &gw::UplinkFrame, gateway_id: &GatewayId) -> Result<Vec<u8>> {
    let rxpk = uplink_frame_to_rxpk(frame)?;

    let push_data = PushData {
        protocol_version: 0x02,
        random_token: rand::random(),
        gateway_id: *gateway_id.as_bytes(),
        payload: crate::packets::PushDataPayload::new(vec![rxpk.clone()]),
    };

    debug!(
        gateway_id = %gateway_id,
        freq = ?rxpk.freq,
        datr = ?rxpk.datr,
        codr = ?rxpk.codr,
        modu = ?rxpk.modu,
        rssi = ?rxpk.rssi,
        lsnr = ?rxpk.lsnr,
        tmst = ?rxpk.tmst,
        tmms = ?rxpk.tmms,
        time = ?rxpk.time,
        ftime = ?rxpk.ftime,
        chan = ?rxpk.chan,
        rfch = ?rxpk.rfch,
        brd = ?rxpk.brd,
        stat = ?rxpk.stat,
        size = ?rxpk.size,
        hpw = ?rxpk.hpw,
        has_rsig = rxpk.rsig.is_some(),
        has_meta = rxpk.meta.is_some(),
        data_len = rxpk.data.len(),
        "Generated PUSH_DATA rxpk from MQTT uplink"
    );

    Ok(push_data.to_bytes())
}

/// Convert a ChirpStack UplinkFrame to Semtech RxPk format.
/// This is the reverse of RxPk::to_uplink_frame() and must preserve all metadata.
fn uplink_frame_to_rxpk(frame: &gw::UplinkFrame) -> Result<RxPk> {
    let tx_info = frame.tx_info.as_ref();
    let rx_info = frame.rx_info.as_ref();

    // Frequency in MHz
    let freq = tx_info.map(|t| t.frequency as f64 / 1_000_000.0);

    // Parse modulation (returns datr, modu, codr)
    let (datr, modu, codr, hpw) = parse_modulation_full(tx_info);

    // Get RSSI and SNR from rx_info
    let rssi = rx_info.map(|r| r.rssi);
    let lsnr = rx_info.map(|r| r.snr as f64);

    // Channel and RF chain
    let chan = rx_info.map(|r| r.channel);
    let rfch = rx_info.map(|r| r.rf_chain);

    // Board
    let brd = rx_info.map(|r| r.board);

    // Antenna (for potential rsig reconstruction)
    let antenna = rx_info.map(|r| r.antenna);

    // Extract internal timestamp from context (4 bytes big-endian)
    let tmst = rx_info.and_then(|r| {
        if r.context.len() >= 4 {
            Some(u32::from_be_bytes([r.context[0], r.context[1], r.context[2], r.context[3]]))
        } else {
            None
        }
    });

    // Extract time (ISO 8601 string) from gw_time
    let time = rx_info.and_then(|rx| {
        rx.gw_time.as_ref().map(|gw_time| {
            let datetime = chrono::DateTime::from_timestamp(gw_time.seconds, gw_time.nanos as u32)
                .unwrap_or_else(|| chrono::Utc::now());
            // Use compact ISO 8601 format with microsecond precision (matches gateway-bridge)
            datetime.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
        })
    });

    // Extract GPS time from time_since_gps_epoch (milliseconds since GPS epoch)
    let tmms = rx_info.and_then(|rx| {
        rx.time_since_gps_epoch.as_ref().map(|dur| {
            // Convert seconds + nanos to milliseconds
            dur.seconds * 1000 + (dur.nanos as i64 / 1_000_000)
        })
    });

    // Extract fine timestamp from fine_time_since_gps_epoch (nanoseconds part)
    let ftime = rx_info.and_then(|rx| {
        rx.fine_time_since_gps_epoch.as_ref().map(|dur| {
            // The nanos field contains the fine timestamp (0-999999999)
            dur.nanos as u32
        })
    });

    // Parse CRC status from rx_info
    let stat = rx_info.map(|rx| {
        match gw::CrcStatus::try_from(rx.crc_status).unwrap_or(gw::CrcStatus::NoCrc) {
            gw::CrcStatus::CrcOk => 1i8,
            gw::CrcStatus::BadCrc => -1i8,
            _ => 0i8,
        }
    });

    // Size of the PHY payload
    let size = Some(frame.phy_payload.len() as u16);

    // Extract metadata map from rx_info
    let meta = rx_info.and_then(|rx| {
        if rx.metadata.is_empty() {
            None
        } else {
            Some(rx.metadata.clone())
        }
    });

    // Build rsig array if we have antenna-specific data
    // Note: A single UplinkFrame only has one antenna's data, so we create a single rsig entry
    let rsig = rx_info.and_then(|rx| {
        // Only include rsig if antenna is non-zero or we have specific antenna data
        if rx.antenna > 0 || antenna.is_some() {
            Some(vec![crate::packets::RSig {
                ant: Some(rx.antenna as u8),
                chan: Some(rx.channel as u8),
                rssic: Some(rx.rssi as i16),
                lsnr: Some(rx.snr),
                etime: None, // Encrypted fine timestamp not available in UplinkFrame
            }])
        } else {
            None
        }
    });

    // Build other fields map for any additional fields we might need
    let other = std::collections::HashMap::new();

    Ok(RxPk {
        data: frame.phy_payload.clone(),
        tmst,
        time,
        tmms,
        freq,
        rfch,
        modu,
        datr,
        codr,
        rssi,
        lsnr,
        chan,
        stat,
        brd,
        ftime,
        hpw,
        aesk: None, // AES key index not available in UplinkFrame
        size,
        rsig,
        meta,
        other,
    })
}

/// Parse modulation info and return (datr, modu, codr, hpw).
fn parse_modulation_full(tx_info: Option<&gw::UplinkTxInfo>) -> (Option<serde_json::Value>, Option<String>, Option<String>, Option<u8>) {
    let modulation = tx_info.and_then(|t| t.modulation.as_ref());
    let params = modulation.and_then(|m| m.parameters.as_ref());

    match params {
        Some(gw::modulation::Parameters::Lora(lora)) => {
            let sf = lora.spreading_factor;
            let bw = lora.bandwidth / 1000; // Hz to kHz
            let datr = serde_json::Value::String(format!("SF{}BW{}", sf, bw));
            let codr = code_rate_to_string(lora.code_rate);
            (Some(datr), Some("LORA".into()), Some(codr.into()), None)
        }
        Some(gw::modulation::Parameters::Fsk(fsk)) => {
            let datr = serde_json::Value::Number(serde_json::Number::from(fsk.datarate));
            (Some(datr), Some("FSK".into()), None, None)
        }
        Some(gw::modulation::Parameters::LrFhss(lr_fhss)) => {
            let ocw = lr_fhss.operating_channel_width / 1000; // Hz to kHz
            let datr = serde_json::Value::String(format!("M0CW{}", ocw));
            let codr = code_rate_to_string(lr_fhss.code_rate);
            let hpw = if lr_fhss.grid_steps > 0 {
                Some(lr_fhss.grid_steps as u8)
            } else {
                None
            };
            (Some(datr), Some("LORA".into()), Some(codr.into()), hpw)
        }
        _ => (None, None, None, None),
    }
}

/// Convert CodeRate enum to string format used in Semtech UDP protocol.
fn code_rate_to_string(code_rate: i32) -> &'static str {
    match gw::CodeRate::try_from(code_rate).unwrap_or(gw::CodeRate::Cr45) {
        gw::CodeRate::Cr45 => "4/5",
        gw::CodeRate::Cr46 => "4/6",
        gw::CodeRate::Cr47 => "4/7",
        gw::CodeRate::Cr48 => "4/8",
        gw::CodeRate::Cr38 => "3/8",
        gw::CodeRate::Cr26 => "2/6",
        gw::CodeRate::Cr14 => "1/4",
        gw::CodeRate::Cr16 => "1/6",
        gw::CodeRate::Cr56 => "5/6",
        gw::CodeRate::CrLi45 => "4/5LI",
        gw::CodeRate::CrLi46 => "4/6LI",
        gw::CodeRate::CrLi48 => "4/8LI",
        _ => "4/5",
    }
}

/// Send an uplink frame to all configured MQTT backends.
pub async fn send_uplink_frame(gateway_id: GatewayId, push_data: &PushData, region: &str, relay_id: Option<&str>) -> Result<()> {
    let states = match STATES.get() {
        Some(s) => s,
        None => return Ok(()), // MQTT not configured
    };

    let states = states.read().await;
    if states.is_empty() {
        return Ok(());
    }

    for state in states.iter() {
        if let Err(e) = send_uplink_frame_to_backend(gateway_id, push_data, state, region, relay_id).await {
            error!(server = %state.server, error = %e, "Failed to send uplink to MQTT backend");
        }
    }

    Ok(())
}

/// Register a gateway as an MQTT-in gateway for downlink routing.
/// Also sends PULL_DATA to UDP servers if needed (first registration or keepalive expired).
async fn register_mqtt_gateway(gateway_id: GatewayId, server: String, region: String, context: Vec<u8>) {
    let gateways = MQTT_GATEWAYS
        .get_or_init(|| async { RwLock::new(std::collections::HashMap::new()) })
        .await;

    let should_send_pull_data = {
        let gateways = gateways.read().await;
        match gateways.get(&gateway_id) {
            None => true, // First registration
            Some(info) => info.last_pull_data.elapsed() >= PULL_DATA_INTERVAL, // Keepalive expired
        }
    };

    if should_send_pull_data {
        // Send PULL_DATA to all UDP servers
        if let Err(e) = forwarder::send_pull_data_for_gateway(gateway_id).await {
            error!(gateway_id = %gateway_id, error = %e, "Failed to send PULL_DATA for MQTT-in gateway");
        }
    }

    // Extract tmst from context (4 bytes big-endian, matching gateway bridge format)
    let tmst = if context.len() >= 4 {
        Some(u32::from_be_bytes([context[0], context[1], context[2], context[3]]))
    } else {
        None
    };

    // Update or insert the gateway info
    let mut gateways = gateways.write().await;
    let now = std::time::Instant::now();

    if let Some(info) = gateways.get_mut(&gateway_id) {
        info.server = server;
        info.region = region;
        if !context.is_empty() {
            info.last_context = context;
            info.last_tmst = tmst;
        }
        if should_send_pull_data {
            info.last_pull_data = now;
        }
    } else {
        gateways.insert(gateway_id, MqttGatewayInfo {
            server,
            region,
            last_pull_data: now,
            last_context: context,
            last_tmst: tmst,
        });
    }
}

/// Check if a gateway is registered as an MQTT-in gateway.
pub async fn is_mqtt_gateway(gateway_id: GatewayId) -> bool {
    let gateways = match MQTT_GATEWAYS.get() {
        Some(g) => g,
        None => return false,
    };
    let gateways = gateways.read().await;
    gateways.contains_key(&gateway_id)
}

/// Get the stored uplink context and tmst for an MQTT-in gateway.
/// Returns (context_bytes, optional_tmst). Context may be empty if no uplink has been received yet.
async fn get_mqtt_gateway_uplink_info(gateway_id: GatewayId) -> (Vec<u8>, Option<u32>) {
    let gateways = match MQTT_GATEWAYS.get() {
        Some(g) => g,
        None => return (Vec::new(), None),
    };
    let gateways = gateways.read().await;
    gateways
        .get(&gateway_id)
        .map(|info| (info.last_context.clone(), info.last_tmst))
        .unwrap_or((Vec::new(), None))
}

/// Get the server, region, and context for an MQTT-in gateway.
/// Used by forwarder to copy border gateway info to virtual relay gateways.
pub async fn get_mqtt_gateway_info(gateway_id: GatewayId) -> Option<(String, String, Vec<u8>)> {
    let gateways = match MQTT_GATEWAYS.get() {
        Some(g) => g,
        None => return None,
    };
    let gateways = gateways.read().await;
    gateways.get(&gateway_id).map(|info| {
        (info.server.clone(), info.region.clone(), info.last_context.clone())
    })
}

/// Public wrapper for register_mqtt_gateway, used by forwarder for virtual relay gateways.
pub async fn register_mqtt_gateway_pub(gateway_id: GatewayId, server: String, region: String, context: Vec<u8>) {
    register_mqtt_gateway(gateway_id, server, region, context).await;
}

/// Register a virtual relay gateway mapping to its real border gateway.
pub async fn register_relay_gateway(virtual_id: GatewayId, border_id: GatewayId) {
    let gateways = RELAY_GATEWAYS
        .get_or_init(|| async { RwLock::new(std::collections::HashMap::new()) })
        .await;
    let mut gateways = gateways.write().await;
    if let Some(info) = gateways.get_mut(&virtual_id) {
        info.border_gateway_id = border_id;
        info.last_seen = std::time::Instant::now();
    } else {
        gateways.insert(virtual_id, RelayGatewayInfo {
            border_gateway_id: border_id,
            last_seen: std::time::Instant::now(),
        });
    }
}

/// Resolve a virtual relay gateway to its real border gateway.
/// Returns None if the gateway_id is not a virtual relay gateway.
pub async fn resolve_relay_gateway(gateway_id: GatewayId) -> Option<GatewayId> {
    let gateways = match RELAY_GATEWAYS.get() {
        Some(g) => g,
        None => return None,
    };
    let gateways = gateways.read().await;
    gateways.get(&gateway_id).map(|info| info.border_gateway_id)
}

/// Periodically clean up stale relay gateway mappings.
async fn cleanup_relay_gateways() {
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;

        let gateways = RELAY_GATEWAYS
            .get_or_init(|| async { RwLock::new(std::collections::HashMap::new()) })
            .await;
        let mut gateways = gateways.write().await;
        let before = gateways.len();
        gateways.retain(|_, info| info.last_seen.elapsed() < Duration::from_secs(300));
        let removed = before - gateways.len();
        if removed > 0 {
            info!(removed = removed, remaining = gateways.len(), "Cleaned up stale relay gateway mappings");
        }
    }
}

/// Send an uplink frame directly to MQTT backends (pass-through from MQTT-in).
/// The payload and topic are forwarded as-is, only filtering is applied.
async fn send_uplink_frame_direct(gateway_id: GatewayId, payload: &[u8], original_topic: &str, phy_payload: &[u8]) -> Result<()> {
    let states = match STATES.get() {
        Some(s) => s,
        None => return Ok(()), // MQTT not configured
    };

    let states = states.read().await;
    if states.is_empty() {
        return Ok(());
    }

    // Extract dev_addr for logging
    let dev_addr_str = if phy_payload.len() >= 5 {
        let mtype = phy_payload[0] >> 5;
        if mtype == 2 || mtype == 4 {
            format!("{:02x}{:02x}{:02x}{:02x}",
                phy_payload[4], phy_payload[3], phy_payload[2], phy_payload[1])
        } else if mtype == 0 {
            "join-req".to_string()
        } else {
            format!("mtype={}", mtype)
        }
    } else {
        "unknown".to_string()
    };

    for state in states.iter() {
        // Skip MQTT-in backends (they receive, not publish uplinks)
        if state.subscribe_uplinks {
            continue;
        }

        // Check gateway ID filters
        if !state.match_gateway_id(gateway_id) {
            debug!(gateway_id = %gateway_id, server = %state.server, "Gateway ID does not match MQTT filters");
            continue;
        }

        // Apply filters to phy_payload
        if !state.allow_deny_filters.dev_addr_deny.is_empty()
            || !state.allow_deny_filters.join_eui_deny.is_empty()
        {
            if !state.allow_deny_filters.matches(phy_payload) {
                debug!(server = %state.server, "Uplink filtered out by allow/deny filters");
                continue;
            }
        } else if !state.filters.dev_addr_prefixes.is_empty()
            || !state.filters.join_eui_prefixes.is_empty()
        {
            if !lrwn_filters::matches(phy_payload, &state.filters) {
                debug!(server = %state.server, "Uplink filtered out by filters");
                continue;
            }
        }

        info!(
            server = %state.server,
            gateway_id = %gateway_id,
            dev_addr = %dev_addr_str,
            "Forwarding MQTT uplink"
        );

        if let Err(e) = state
            .client
            .publish(original_topic, state.qos, false, payload)
            .await
        {
            error!(server = %state.server, error = %e, "Failed to publish MQTT uplink");
        }
    }

    Ok(())
}

/// Send a downlink frame directly to an MQTT-in gateway.
/// If the gateway_id is a virtual relay gateway, resolves to the border gateway and rewrites the topic.
async fn send_downlink_frame_direct(gateway_id: GatewayId, payload: &[u8], topic: &str) -> Result<()> {
    // Resolve virtual relay gateway to border gateway
    let border_gw = resolve_relay_gateway(gateway_id).await;
    let route_gw = border_gw.unwrap_or(gateway_id);

    // Get the server this gateway is associated with
    let (server, region) = {
        let gateways = match MQTT_GATEWAYS.get() {
            Some(g) => g,
            None => return Err(anyhow!("MQTT gateways not initialized")),
        };
        let gateways = gateways.read().await;
        gateways.get(&route_gw).map(|info| (info.server.clone(), info.region.clone()))
    }
    .ok_or_else(|| anyhow!("Gateway {} not registered as MQTT gateway", route_gw))?;

    // Reconstruct topic with border gateway if this is a relay
    let effective_topic = if border_gw.is_some() {
        let route_gw_str = route_gw.to_string();
        format!("{}/gateway/{}/command/down", region, route_gw_str)
    } else {
        topic.to_string()
    };

    // Find the MQTT-in backend for this gateway
    let states = match STATES.get() {
        Some(s) => s,
        None => return Err(anyhow!("MQTT states not initialized")),
    };

    let states = states.read().await;
    for state in states.iter() {
        if state.server == server && state.subscribe_uplinks {
            if border_gw.is_some() {
                info!(
                    server = %state.server,
                    virtual_gateway = %gateway_id,
                    border_gateway = %route_gw,
                    topic = %effective_topic,
                    "Forwarding MQTT downlink (relay → border gateway)"
                );
            } else {
                info!(
                    server = %state.server,
                    gateway_id = %gateway_id,
                    "Forwarding MQTT downlink"
                );
            }

            state
                .client
                .publish(&effective_topic, state.qos, false, payload)
                .await
                .context("MQTT publish downlink")?;

            return Ok(());
        }
    }

    Err(anyhow!("No MQTT-in backend found for gateway {}", route_gw))
}

/// Send a downlink frame to an MQTT-in gateway.
/// The data is expected to be a PULL_RESP packet (Semtech UDP format).
/// If the gateway_id is a virtual relay gateway, resolves to the border gateway.
pub async fn send_downlink_frame(gateway_id: GatewayId, data: &[u8]) -> Result<()> {
    // Resolve virtual relay gateway to border gateway for routing
    let border_gw = resolve_relay_gateway(gateway_id).await;
    let route_gw = border_gw.unwrap_or(gateway_id);

    if let Some(bgw) = border_gw {
        info!(
            virtual_gateway = %gateway_id,
            border_gateway = %bgw,
            "Resolving relay downlink to border gateway"
        );
    }

    // Get the server and region for the gateway we're routing to (border or original)
    let (server, region) = {
        let gateways = match MQTT_GATEWAYS.get() {
            Some(g) => g,
            None => return Err(anyhow!("MQTT gateways not initialized")),
        };
        let gateways = gateways.read().await;
        gateways.get(&route_gw).map(|info| (info.server.clone(), info.region.clone()))
    }
    .ok_or_else(|| anyhow!("Gateway {} not registered as MQTT gateway", route_gw))?;

    // Parse the PULL_RESP packet to get the downlink frame
    let pull_resp = PullResp::from_slice(data)?;

    // Find the MQTT-in backend for this gateway
    let states = match STATES.get() {
        Some(s) => s,
        None => return Err(anyhow!("MQTT states not initialized")),
    };

    // Look up stored original uplink context for the border gateway
    let (stored_context, stored_tmst) = get_mqtt_gateway_uplink_info(route_gw).await;

    let states = states.read().await;
    for state in states.iter() {
        if state.server == server && state.subscribe_uplinks {
            // Route downlink to the border gateway's MQTT topic
            let route_gw_str = route_gw.to_string();
            let topic = format!("{}/gateway/{}/command/down", region, route_gw_str);

            // Convert PULL_RESP to ChirpStack DownlinkFrame, using original uplink
            // context when available so the gateway bridge can properly schedule TX
            let frame = if !stored_context.is_empty() {
                pull_resp.to_downlink_frame_with_context(
                    &route_gw,
                    Some(stored_context.clone()),
                    stored_tmst,
                )?
            } else {
                pull_resp.to_downlink_frame(&route_gw)?
            };

            let payload = if state.json {
                serde_json::to_vec(&frame)?
            } else {
                frame.encode_to_vec()
            };

            info!(
                gateway_id = %route_gw,
                virtual_gateway = %gateway_id,
                server = %state.server,
                topic = %topic,
                has_original_context = !stored_context.is_empty(),
                "Sending MQTT downlink"
            );

            state
                .client
                .publish(&topic, state.qos, false, payload)
                .await
                .context("MQTT publish downlink")?;

            return Ok(());
        }
    }

    Err(anyhow!("No MQTT-in backend found for gateway {}", gateway_id))
}

/// Send an uplink frame to a single MQTT backend.
async fn send_uplink_frame_to_backend(
    gateway_id: GatewayId,
    push_data: &PushData,
    state: &State,
    region: &str,
    relay_id: Option<&str>,
) -> Result<()> {
    // Skip backends that subscribe to uplinks (MQTT-in) to avoid feedback loops
    if state.subscribe_uplinks {
        debug!(server = %state.server, "Skipping MQTT-in backend for uplink publishing");
        return Ok(());
    }

    // Determine effective gateway_id: apply relay prefix if configured and relay_id present
    let effective_gateway_id = if let Some(rid) = relay_id {
        if !state.relay_gateway_id_prefix.is_empty() {
            let virtual_hex = format!("{}{}", state.relay_gateway_id_prefix, rid);
            match GatewayId::from_hex(&virtual_hex) {
                Ok(virtual_gw) => {
                    // Register mapping for downlink routing
                    register_relay_gateway(virtual_gw, gateway_id).await;
                    // Register virtual gateway in MQTT_GATEWAYS so downlinks can route back
                    let gw_info = {
                        let gateways = MQTT_GATEWAYS
                            .get_or_init(|| async { RwLock::new(std::collections::HashMap::new()) })
                            .await;
                        let gateways = gateways.read().await;
                        gateways.get(&gateway_id).map(|info| (info.server.clone(), info.region.clone(), info.last_context.clone()))
                    };
                    if let Some((server, gw_region, context)) = gw_info {
                        register_mqtt_gateway(virtual_gw, server, gw_region, context).await;
                    }
                    info!(
                        server = %state.server,
                        border_gateway = %gateway_id,
                        virtual_gateway = %virtual_gw,
                        relay_id = %rid,
                        "Rewriting gateway_id for mesh relay (MQTT output)"
                    );
                    virtual_gw
                }
                Err(e) => {
                    warn!(relay_id = %rid, prefix = %state.relay_gateway_id_prefix, error = %e, "Invalid virtual gateway ID, using original");
                    gateway_id
                }
            }
        } else {
            gateway_id
        }
    } else {
        gateway_id
    };

    // Check gateway ID filters (new allow/deny takes precedence)
    if !state.match_gateway_id(effective_gateway_id) {
        debug!(gateway_id = %effective_gateway_id, server = %state.server, "Gateway ID does not match MQTT filters");
        return Ok(());
    }

    // Apply filters to the push data
    let mut filtered_push_data = push_data.payload.clone();
    let before_count = filtered_push_data.rxpk.len();

    // Apply filters with deny support
    if !state.allow_deny_filters.dev_addr_deny.is_empty()
        || !state.allow_deny_filters.join_eui_deny.is_empty()
    {
        // Use allow/deny filters when deny lists are configured
        filtered_push_data.filter_rxpk_allow_deny(&state.allow_deny_filters);
    } else if !state.filters.dev_addr_prefixes.is_empty()
        || !state.filters.join_eui_prefixes.is_empty()
    {
        // Use legacy filtering (allow-only) when allow lists are configured
        filtered_push_data.filter_rxpk(&state.filters);
    }
    // If no filters configured, all packets pass through

    // Extract dev_addr and fport from first rxpk for logging
    let (dev_addr_str, fport_str) = push_data.payload.rxpk.first().map(|rxpk| {
        if rxpk.data.len() >= 5 {
            let mhdr = rxpk.data[0];
            let mtype = mhdr >> 5;
            // MType 2 = Unconfirmed Data Up, MType 4 = Confirmed Data Up
            if mtype == 2 || mtype == 4 {
                // Data uplink - extract DevAddr (bytes 1-4, little-endian)
                let dev_addr = format!("{:02x}{:02x}{:02x}{:02x}",
                    rxpk.data[4], rxpk.data[3], rxpk.data[2], rxpk.data[1]);
                // FPort is at byte 8 (after MHDR[1] + DevAddr[4] + FCtrl[1] + FCnt[2])
                // Minimum frame with FPort: 13 bytes (includes MIC)
                let fport = if rxpk.data.len() >= 13 {
                    rxpk.data[8].to_string()
                } else {
                    "-".to_string()
                };
                (dev_addr, fport)
            } else {
                // Log the mtype for debugging
                (format!("mtype={}", mtype), "-".to_string())
            }
        } else {
            (format!("len={}", rxpk.data.len()), "-".to_string())
        }
    }).unwrap_or_else(|| ("none".to_string(), "-".to_string()));

    info!(
        server = %state.server,
        dev_addr = %dev_addr_str,
        f_port = %fport_str,
        before = before_count,
        after = filtered_push_data.rxpk.len(),
        dev_addr_prefixes = state.filters.dev_addr_prefixes.len(),
        "Applied MQTT output filters"
    );

    if filtered_push_data.rxpk.is_empty() {
        debug!(server = %state.server, "No rxpk entries after filtering");
        return Ok(());
    }

    // Strip relay metadata when relay rewriting is active, so downstream
    // LNS doesn't see relay_id/hop_count and try to handle it itself.
    if effective_gateway_id != gateway_id {
        for rxpk in &mut filtered_push_data.rxpk {
            if let Some(ref mut meta) = rxpk.meta {
                meta.remove("relay_id");
                meta.remove("hop_count");
                if meta.is_empty() {
                    rxpk.meta = None;
                }
            }
        }
        debug!(server = %state.server, "Stripped relay metadata from uplink");
    }

    let effective_gw_str = effective_gateway_id.to_string();
    let topic = format!("{}/gateway/{}/event/up", region, effective_gw_str);

    for rxpk in &filtered_push_data.rxpk {
        let frame = rxpk.to_uplink_frame(&effective_gw_str);

        let payload = if state.json {
            serde_json::to_vec(&frame)?
        } else {
            frame.encode_to_vec()
        };

        trace!(server = %state.server, topic = %topic, "Publishing MQTT uplink");
        state
            .client
            .publish(&topic, state.qos, false, payload)
            .await
            .context("MQTT publish")?;

        inc_mqtt_messages_published(&state.server, "up").await;
    }

    info!(
        gateway_id = %effective_gateway_id,
        server = %state.server,
        topic = %topic,
        dev_addr = %dev_addr_str,
        f_port = %fport_str,
        count = filtered_push_data.rxpk.len(),
        "Published MQTT uplink frames"
    );

    Ok(())
}

/// Check if any MQTT backend is enabled and configured.
pub async fn is_enabled() -> bool {
    match STATES.get() {
        Some(states) => !states.read().await.is_empty(),
        None => false,
    }
}

/// Send a TX_ACK (DownlinkTxAck) to all configured MQTT backends.
pub async fn send_tx_ack(gateway_id: GatewayId, tx_ack: &gw::DownlinkTxAck, region: &str) -> Result<()> {
    let states = match STATES.get() {
        Some(s) => s,
        None => return Ok(()), // MQTT not configured
    };

    let states = states.read().await;
    if states.is_empty() {
        return Ok(());
    }

    for state in states.iter() {
        // Skip MQTT-in backends
        if state.subscribe_uplinks {
            continue;
        }

        // Check gateway ID filters
        if !state.match_gateway_id(gateway_id) {
            continue;
        }

        let gateway_id_str = gateway_id.to_string();
        let topic = format!("{}/gateway/{}/event/ack", region, gateway_id_str);

        let payload = if state.json {
            serde_json::to_vec(tx_ack)?
        } else {
            tx_ack.encode_to_vec()
        };

        info!(
            gateway_id = %gateway_id,
            server = %state.server,
            topic = %topic,
            downlink_id = tx_ack.downlink_id,
            "Publishing MQTT DownlinkTxAck"
        );

        if let Err(e) = state
            .client
            .publish(&topic, state.qos, false, payload)
            .await
        {
            error!(server = %state.server, error = %e, "Failed to publish MQTT TxAck");
        }

        inc_mqtt_messages_published(&state.server, "ack").await;
    }

    Ok(())
}

/// Send gateway stats to all configured MQTT backends.
pub async fn send_gateway_stats(gateway_id: GatewayId, stats: &gw::GatewayStats, region: &str) -> Result<()> {
    let states = match STATES.get() {
        Some(s) => s,
        None => return Ok(()), // MQTT not configured
    };

    let states = states.read().await;
    if states.is_empty() {
        return Ok(());
    }

    for state in states.iter() {
        // Skip MQTT-in backends
        if state.subscribe_uplinks {
            continue;
        }

        // Check gateway ID filters
        if !state.match_gateway_id(gateway_id) {
            continue;
        }

        let gateway_id_str = gateway_id.to_string();
        let topic = format!("{}/gateway/{}/event/stats", region, gateway_id_str);

        let payload = if state.json {
            serde_json::to_vec(stats)?
        } else {
            stats.encode_to_vec()
        };

        info!(
            gateway_id = %gateway_id,
            server = %state.server,
            topic = %topic,
            "Publishing MQTT GatewayStats"
        );

        if let Err(e) = state
            .client
            .publish(&topic, state.qos, false, payload)
            .await
        {
            error!(server = %state.server, error = %e, "Failed to publish MQTT stats");
        }

        inc_mqtt_messages_published(&state.server, "stats").await;
    }

    Ok(())
}
