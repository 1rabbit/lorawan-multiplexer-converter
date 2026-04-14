use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Cursor};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use axum::extract::ws::{Message as AxumMessage, WebSocket};
use axum::extract::{Path, WebSocketUpgrade};
use axum::routing::any;
use axum::Router;
use base64::{Engine as _, engine::general_purpose};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::{OnceCell, RwLock};
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
use tokio_tungstenite::Connector;
use tracing::{debug, error, info, warn};

use crate::config;
use crate::filters::{AllowDenyFilters, GatewayIdFilters};
use crate::forwarder;
use crate::packets::{GatewayId, PullResp, PushData, PushDataPayload, RxPk};

// ----- Statics -----

/// Input-connected BS gateways (gateway_id -> info with downlink sender).
static BS_GATEWAYS: OnceCell<RwLock<HashMap<GatewayId, BsGatewayInfo>>> = OnceCell::const_new();

/// Output connection states.
static STATES: OnceCell<RwLock<Vec<OutputState>>> = OnceCell::const_new();

struct BsGatewayInfo {
    /// Region / topic_prefix for this gateway (from the input config).
    #[allow(dead_code)]
    region: String,
    /// Sender to push downlink JSON messages to the gateway's WS writer task.
    downlink_sender: UnboundedSender<String>,
    /// Last xtime+rctx received from this gateway (for building dnmsg).
    last_xtime: u64,
    last_rctx: u64,
    /// DR table from the input config (for datr <-> DR index conversions).
    dr_table: Vec<[i32; 3]>,
}

/// Uplink data sent to BS output connections.
pub enum BsUplinkData {
    /// Passthrough from BS-in: original JSON text, forwarded as-is.
    RawJson(String),
    /// Cross-protocol from GWMP-in: GWMP binary bytes, needs conversion to BS JSON.
    GwmpBytes(Vec<u8>),
}

/// Pending BS output downlinks: (gateway_id, downlink_id) → diid.
/// Used to correlate TX_ACKs back to dntxed messages.
static PENDING_BS_DOWNLINKS: OnceCell<RwLock<HashMap<(GatewayId, u32), PendingDntxed>>> =
    OnceCell::const_new();

struct PendingDntxed {
    diid: u64,
    /// Index into the STATES vec identifying which BS output to send dntxed on.
    state_index: usize,
}

struct OutputState {
    server: String,
    #[allow(dead_code)]
    uplink_only: bool,
    gateway_id_filters: GatewayIdFilters,
    allow_deny_filters: AllowDenyFilters,
    /// Per-gateway auth tokens. When non-empty, acts as an implicit allow list.
    gateway_tokens: HashMap<GatewayId, String>,
    /// Sender for uplink data to this output's WS writer tasks.
    /// (gateway_id, uplink_data, region)
    uplink_sender: UnboundedSender<(GatewayId, BsUplinkData, String)>,
    /// Sender for dntxed messages to be sent back to the LNS.
    dntxed_sender: UnboundedSender<(GatewayId, u64)>,
}

// ----- JSON Message Structs -----

#[derive(Serialize, Deserialize, Debug)]
#[allow(dead_code)]
struct VersionMessage {
    msgtype: String,
    #[serde(default)]
    station: String,
    #[serde(default)]
    firmware: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    protocol: i32,
    #[serde(default)]
    features: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct RouterConfigMessage {
    msgtype: String,
    #[serde(rename = "NetID")]
    net_id: Vec<u32>,
    #[serde(rename = "JoinEui")]
    join_eui: Vec<[u64; 2]>,
    region: String,
    #[serde(default)]
    hwspec: String,
    freq_range: [u32; 2],
    #[serde(rename = "DRs")]
    drs: Vec<[i32; 3]>,
    #[serde(rename = "MuxTime")]
    mux_time: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct UplinkDataFrame {
    msgtype: String,
    #[serde(rename = "MHdr")]
    mhdr: u8,
    #[serde(rename = "DevAddr")]
    dev_addr: i64,
    #[serde(rename = "FCtrl")]
    fctrl: u8,
    #[serde(rename = "FCnt")]
    fcnt: u16,
    #[serde(rename = "FOpts")]
    fopts: String,
    #[serde(rename = "FPort")]
    fport: i32,
    #[serde(rename = "FRMPayload")]
    frm_payload: String,
    #[serde(rename = "MIC")]
    mic: i32,
    #[serde(rename = "DR")]
    dr: u8,
    #[serde(rename = "Freq")]
    freq: u32,
    pub upinfo: UpInfo,
}

#[derive(Serialize, Deserialize, Debug)]
struct JoinRequest {
    msgtype: String,
    #[serde(rename = "MHdr")]
    mhdr: u8,
    #[serde(rename = "JoinEui")]
    join_eui: u64,
    #[serde(rename = "DevEui")]
    dev_eui: u64,
    #[serde(rename = "DevNonce")]
    dev_nonce: u16,
    #[serde(rename = "MIC")]
    mic: i32,
    #[serde(rename = "DR")]
    dr: u8,
    #[serde(rename = "Freq")]
    freq: u32,
    pub upinfo: UpInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct UpInfo {
    rctx: u64,
    xtime: u64,
    #[serde(default)]
    gpstime: u64,
    rssi: f64,
    snr: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct DownlinkMessage {
    msgtype: String,
    #[serde(rename = "DevEui", default)]
    dev_eui: String,
    #[serde(default, rename = "dC")]
    dc: u8,
    diid: u64,
    pdu: String,
    #[serde(rename = "RxDelay", default)]
    rx_delay: u8,
    #[serde(rename = "RX1DR", default)]
    rx1dr: u8,
    #[serde(rename = "RX1Freq", default)]
    rx1freq: u32,
    #[serde(rename = "RX2DR", default)]
    rx2dr: u8,
    #[serde(rename = "RX2Freq", default)]
    rx2freq: u32,
    /// Class A/C: xtime from the corresponding uplink. Class B/C unsolicited: absent.
    #[serde(default)]
    xtime: u64,
    #[serde(default)]
    rctx: u64,
    #[serde(default)]
    priority: u8,
    /// Class B only: DR index for the downlink.
    #[serde(rename = "DR", default)]
    dr: Option<u8>,
    /// Class B only: Frequency in Hz for the downlink.
    #[serde(rename = "Freq", default)]
    freq: Option<u32>,
    /// Class B only: GPS time for the ping slot.
    #[serde(default)]
    gpstime: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct DownlinkTransmitted {
    msgtype: String,
    diid: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct TimeSyncRequest {
    msgtype: String,
    #[serde(default)]
    txtime: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct TimeSyncResponse {
    msgtype: String,
    txtime: f64,
    #[serde(rename = "gpstime")]
    gps_time: u64,
}

#[derive(Deserialize, Debug)]
struct GenericMessage {
    msgtype: String,
}

// ----- Public API -----

pub async fn setup(
    config: &config::BasicsConfig,
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    uplink_tx: UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
) -> Result<()> {
    BS_GATEWAYS
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;
    STATES
        .get_or_init(|| async { RwLock::new(Vec::new()) })
        .await;
    PENDING_BS_DOWNLINKS
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;

    for input in &config.inputs {
        setup_input(input, downlink_tx.clone(), uplink_tx.clone()).await?;
    }

    for output in &config.outputs {
        setup_output(output, downlink_tx.clone()).await?;
    }

    Ok(())
}

pub async fn is_bs_gateway(gateway_id: GatewayId) -> bool {
    let gateways = match BS_GATEWAYS.get() {
        Some(g) => g,
        None => return false,
    };
    let gateways = gateways.read().await;
    gateways.contains_key(&gateway_id)
}

pub async fn is_enabled() -> bool {
    match STATES.get() {
        Some(states) => !states.read().await.is_empty(),
        None => false,
    }
}

/// Send a downlink to a BS-connected gateway. `data` is PULL_RESP bytes.
pub async fn send_downlink_frame(gateway_id: GatewayId, data: &[u8]) -> Result<()> {
    let pull_resp = PullResp::from_slice(data)?;

    let gateways = BS_GATEWAYS
        .get()
        .ok_or_else(|| anyhow!("BS_GATEWAYS not initialized"))?;
    let gateways = gateways.read().await;
    let info = gateways
        .get(&gateway_id)
        .ok_or_else(|| anyhow!("Gateway {} not registered as BS gateway", gateway_id))?;

    let dnmsg = pull_resp_to_dnmsg(&pull_resp, info)?;
    let json = serde_json::to_string(&dnmsg)?;

    info!(
        gateway_id = %gateway_id,
        diid = dnmsg.diid,
        "Sending BS downlink"
    );

    info.downlink_sender
        .send(json)
        .map_err(|_| anyhow!("Downlink channel closed for gateway {}", gateway_id))?;

    Ok(())
}

/// Send an uplink (PushData) to all BS outputs. Called from forwarder (cross-protocol: GWMP→BS).
pub async fn send_uplink_frame(
    gateway_id: GatewayId,
    push_data: &PushData,
    region: &str,
) -> Result<()> {
    let states = match STATES.get() {
        Some(s) => s,
        None => return Ok(()),
    };

    let states = states.read().await;
    if states.is_empty() {
        return Ok(());
    }

    let data = push_data.to_bytes();

    for state in states.iter() {
        // When gateway_tokens is non-empty, it acts as an implicit allow list.
        if !state.gateway_tokens.is_empty() && !state.gateway_tokens.contains_key(&gateway_id) {
            continue;
        }

        let gw_id_le = gateway_id.as_bytes_le();
        if !state.gateway_id_filters.allow.is_empty()
            || !state.gateway_id_filters.deny.is_empty()
        {
            if !state.gateway_id_filters.matches(gw_id_le) {
                continue;
            }
        }

        // Apply payload filters
        for rxpk in &push_data.payload.rxpk {
            if !state.allow_deny_filters.dev_addr_deny.is_empty()
                || !state.allow_deny_filters.join_eui_deny.is_empty()
            {
                if !state.allow_deny_filters.matches(&rxpk.data) {
                    continue;
                }
            }
        }

        if let Err(e) = state.uplink_sender.send((gateway_id, BsUplinkData::GwmpBytes(data.clone()), region.to_string())) {
            error!(server = %state.server, error = %e, "BS output uplink send error");
        }
    }

    Ok(())
}

/// Send an uplink directly to BS outputs (BS→BS passthrough).
/// The original JSON text is forwarded as-is, only filtering is applied.
async fn send_uplink_frame_direct(
    gateway_id: GatewayId,
    original_json: &str,
    phy_payload: &[u8],
) -> Result<()> {
    let states = match STATES.get() {
        Some(s) => s,
        None => return Ok(()),
    };

    let states = states.read().await;
    if states.is_empty() {
        return Ok(());
    }

    for state in states.iter() {
        // When gateway_tokens is non-empty, it acts as an implicit allow list.
        if !state.gateway_tokens.is_empty() && !state.gateway_tokens.contains_key(&gateway_id) {
            continue;
        }

        // Check gateway ID filters.
        let gw_id_le = gateway_id.as_bytes_le();
        if !state.gateway_id_filters.allow.is_empty()
            || !state.gateway_id_filters.deny.is_empty()
        {
            if !state.gateway_id_filters.matches(gw_id_le) {
                continue;
            }
        }

        // Apply payload filters on the PHY payload.
        if !state.allow_deny_filters.dev_addr_deny.is_empty()
            || !state.allow_deny_filters.join_eui_deny.is_empty()
            || !state.allow_deny_filters.dev_addr_prefixes.is_empty()
            || !state.allow_deny_filters.join_eui_prefixes.is_empty()
        {
            if !state.allow_deny_filters.matches(phy_payload) {
                continue;
            }
        }

        if let Err(e) = state.uplink_sender.send((
            gateway_id,
            BsUplinkData::RawJson(original_json.to_string()),
            String::new(),
        )) {
            error!(server = %state.server, error = %e, "BS output uplink send error");
        }
    }

    Ok(())
}

/// Send a dntxed to the BS output that originated the downlink.
/// Called when a gateway (MQTT or UDP) acknowledges a downlink that came from a BS output.
pub async fn send_tx_ack(
    gateway_id: GatewayId,
    ack: &chirpstack_api::gw::DownlinkTxAck,
    _region: &str,
) -> Result<()> {
    let downlink_id = ack.downlink_id;

    let pending = match PENDING_BS_DOWNLINKS.get() {
        Some(p) => p,
        None => return Ok(()),
    };

    let entry = {
        let mut pending = pending.write().await;
        pending.remove(&(gateway_id, downlink_id))
    };

    let entry = match entry {
        Some(e) => e,
        None => return Ok(()), // Not a downlink from a BS output
    };

    let states = match STATES.get() {
        Some(s) => s,
        None => return Ok(()),
    };
    let states = states.read().await;

    if let Some(state) = states.get(entry.state_index) {
        info!(
            gateway_id = %gateway_id,
            diid = entry.diid,
            server = %state.server,
            "Sending dntxed to BS output"
        );
        if let Err(e) = state.dntxed_sender.send((gateway_id, entry.diid)) {
            error!(server = %state.server, error = %e, "Failed to send dntxed");
        }
    }

    Ok(())
}

// ----- Input (WS Server) -----

async fn setup_input(
    conf: &config::BasicsInput,
    _downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    uplink_tx: UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
) -> Result<()> {
    info!(bind = %conf.bind, topic_prefix = %conf.topic_prefix, "Setting up Basic Station input");

    let region = conf.topic_prefix.clone();
    let router_config = conf.router_config.clone();
    let dr_table = conf.router_config.drs.clone();
    let ping_interval = conf.ping_interval;
    let read_timeout = conf.read_timeout;
    let gateway_id_filters = GatewayIdFilters {
        allow: conf.gateway_id_prefixes.clone(),
        deny: conf.gateway_id_deny.clone(),
    };
    let allow_deny_filters = AllowDenyFilters {
        dev_addr_prefixes: conf.filters.dev_addr_prefixes.clone(),
        dev_addr_deny: conf.filters.dev_addr_deny.clone(),
        join_eui_prefixes: conf.filters.join_eui_prefixes.clone(),
        join_eui_deny: conf.filters.join_eui_deny.clone(),
    };

    let app = Router::new()
        .route(
            "/router-info",
            any({
                let region = region.clone();
                let router_config = router_config.clone();
                move |ws: WebSocketUpgrade| {
                    let region = region.clone();
                    let router_config = router_config.clone();
                    async move {
                        ws.on_upgrade(move |socket| {
                            handle_router_info(socket, region, router_config)
                        })
                    }
                }
            }),
        )
        .route(
            "/gateway/{gateway_id}",
            any({
                let region = region.clone();
                let router_config = router_config.clone();
                let dr_table = dr_table.clone();
                let uplink_tx = uplink_tx.clone();
                let ping_interval = ping_interval;
                let read_timeout = read_timeout;
                let gateway_id_filters = gateway_id_filters.clone();
                let allow_deny_filters = allow_deny_filters.clone();
                move |Path(gw_id): Path<String>, ws: WebSocketUpgrade| {
                    let region = region.clone();
                    let router_config = router_config.clone();
                    let dr_table = dr_table.clone();
                    let uplink_tx = uplink_tx.clone();
                    let gateway_id_filters = gateway_id_filters.clone();
                    let allow_deny_filters = allow_deny_filters.clone();
                    async move {
                        ws.on_upgrade(move |socket| {
                            handle_gateway_ws(
                                socket,
                                gw_id,
                                region,
                                router_config,
                                dr_table,
                                uplink_tx,
                                ping_interval,
                                read_timeout,
                                gateway_id_filters,
                                allow_deny_filters,
                            )
                        })
                    }
                }
            }),
        );

    let listener = TcpListener::bind(&conf.bind).await.context("Bind BS listener")?;

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            error!(error = %e, "Basic Station server error");
        }
    });

    Ok(())
}

async fn handle_router_info(
    mut socket: WebSocket,
    region: String,
    router_config: config::RouterConfig,
) {
    // Wait for the version message, then respond with router_config.
    if let Some(Ok(msg)) = socket.recv().await {
        if let AxumMessage::Text(text) = msg {
            debug!(msg = %text, "Received router-info request");
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let rc = RouterConfigMessage {
        msgtype: "router_config".into(),
        net_id: router_config.net_ids.clone(),
        join_eui: router_config.join_euis.clone(),
        region: region.clone(),
        hwspec: "sx1301/1".into(),
        freq_range: router_config.freq_range,
        drs: router_config.drs.clone(),
        mux_time: now,
    };

    let json = serde_json::to_string(&rc).unwrap_or_default();
    if let Err(e) = socket.send(AxumMessage::Text(json.into())).await {
        debug!(error = %e, "Failed to send router_config on /router-info");
    }
}

async fn handle_gateway_ws(
    socket: WebSocket,
    gw_id_str: String,
    region: String,
    router_config: config::RouterConfig,
    dr_table: Vec<[i32; 3]>,
    uplink_tx: UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    ping_interval: Duration,
    read_timeout: Duration,
    gateway_id_filters: GatewayIdFilters,
    allow_deny_filters: AllowDenyFilters,
) {
    // Parse gateway ID (may have "eui-" prefix from some BS implementations).
    let gw_hex = gw_id_str
        .strip_prefix("eui-")
        .unwrap_or(&gw_id_str);
    let gateway_id = match GatewayId::from_hex(gw_hex) {
        Ok(id) => id,
        Err(e) => {
            warn!(gw_id = %gw_id_str, error = %e, "Invalid gateway ID in BS connection");
            return;
        }
    };

    // Check gateway ID filters.
    let gw_id_le = gateway_id.as_bytes_le();
    if !gateway_id_filters.allow.is_empty() || !gateway_id_filters.deny.is_empty() {
        if !gateway_id_filters.matches(gw_id_le) {
            warn!(gateway_id = %gateway_id, "BS gateway rejected by gateway ID filters");
            return;
        }
    }

    info!(gateway_id = %gateway_id, "Basic Station gateway connected");

    let (mut ws_sink, mut ws_stream) = socket.split();

    // Wait for version message.
    let version_msg = match tokio::time::timeout(read_timeout, ws_stream.next()).await {
        Ok(Some(Ok(AxumMessage::Text(text)))) => text,
        _ => {
            warn!(gateway_id = %gateway_id, "Did not receive version message");
            return;
        }
    };

    debug!(gateway_id = %gateway_id, msg = %version_msg, "Received BS version");

    // Send router_config response.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let rc = RouterConfigMessage {
        msgtype: "router_config".into(),
        net_id: router_config.net_ids.clone(),
        join_eui: router_config.join_euis.clone(),
        region: region.clone(),
        hwspec: "sx1301/1".into(),
        freq_range: router_config.freq_range,
        drs: router_config.drs.clone(),
        mux_time: now,
    };

    let rc_json = serde_json::to_string(&rc).unwrap_or_default();
    if let Err(e) = ws_sink.send(AxumMessage::Text(rc_json.into())).await {
        error!(gateway_id = %gateway_id, error = %e, "Failed to send router_config");
        return;
    }

    info!(gateway_id = %gateway_id, "Sent router_config to BS gateway");

    // Create downlink channel and register gateway.
    let (dl_tx, mut dl_rx) = unbounded_channel::<String>();

    {
        let gateways = BS_GATEWAYS
            .get_or_init(|| async { RwLock::new(HashMap::new()) })
            .await;
        let mut gateways = gateways.write().await;
        gateways.insert(
            gateway_id,
            BsGatewayInfo {
                region: region.clone(),
                downlink_sender: dl_tx,
                last_xtime: 0,
                last_rctx: 0,
                dr_table: dr_table.clone(),
            },
        );
    }

    // Send synthetic PULL_DATA so UDP servers know about this gateway.
    if let Err(e) = forwarder::send_pull_data_for_gateway(gateway_id).await {
        error!(gateway_id = %gateway_id, error = %e, "Failed to send PULL_DATA for BS gateway");
    }

    // Spawn a PULL_DATA keepalive task.
    let pull_data_gw_id = gateway_id;
    let pull_data_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            if let Err(e) = forwarder::send_pull_data_for_gateway(pull_data_gw_id).await {
                debug!(gateway_id = %pull_data_gw_id, error = %e, "PULL_DATA keepalive failed");
                break;
            }
        }
    });

    // Message loop.
    let mut ping_interval_timer = tokio::time::interval(ping_interval);

    loop {
        tokio::select! {
            msg = tokio::time::timeout(read_timeout, ws_stream.next()) => {
                match msg {
                    Ok(Some(Ok(AxumMessage::Text(text)))) => {
                        if let Err(e) = handle_bs_message(
                            gateway_id,
                            &text,
                            &region,
                            &dr_table,
                            &uplink_tx,
                            &mut ws_sink,
                            &allow_deny_filters,
                        ).await {
                            error!(gateway_id = %gateway_id, error = %e, "Handle BS message error");
                        }
                    }
                    Ok(Some(Ok(AxumMessage::Ping(_)))) => {
                        // Pong is sent automatically by axum.
                    }
                    Ok(Some(Ok(AxumMessage::Close(_)))) | Ok(None) => {
                        info!(gateway_id = %gateway_id, "BS gateway disconnected");
                        break;
                    }
                    Ok(Some(Err(e))) => {
                        warn!(gateway_id = %gateway_id, error = %e, "BS WS receive error");
                        break;
                    }
                    Err(_) => {
                        warn!(gateway_id = %gateway_id, "BS gateway read timeout");
                        break;
                    }
                    _ => {}
                }
            }
            Some(dl_json) = dl_rx.recv() => {
                if let Err(e) = ws_sink.send(AxumMessage::Text(dl_json.into())).await {
                    error!(gateway_id = %gateway_id, error = %e, "Failed to send BS downlink");
                    break;
                }
            }
            _ = ping_interval_timer.tick() => {
                if let Err(e) = ws_sink.send(AxumMessage::Ping(vec![].into())).await {
                    debug!(gateway_id = %gateway_id, error = %e, "BS ping failed");
                    break;
                }
            }
        }
    }

    // Cleanup.
    pull_data_handle.abort();
    {
        let gateways = BS_GATEWAYS
            .get_or_init(|| async { RwLock::new(HashMap::new()) })
            .await;
        let mut gateways = gateways.write().await;
        gateways.remove(&gateway_id);
    }
    info!(gateway_id = %gateway_id, "BS gateway deregistered");
}

async fn handle_bs_message(
    gateway_id: GatewayId,
    text: &str,
    region: &str,
    dr_table: &[[i32; 3]],
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    ws_sink: &mut futures_util::stream::SplitSink<WebSocket, AxumMessage>,
    allow_deny_filters: &AllowDenyFilters,
) -> Result<()> {
    let generic: GenericMessage =
        serde_json::from_str(text).context("Parse BS message msgtype")?;

    match generic.msgtype.as_str() {
        "updf" => {
            let updf: UplinkDataFrame =
                serde_json::from_str(text).context("Parse updf")?;
            handle_updf(gateway_id, updf, text, region, dr_table, uplink_tx, allow_deny_filters).await?;
        }
        "jreq" => {
            let jreq: JoinRequest =
                serde_json::from_str(text).context("Parse jreq")?;
            handle_jreq(gateway_id, jreq, text, region, dr_table, uplink_tx, allow_deny_filters).await?;
        }
        "dntxed" => {
            let dntxed: DownlinkTransmitted =
                serde_json::from_str(text).context("Parse dntxed")?;
            info!(gateway_id = %gateway_id, diid = dntxed.diid, "BS downlink transmitted");
        }
        "timesync" => {
            let req: TimeSyncRequest =
                serde_json::from_str(text).context("Parse timesync")?;
            let gps_time = gps_time_now();
            let resp = TimeSyncResponse {
                msgtype: "timesync".into(),
                txtime: req.txtime,
                gps_time,
            };
            let json: String = serde_json::to_string(&resp)?;
            ws_sink
                .send(AxumMessage::Text(json.into()))
                .await
                .context("Send timesync response")?;
        }
        "version" => {
            debug!(gateway_id = %gateway_id, "Received additional version message (ignoring)");
        }
        other => {
            debug!(gateway_id = %gateway_id, msgtype = %other, "Unknown BS message type");
        }
    }

    Ok(())
}

async fn handle_updf(
    gateway_id: GatewayId,
    updf: UplinkDataFrame,
    original_text: &str,
    region: &str,
    dr_table: &[[i32; 3]],
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    allow_deny_filters: &AllowDenyFilters,
) -> Result<()> {
    // Update last xtime/rctx.
    {
        let gateways = BS_GATEWAYS
            .get_or_init(|| async { RwLock::new(HashMap::new()) })
            .await;
        let mut gateways = gateways.write().await;
        if let Some(info) = gateways.get_mut(&gateway_id) {
            info.last_xtime = updf.upinfo.xtime;
            info.last_rctx = updf.upinfo.rctx;
        }
    }

    let phy = reconstruct_phy_payload_updf(&updf)?;

    // Apply input-level filters.
    if !allow_deny_filters.dev_addr_deny.is_empty()
        || !allow_deny_filters.join_eui_deny.is_empty()
        || !allow_deny_filters.dev_addr_prefixes.is_empty()
        || !allow_deny_filters.join_eui_prefixes.is_empty()
    {
        if !allow_deny_filters.matches(&phy) {
            debug!(gateway_id = %gateway_id, "BS updf filtered out");
            return Ok(());
        }
    }

    let dev_addr = format!(
        "{:02x}{:02x}{:02x}{:02x}",
        (updf.dev_addr >> 24) & 0xFF,
        (updf.dev_addr >> 16) & 0xFF,
        (updf.dev_addr >> 8) & 0xFF,
        updf.dev_addr & 0xFF,
    );

    info!(
        gateway_id = %gateway_id,
        dev_addr = %dev_addr,
        f_port = updf.fport,
        dr = updf.dr,
        freq = updf.freq,
        "Received BS updf"
    );

    // BS→BS passthrough: forward original JSON directly to BS outputs.
    if let Err(e) = send_uplink_frame_direct(gateway_id, original_text, &phy).await {
        error!(error = %e, "BS direct uplink send error");
    }

    // Cross-protocol: convert to GWMP binary for GWMP/MQTT outputs.
    let push_data = updf_to_push_data(&updf, &phy, &gateway_id, dr_table)?;
    let data = push_data.to_bytes();

    uplink_tx
        .send((gateway_id, data, region.to_string(), None))
        .context("Uplink channel send")?;

    Ok(())
}

async fn handle_jreq(
    gateway_id: GatewayId,
    jreq: JoinRequest,
    original_text: &str,
    region: &str,
    dr_table: &[[i32; 3]],
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>)>,
    allow_deny_filters: &AllowDenyFilters,
) -> Result<()> {
    // Update last xtime/rctx.
    {
        let gateways = BS_GATEWAYS
            .get_or_init(|| async { RwLock::new(HashMap::new()) })
            .await;
        let mut gateways = gateways.write().await;
        if let Some(info) = gateways.get_mut(&gateway_id) {
            info.last_xtime = jreq.upinfo.xtime;
            info.last_rctx = jreq.upinfo.rctx;
        }
    }

    let phy = reconstruct_phy_payload_jreq(&jreq)?;

    // Apply input-level filters.
    if !allow_deny_filters.join_eui_deny.is_empty()
        || !allow_deny_filters.join_eui_prefixes.is_empty()
    {
        if !allow_deny_filters.matches(&phy) {
            debug!(gateway_id = %gateway_id, "BS jreq filtered out");
            return Ok(());
        }
    }

    info!(
        gateway_id = %gateway_id,
        join_eui = format!("{:016x}", jreq.join_eui),
        dev_eui = format!("{:016x}", jreq.dev_eui),
        dr = jreq.dr,
        freq = jreq.freq,
        "Received BS jreq"
    );

    // BS→BS passthrough: forward original JSON directly to BS outputs.
    if let Err(e) = send_uplink_frame_direct(gateway_id, original_text, &phy).await {
        error!(error = %e, "BS direct uplink send error");
    }

    // Cross-protocol: convert to GWMP binary for GWMP/MQTT outputs.
    let push_data = jreq_to_push_data(&jreq, &phy, &gateway_id, dr_table)?;
    let data = push_data.to_bytes();

    uplink_tx
        .send((gateway_id, data, region.to_string(), None))
        .context("Uplink channel send")?;

    Ok(())
}

// ----- PHY Payload Reconstruction -----

fn reconstruct_phy_payload_updf(updf: &UplinkDataFrame) -> Result<Vec<u8>> {
    let mut phy = Vec::new();

    // MHDR (1 byte)
    phy.push(updf.mhdr);

    // DevAddr (4 bytes, little-endian)
    let dev_addr = updf.dev_addr as u32;
    phy.extend_from_slice(&dev_addr.to_le_bytes());

    // FCtrl (1 byte)
    phy.push(updf.fctrl);

    // FCnt (2 bytes, little-endian)
    phy.extend_from_slice(&updf.fcnt.to_le_bytes());

    // FOpts (variable, hex-encoded)
    if !updf.fopts.is_empty() {
        let fopts_bytes = hex::decode(&updf.fopts).unwrap_or_default();
        phy.extend_from_slice(&fopts_bytes);
    }

    // FPort (1 byte, only if >= 0)
    if updf.fport >= 0 {
        phy.push(updf.fport as u8);
    }

    // FRMPayload (hex-encoded)
    if !updf.frm_payload.is_empty() {
        let payload_bytes = hex::decode(&updf.frm_payload).unwrap_or_default();
        phy.extend_from_slice(&payload_bytes);
    }

    // MIC (4 bytes, little-endian)
    let mic = updf.mic as u32;
    phy.extend_from_slice(&mic.to_le_bytes());

    Ok(phy)
}

fn reconstruct_phy_payload_jreq(jreq: &JoinRequest) -> Result<Vec<u8>> {
    let mut phy = Vec::new();

    // MHDR (1 byte)
    phy.push(jreq.mhdr);

    // JoinEUI (8 bytes, little-endian)
    phy.extend_from_slice(&jreq.join_eui.to_le_bytes());

    // DevEUI (8 bytes, little-endian)
    phy.extend_from_slice(&jreq.dev_eui.to_le_bytes());

    // DevNonce (2 bytes, little-endian)
    phy.extend_from_slice(&jreq.dev_nonce.to_le_bytes());

    // MIC (4 bytes, little-endian)
    let mic = jreq.mic as u32;
    phy.extend_from_slice(&mic.to_le_bytes());

    Ok(phy)
}

// ----- Conversions -----

fn dr_to_datr(dr: u8, dr_table: &[[i32; 3]]) -> Option<String> {
    if (dr as usize) >= dr_table.len() {
        return None;
    }
    let entry = dr_table[dr as usize];
    let sf = entry[0];
    let bw = entry[1];

    if sf < 0 {
        return None; // Unused DR slot
    }

    if sf == 0 && bw == 0 {
        // FSK
        Some("50000".to_string())
    } else {
        Some(format!("SF{}BW{}", sf, bw))
    }
}

fn datr_to_dr(datr: &serde_json::Value, dr_table: &[[i32; 3]]) -> u8 {
    match datr {
        serde_json::Value::String(s) => {
            // Parse "SF7BW125" -> look up in DR table
            let sf = s
                .strip_prefix("SF")
                .and_then(|rest| {
                    rest.chars()
                        .take_while(|c| c.is_ascii_digit())
                        .collect::<String>()
                        .parse::<i32>()
                        .ok()
                })
                .unwrap_or(7);
            let bw = s
                .find("BW")
                .and_then(|i| s[i + 2..].parse::<i32>().ok())
                .unwrap_or(125);

            for (idx, entry) in dr_table.iter().enumerate() {
                if entry[0] == sf && entry[1] == bw {
                    return idx as u8;
                }
            }
            0
        }
        serde_json::Value::Number(_) => {
            // FSK — find the FSK entry (sf=0, bw=0)
            for (idx, entry) in dr_table.iter().enumerate() {
                if entry[0] == 0 && entry[1] == 0 {
                    return idx as u8;
                }
            }
            0
        }
        _ => 0,
    }
}

fn updf_to_push_data(
    updf: &UplinkDataFrame,
    phy: &[u8],
    gateway_id: &GatewayId,
    dr_table: &[[i32; 3]],
) -> Result<PushData> {
    let datr = dr_to_datr(updf.dr, dr_table);
    let (datr_val, modu, codr) = match &datr {
        Some(s) if s.starts_with("SF") => (
            Some(serde_json::Value::String(s.clone())),
            Some("LORA".to_string()),
            Some("4/5".to_string()),
        ),
        Some(s) => (
            Some(serde_json::Value::Number(
                serde_json::Number::from(s.parse::<u32>().unwrap_or(50000)),
            )),
            Some("FSK".to_string()),
            None,
        ),
        None => (None, None, None),
    };

    let rxpk = RxPk {
        data: phy.to_vec(),
        tmst: Some((updf.upinfo.xtime & 0xFFFFFFFF) as u32),
        freq: Some(updf.freq as f64 / 1_000_000.0),
        rssi: Some(updf.upinfo.rssi as i32),
        lsnr: Some(updf.upinfo.snr),
        datr: datr_val,
        modu,
        codr,
        stat: Some(1), // CRC OK
        size: Some(phy.len() as u16),
        ..Default::default()
    };

    Ok(PushData {
        protocol_version: 0x02,
        random_token: rand::random(),
        gateway_id: *gateway_id.as_bytes(),
        payload: PushDataPayload::new(vec![rxpk]),
    })
}

fn jreq_to_push_data(
    jreq: &JoinRequest,
    phy: &[u8],
    gateway_id: &GatewayId,
    dr_table: &[[i32; 3]],
) -> Result<PushData> {
    let datr = dr_to_datr(jreq.dr, dr_table);
    let (datr_val, modu, codr) = match &datr {
        Some(s) if s.starts_with("SF") => (
            Some(serde_json::Value::String(s.clone())),
            Some("LORA".to_string()),
            Some("4/5".to_string()),
        ),
        Some(s) => (
            Some(serde_json::Value::Number(
                serde_json::Number::from(s.parse::<u32>().unwrap_or(50000)),
            )),
            Some("FSK".to_string()),
            None,
        ),
        None => (None, None, None),
    };

    let rxpk = RxPk {
        data: phy.to_vec(),
        tmst: Some((jreq.upinfo.xtime & 0xFFFFFFFF) as u32),
        freq: Some(jreq.freq as f64 / 1_000_000.0),
        rssi: Some(jreq.upinfo.rssi as i32),
        lsnr: Some(jreq.upinfo.snr),
        datr: datr_val,
        modu,
        codr,
        stat: Some(1),
        size: Some(phy.len() as u16),
        ..Default::default()
    };

    Ok(PushData {
        protocol_version: 0x02,
        random_token: rand::random(),
        gateway_id: *gateway_id.as_bytes(),
        payload: PushDataPayload::new(vec![rxpk]),
    })
}

fn pull_resp_to_dnmsg(pull_resp: &PullResp, info: &BsGatewayInfo) -> Result<DownlinkMessage> {
    let txpk = &pull_resp.txpk;

    // Convert datr to DR index.
    let dr = txpk
        .datr
        .as_ref()
        .map(|d| datr_to_dr(d, &info.dr_table))
        .unwrap_or(0);

    // Frequency in Hz.
    let freq_hz = (txpk.freq.unwrap_or(868.1) * 1_000_000.0) as u32;

    // PDU as hex string.
    let pdu = hex::encode(&txpk.data);

    // RxDelay: extract from timing. For delayed transmissions, the delay in seconds.
    let rx_delay = 1u8; // Default RX1 delay

    Ok(DownlinkMessage {
        msgtype: "dnmsg".into(),
        dev_eui: String::new(),
        dc: 0, // Class A
        diid: pull_resp.random_token as u64,
        pdu,
        rx_delay,
        rx1dr: dr,
        rx1freq: freq_hz,
        rx2dr: dr, // Simplified: same DR for RX2
        rx2freq: freq_hz,
        xtime: info.last_xtime,
        rctx: info.last_rctx,
        priority: 0,
        dr: None,
        freq: None,
        gpstime: None,
    })
}

// ----- Output (WS Client) -----

async fn setup_output(
    conf: &config::BasicsOutput,
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
) -> Result<()> {
    info!(server = %conf.server, "Setting up Basic Station output");

    let gateway_id_filters = GatewayIdFilters {
        allow: conf.gateway_id_prefixes.clone(),
        deny: conf.gateway_id_deny.clone(),
    };
    let allow_deny_filters = AllowDenyFilters {
        dev_addr_prefixes: conf.filters.dev_addr_prefixes.clone(),
        dev_addr_deny: conf.filters.dev_addr_deny.clone(),
        join_eui_prefixes: conf.filters.join_eui_prefixes.clone(),
        join_eui_deny: conf.filters.join_eui_deny.clone(),
    };

    // Parse gateway_tokens hex keys into GatewayId values.
    let mut gateway_tokens = HashMap::new();
    for (hex_key, header_value) in &conf.gateway_tokens {
        let gw_id = GatewayId::from_hex(hex_key)
            .with_context(|| format!("Invalid gateway ID in gateway_tokens: {}", hex_key))?;
        gateway_tokens.insert(gw_id, header_value.clone());
    }

    if !gateway_tokens.is_empty() {
        info!(
            server = %conf.server,
            count = gateway_tokens.len(),
            "Configured per-gateway auth tokens (implicit allow list)"
        );
    }

    let (uplink_sender, uplink_rx) = unbounded_channel::<(GatewayId, BsUplinkData, String)>();
    let (dntxed_sender, dntxed_rx) = unbounded_channel::<(GatewayId, u64)>();

    let state_index;
    {
        let states = STATES
            .get()
            .ok_or_else(|| anyhow!("STATES not initialized"))?;
        let mut states = states.write().await;
        state_index = states.len();
        states.push(OutputState {
            server: conf.server.clone(),
            uplink_only: conf.uplink_only,
            gateway_id_filters,
            allow_deny_filters,
            gateway_tokens: gateway_tokens.clone(),
            uplink_sender,
            dntxed_sender,
        });
    }

    let server = conf.server.clone();
    let reconnect_interval = conf.reconnect_interval;
    let ping_interval = conf.ping_interval;
    let uplink_only = conf.uplink_only;

    // Build TLS connector for wss:// connections.
    let tls_connector = if server.starts_with("wss://") {
        let tls_config = build_ws_tls_config(&conf.ca_cert, &conf.tls_cert, &conf.tls_key)
            .context("Build TLS config for BS output")?;
        Some(Connector::Rustls(Arc::new(tls_config)))
    } else {
        None
    };

    tokio::spawn(output_connection_loop(
        server,
        reconnect_interval,
        ping_interval,
        uplink_only,
        downlink_tx,
        uplink_rx,
        dntxed_rx,
        state_index,
        gateway_tokens,
        tls_connector,
    ));

    Ok(())
}

async fn output_connection_loop(
    server: String,
    reconnect_interval: Duration,
    ping_interval: Duration,
    uplink_only: bool,
    downlink_tx: UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    mut uplink_rx: tokio::sync::mpsc::UnboundedReceiver<(GatewayId, BsUplinkData, String)>,
    mut dntxed_rx: tokio::sync::mpsc::UnboundedReceiver<(GatewayId, u64)>,
    state_index: usize,
    gateway_tokens: HashMap<GatewayId, String>,
    tls_connector: Option<Connector>,
) {
    loop {
        info!(server = %server, "Connecting to BS output");

        match output_connect_and_run(
            &server,
            ping_interval,
            uplink_only,
            &downlink_tx,
            &mut uplink_rx,
            &mut dntxed_rx,
            state_index,
            &gateway_tokens,
            tls_connector.clone(),
        )
        .await
        {
            Ok(()) => {
                info!(server = %server, "BS output connection closed");
            }
            Err(e) => {
                error!(server = %server, error = %e, "BS output connection error");
            }
        }

        info!(server = %server, interval = ?reconnect_interval, "Reconnecting to BS output");
        tokio::time::sleep(reconnect_interval).await;
    }
}

async fn output_connect_and_run(
    server: &str,
    ping_interval: Duration,
    uplink_only: bool,
    downlink_tx: &UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    uplink_rx: &mut tokio::sync::mpsc::UnboundedReceiver<(GatewayId, BsUplinkData, String)>,
    dntxed_rx: &mut tokio::sync::mpsc::UnboundedReceiver<(GatewayId, u64)>,
    state_index: usize,
    gateway_tokens: &HashMap<GatewayId, String>,
    tls_connector: Option<Connector>,
) -> Result<()> {
    // Connect to the LNS. We need to derive the WS URL.
    // The server config is something like "wss://lns.example.com:3001" or "ws://..."
    // We connect to {server}/router-info first, then to {server}/gateway/{gw_id} per gateway.
    // For simplicity, this output maintains a single connection that forwards all matched gateways.
    // We'll connect per-gateway when we receive uplinks.

    // This is a simplified implementation: we forward uplinks as they come.
    // Each uplink creates/reuses a connection per gateway.

    let mut connections: HashMap<GatewayId, OutputGatewayConn> = HashMap::new();
    let mut ping_timer = tokio::time::interval(ping_interval);

    // Aggregated channel for WebSocket messages from all per-gateway reader tasks.
    let (ws_event_tx, mut ws_event_rx) = unbounded_channel::<WsEvent>();

    loop {
        tokio::select! {
            Some((gateway_id, uplink_data, _region)) = uplink_rx.recv() => {
                // Get or create connection for this gateway.
                let conn = match connections.get_mut(&gateway_id) {
                    Some(c) if !c.closed => c,
                    _ => {
                        // Create new connection.
                        let auth_header = gateway_tokens.get(&gateway_id).map(|s| s.as_str());
                        match create_output_connection(server, gateway_id, auth_header, tls_connector.clone(), ws_event_tx.clone()).await {
                            Ok(c) => {
                                info!(server = %server, gateway_id = %gateway_id, "BS output connection established");
                                connections.insert(gateway_id, c);
                                connections.get_mut(&gateway_id).unwrap()
                            }
                            Err(e) => {
                                error!(server = %server, gateway_id = %gateway_id, error = format!("{:#}", e), "Failed to connect BS output");
                                continue;
                            }
                        }
                    }
                };

                match uplink_data {
                    BsUplinkData::RawJson(json) => {
                        // BS→BS passthrough: send original JSON directly.
                        if let Err(e) = conn.sink.send(TungsteniteMessage::Text(json.into())).await {
                            error!(server = %server, gateway_id = %gateway_id, error = %e, "BS output send error");
                            conn.closed = true;
                        }
                    }
                    BsUplinkData::GwmpBytes(data) => {
                        // Cross-protocol: parse GWMP bytes and convert to BS messages.
                        if let Ok(push_data) = PushData::from_slice(&data) {
                            for rxpk in &push_data.payload.rxpk {
                                let bs_msg = rxpk_to_bs_message(rxpk, &gateway_id, &conn.dr_table);
                                if let Some(json) = bs_msg {
                                    if let Err(e) = conn.sink.send(TungsteniteMessage::Text(json.into())).await {
                                        error!(server = %server, gateway_id = %gateway_id, error = %e, "BS output send error");
                                        conn.closed = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Receive downlinks from LNS continuously via per-gateway reader tasks.
            Some(event) = ws_event_rx.recv() => {
                match event {
                    WsEvent::Message(gateway_id, text) => {
                        info!(server = %server, gateway_id = %gateway_id, uplink_only = uplink_only, "BS output received WsEvent::Message");
                        if !uplink_only {
                            if let Some(conn) = connections.get(&gateway_id) {
                                if let Err(e) = handle_output_message(
                                    server, gateway_id, &text, downlink_tx, &conn.dr_table, state_index,
                                ).await {
                                    error!(server = %server, gateway_id = %gateway_id, error = %e, "Handle BS output message error");
                                }
                            } else {
                                warn!(server = %server, gateway_id = %gateway_id, "BS output received message but no connection found");
                            }
                        }
                    }
                    WsEvent::Closed(gateway_id) => {
                        info!(server = %server, gateway_id = %gateway_id, "BS output connection closed (reader task ended)");
                        if let Some(conn) = connections.get_mut(&gateway_id) {
                            conn.closed = true;
                        }
                    }
                }
            }

            // Send dntxed back to LNS when a gateway acknowledges a downlink.
            Some((gateway_id, diid)) = dntxed_rx.recv() => {
                if let Some(conn) = connections.get_mut(&gateway_id) {
                    if !conn.closed {
                        let dntxed = serde_json::json!({
                            "msgtype": "dntxed",
                            "diid": diid
                        });
                        if let Ok(json) = serde_json::to_string(&dntxed) {
                            if let Err(e) = conn.sink.send(TungsteniteMessage::Text(json.into())).await {
                                error!(server = %server, gateway_id = %gateway_id, error = %e, "Failed to send dntxed");
                                conn.closed = true;
                            } else {
                                info!(server = %server, gateway_id = %gateway_id, diid = diid, "Sent dntxed to LNS");
                            }
                        }
                    }
                }
            }

            _ = ping_timer.tick() => {
                // Send pings and clean up closed connections.
                let mut to_remove = Vec::new();
                for (gw_id, conn) in connections.iter_mut() {
                    if conn.closed {
                        to_remove.push(*gw_id);
                        continue;
                    }
                    if let Err(e) = conn.sink.send(TungsteniteMessage::Ping(vec![].into())).await {
                        debug!(server = %server, gateway_id = %gw_id, error = %e, "BS output ping failed");
                        conn.closed = true;
                        to_remove.push(*gw_id);
                    }
                }
                for gw_id in to_remove {
                    connections.remove(&gw_id);
                }
            }
        }
    }
}

/// Events from per-gateway WebSocket reader tasks.
enum WsEvent {
    /// A text message received from the LNS.
    Message(GatewayId, String),
    /// The reader task has ended (connection closed or errored).
    Closed(GatewayId),
}

struct OutputGatewayConn {
    sink: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio_tungstenite::tungstenite::Message,
    >,
    dr_table: Vec<[i32; 3]>,
    closed: bool,
}

async fn create_output_connection(
    server: &str,
    gateway_id: GatewayId,
    auth_header: Option<&str>,
    tls_connector: Option<Connector>,
    ws_event_tx: UnboundedSender<WsEvent>,
) -> Result<OutputGatewayConn> {
    // Step 1: Query /router-info to discover the traffic endpoint.
    let router_info_url = format!("{}/router-info", server.trim_end_matches('/'));
    info!(server = %server, gateway_id = %gateway_id, "Querying router-info");

    let (mut ri_stream, _) = if let Some(header_line) = auth_header {
        let (name, value) = header_line
            .split_once(':')
            .ok_or_else(|| anyhow!("Invalid auth header format (expected 'Name: value'): {}", header_line))?;
        let name = name.trim();
        let value = value.trim();

        let request = tokio_tungstenite::tungstenite::http::Request::builder()
            .uri(&router_info_url)
            .header("Host", router_info_url.split("//").nth(1).and_then(|s| s.split('/').next()).unwrap_or(""))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
            .header(name, value)
            .body(())
            .context("Build router-info request with auth header")?;

        tokio_tungstenite::connect_async_tls_with_config(request, None, false, tls_connector.clone())
            .await
            .context("Connect to router-info with auth")?
    } else {
        tokio_tungstenite::connect_async_tls_with_config(
            &router_info_url,
            None,
            false,
            tls_connector.clone(),
        )
        .await
        .context("Connect to router-info")?
    };

    // Format EUI as "XX-XX-XX-XX-XX-XX-XX-XX" (Basic Station id6 convention).
    let eui_hex = gateway_id.to_string();
    let eui_dashed: String = eui_hex
        .as_bytes()
        .chunks(2)
        .map(|c| std::str::from_utf8(c).unwrap())
        .collect::<Vec<_>>()
        .join("-");
    let ri_req = serde_json::json!({ "router": eui_dashed });
    ri_stream
        .send(TungsteniteMessage::Text(serde_json::to_string(&ri_req)?.into()))
        .await
        .context("Send router-info request")?;

    let traffic_uri = match ri_stream.next().await {
        Some(Ok(TungsteniteMessage::Text(text))) => {
            let resp: serde_json::Value =
                serde_json::from_str(&text).context("Parse router-info response")?;
            if let Some(err) = resp.get("error").and_then(|e| e.as_str()) {
                return Err(anyhow!("router-info error: {}", err));
            }
            resp.get("uri")
                .and_then(|u| u.as_str())
                .ok_or_else(|| anyhow!("router-info response missing 'uri' field: {}", text))?
                .to_string()
        }
        Some(Ok(msg)) => return Err(anyhow!("Unexpected router-info message: {:?}", msg)),
        Some(Err(e)) => return Err(anyhow!("router-info receive error: {}", e)),
        None => return Err(anyhow!("router-info connection closed without response")),
    };
    ri_stream.close(None).await.ok();

    info!(server = %server, gateway_id = %gateway_id, uri = %traffic_uri, "Got traffic URI from router-info");

    // Step 2: Connect to the traffic URI.
    let (ws_stream, _) = if let Some(header_line) = auth_header {
        // Parse "Header-Name: value" into name and value parts.
        let (name, value) = header_line
            .split_once(':')
            .ok_or_else(|| anyhow!("Invalid auth header format (expected 'Name: value'): {}", header_line))?;
        let name = name.trim();
        let value = value.trim();

        let request = tokio_tungstenite::tungstenite::http::Request::builder()
            .uri(&traffic_uri)
            .header("Host", traffic_uri.split("//").nth(1).and_then(|s| s.split('/').next()).unwrap_or(""))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
            .header(name, value)
            .body(())
            .context("Build WS request with auth header")?;

        tokio_tungstenite::connect_async_tls_with_config(request, None, false, tls_connector)
            .await
            .context("Connect to BS LNS traffic with auth")?
    } else {
        tokio_tungstenite::connect_async_tls_with_config(&traffic_uri, None, false, tls_connector)
            .await
            .context("Connect to BS LNS traffic")?
    };

    let (mut sink, mut stream) = ws_stream.split();

    // Send version message.
    let version = serde_json::json!({
        "msgtype": "version",
        "station": "lorawan-multiplexer-converter",
        "firmware": env!("CARGO_PKG_VERSION"),
        "protocol": 2,
        "model": "multiplexer",
        "features": ""
    });
    sink.send(TungsteniteMessage::Text(
        serde_json::to_string(&version)?.into(),
    ))
    .await
    .context("Send version to LNS")?;

    // Receive router_config.
    let mut dr_table = Vec::new();
    if let Some(Ok(TungsteniteMessage::Text(text))) = stream.next().await {
        if let Ok(rc) = serde_json::from_str::<RouterConfigMessage>(&text) {
            dr_table = rc.drs;
            info!(
                server = %server,
                gateway_id = %gateway_id,
                region = %rc.region,
                "Received router_config from LNS"
            );
        }
    }

    // Spawn a reader task that continuously reads from the WebSocket stream
    // and forwards messages to the aggregated channel.
    let gw_id = gateway_id;
    let server_name = server.to_string();
    tokio::spawn(async move {
        info!(server = %server_name, gateway_id = %gw_id, "BS output reader task started");
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(TungsteniteMessage::Text(text)) => {
                    info!(server = %server_name, gateway_id = %gw_id, len = text.len(), "BS output received text from LNS");
                    if ws_event_tx.send(WsEvent::Message(gw_id, text.to_string())).is_err() {
                        warn!(server = %server_name, gateway_id = %gw_id, "BS output WsEvent channel closed");
                        break;
                    }
                }
                Ok(TungsteniteMessage::Close(frame)) => {
                    info!(server = %server_name, gateway_id = %gw_id, frame = ?frame, "BS output connection closed by server");
                    break;
                }
                Err(e) => {
                    warn!(server = %server_name, gateway_id = %gw_id, error = %e, "BS output receive error");
                    break;
                }
                _ => {}
            }
        }
        info!(server = %server_name, gateway_id = %gw_id, "BS output reader task ended");
        // Signal that this connection is closed.
        let _ = ws_event_tx.send(WsEvent::Closed(gw_id));
    });

    Ok(OutputGatewayConn {
        sink,
        dr_table,
        closed: false,
    })
}

/// Build a rustls 0.23 ClientConfig for WebSocket TLS (tokio-tungstenite).
fn build_ws_tls_config(
    ca_cert: &str,
    tls_cert: &str,
    tls_key: &str,
) -> Result<rustls_v023::ClientConfig> {
    let mut root_store = rustls_v023::RootCertStore::empty();

    if !ca_cert.is_empty() {
        let ca_data = fs::read(ca_cert).context("Read CA certificate")?;
        let mut reader = BufReader::new(Cursor::new(ca_data));
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .context("Parse CA certificates")?;
        for cert in certs {
            root_store
                .add(cert)
                .context("Add CA certificate")?;
        }
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let builder = rustls_v023::ClientConfig::builder().with_root_certificates(root_store);

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

fn rxpk_to_bs_message(
    rxpk: &RxPk,
    _gateway_id: &GatewayId,
    dr_table: &[[i32; 3]],
) -> Option<String> {
    let phy = &rxpk.data;
    if phy.is_empty() {
        return None;
    }

    let mhdr = phy[0];
    let mtype = mhdr >> 5;

    let dr = rxpk
        .datr
        .as_ref()
        .map(|d| datr_to_dr(d, dr_table))
        .unwrap_or(0);
    let freq_hz = rxpk
        .freq
        .map(|f| (f * 1_000_000.0) as u32)
        .unwrap_or(868100000);

    let upinfo = UpInfo {
        rctx: 0,
        xtime: rxpk.tmst.unwrap_or(0) as u64,
        gpstime: 0,
        rssi: rxpk.rssi.unwrap_or(0) as f64,
        snr: rxpk.lsnr.unwrap_or(0.0),
    };

    match mtype {
        // Join Request
        0 => {
            if phy.len() < 23 {
                return None;
            }
            let join_eui = u64::from_le_bytes(phy[1..9].try_into().ok()?);
            let dev_eui = u64::from_le_bytes(phy[9..17].try_into().ok()?);
            let dev_nonce = u16::from_le_bytes(phy[17..19].try_into().ok()?);
            let mic = i32::from_le_bytes(phy[19..23].try_into().ok()?);

            let jreq = JoinRequest {
                msgtype: "jreq".into(),
                mhdr,
                join_eui,
                dev_eui,
                dev_nonce,
                mic,
                dr,
                freq: freq_hz,
                upinfo,
            };
            serde_json::to_string(&jreq).ok()
        }
        // Unconfirmed Data Up (2) or Confirmed Data Up (4)
        2 | 4 => {
            if phy.len() < 12 {
                return None;
            }
            let dev_addr = u32::from_le_bytes(phy[1..5].try_into().ok()?);
            let fctrl = phy[5];
            let fcnt = u16::from_le_bytes(phy[6..8].try_into().ok()?);
            let fopts_len = (fctrl & 0x0F) as usize;
            let mic_offset = phy.len() - 4;
            let mic = i32::from_le_bytes(phy[mic_offset..mic_offset + 4].try_into().ok()?);

            // FOpts
            let fopts_start = 8;
            let fopts_end = fopts_start + fopts_len;
            let fopts = if fopts_end <= mic_offset {
                hex::encode(&phy[fopts_start..fopts_end])
            } else {
                String::new()
            };

            // FPort and FRMPayload
            let payload_start = fopts_end;
            let (fport, frm_payload) = if payload_start < mic_offset {
                let fport = phy[payload_start] as i32;
                let frm_start = payload_start + 1;
                let frm_payload = if frm_start < mic_offset {
                    hex::encode(&phy[frm_start..mic_offset])
                } else {
                    String::new()
                };
                (fport, frm_payload)
            } else {
                (-1, String::new())
            };

            let updf = UplinkDataFrame {
                msgtype: "updf".into(),
                mhdr,
                dev_addr: dev_addr as i64,
                fctrl,
                fcnt,
                fopts,
                fport,
                frm_payload,
                mic,
                dr,
                freq: freq_hz,
                upinfo,
            };
            serde_json::to_string(&updf).ok()
        }
        _ => None,
    }
}

async fn handle_output_message(
    server: &str,
    gateway_id: GatewayId,
    text: &str,
    downlink_tx: &UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,
    dr_table: &[[i32; 3]],
    state_index: usize,
) -> Result<()> {
    let generic: GenericMessage =
        serde_json::from_str(text).context("Parse BS output message")?;

    match generic.msgtype.as_str() {
        "dnmsg" => {
            debug!(server = %server, gateway_id = %gateway_id, text = %text, "Raw dnmsg from LNS");
            let dnmsg: DownlinkMessage =
                serde_json::from_str(text).context("Parse dnmsg from LNS")?;

            let diid = dnmsg.diid;
            // The GWMP PULL_RESP token is only 16 bits, so the downlink_id that
            // arrives in the TX_ACK will be truncated to 16 bits. Store accordingly.
            let downlink_id = (diid as u16) as u32;

            info!(
                server = %server,
                gateway_id = %gateway_id,
                diid = diid,
                "Received dnmsg from BS LNS"
            );

            let pull_resp = dnmsg_to_pull_resp(&dnmsg, dr_table)?;

            // Store pending downlink so we can send dntxed back when TX_ACK arrives.
            {
                let pending = PENDING_BS_DOWNLINKS
                    .get()
                    .ok_or_else(|| anyhow!("PENDING_BS_DOWNLINKS not initialized"))?;
                let mut pending = pending.write().await;
                pending.insert(
                    (gateway_id, downlink_id),
                    PendingDntxed { diid, state_index },
                );
            }

            downlink_tx
                .send((gateway_id, pull_resp, Some(downlink_id)))
                .context("Downlink channel send")?;
        }
        "router_config" => {
            debug!(server = %server, "Received updated router_config from LNS");
        }
        other => {
            debug!(server = %server, msgtype = %other, "Unknown BS output message");
        }
    }

    Ok(())
}

fn dnmsg_to_pull_resp(dnmsg: &DownlinkMessage, dr_table: &[[i32; 3]]) -> Result<Vec<u8>> {
    let phy_payload = hex::decode(&dnmsg.pdu).context("Decode dnmsg PDU")?;

    // Use RX1DR for Class A/C, fall back to DR for Class B.
    let dr_index = if dnmsg.rx1dr > 0 || dnmsg.dc != 1 {
        dnmsg.rx1dr
    } else {
        dnmsg.dr.unwrap_or(0)
    };
    let datr = dr_to_datr(dr_index, dr_table);

    // Use RX1Freq for Class A/C, fall back to Freq for Class B.
    let freq_hz = if dnmsg.rx1freq > 0 {
        dnmsg.rx1freq
    } else {
        dnmsg.freq.unwrap_or(868100000)
    };
    let freq_mhz = freq_hz as f64 / 1_000_000.0;

    let (datr_json, modu, codr) = match &datr {
        Some(s) if s.starts_with("SF") => (
            serde_json::json!(s),
            "LORA",
            Some("4/5"),
        ),
        Some(s) => (
            serde_json::json!(s.parse::<u32>().unwrap_or(50000)),
            "FSK",
            None,
        ),
        None => (serde_json::json!("SF7BW125"), "LORA", Some("4/5")),
    };

    let mut txpk = serde_json::Map::new();
    txpk.insert("imme".into(), serde_json::json!(false));
    txpk.insert("tmst".into(), serde_json::json!((dnmsg.xtime & 0xFFFFFFFF) as u32));
    txpk.insert("freq".into(), serde_json::json!(freq_mhz));
    txpk.insert("rfch".into(), serde_json::json!(0));
    txpk.insert("powe".into(), serde_json::json!(14));
    txpk.insert("modu".into(), serde_json::json!(modu));
    txpk.insert("datr".into(), datr_json);
    if let Some(cr) = codr {
        txpk.insert("codr".into(), serde_json::json!(cr));
    }
    txpk.insert("ipol".into(), serde_json::json!(true));
    txpk.insert(
        "data".into(),
        serde_json::json!(general_purpose::STANDARD.encode(&phy_payload)),
    );
    txpk.insert("size".into(), serde_json::json!(phy_payload.len()));

    let payload_json = serde_json::to_string(&serde_json::json!({ "txpk": txpk }))?;

    let token = (dnmsg.diid as u16).to_le_bytes();
    let mut b = vec![0x02]; // Protocol version 2
    b.extend_from_slice(&token);
    b.push(0x03); // PULL_RESP identifier
    b.extend_from_slice(payload_json.as_bytes());

    Ok(b)
}

// ----- Helpers -----

fn gps_time_now() -> u64 {
    // GPS epoch: January 6, 1980 00:00:00 UTC
    // Offset from Unix epoch to GPS epoch in seconds
    const GPS_EPOCH_OFFSET: u64 = 315964800;
    let unix_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // GPS time doesn't include leap seconds, but for simplicity we use Unix time offset.
    // Current leap seconds offset (as of 2024): 18 seconds
    unix_secs.saturating_sub(GPS_EPOCH_OFFSET) + 18
}
