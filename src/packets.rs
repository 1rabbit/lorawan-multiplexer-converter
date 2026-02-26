use std::collections::HashMap;
use std::fmt;

use anyhow::{Result, anyhow};
use base64::{Engine as _, engine::general_purpose};
use chirpstack_api::gw;
use chirpstack_api::pbjson_types;
use serde::{Deserialize, Serialize};

use crate::filters::AllowDenyFilters;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketType {
    PushData,
    PushAck,
    PullData,
    PullResp,
    PullAck,
    TxAck,
}

impl From<PacketType> for u8 {
    fn from(p: PacketType) -> u8 {
        match p {
            PacketType::PushData => 0x00,
            PacketType::PushAck => 0x01,
            PacketType::PullData => 0x02,
            PacketType::PullResp => 0x03,
            PacketType::PullAck => 0x04,
            PacketType::TxAck => 0x05,
        }
    }
}

impl TryFrom<&[u8]> for PacketType {
    type Error = anyhow::Error;

    fn try_from(v: &[u8]) -> Result<PacketType> {
        if v.len() < 4 {
            return Err(anyhow!("At least 4 bytes are expected"));
        }

        Ok(match v[3] {
            0x00 => PacketType::PushData,
            0x01 => PacketType::PushAck,
            0x02 => PacketType::PullData,
            0x03 => PacketType::PullResp,
            0x04 => PacketType::PullAck,
            0x05 => PacketType::TxAck,
            _ => return Err(anyhow!("Invalid packet-type: {}", v[3])),
        })
    }
}

impl fmt::Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ProtocolVersion {
    Version1,
    Version2,
}

impl TryFrom<&[u8]> for ProtocolVersion {
    type Error = anyhow::Error;

    fn try_from(v: &[u8]) -> Result<ProtocolVersion> {
        if v.is_empty() {
            return Err(anyhow!("At least 1 byte is expected"));
        }

        Ok(match v[0] {
            0x01 => ProtocolVersion::Version1,
            0x02 => ProtocolVersion::Version2,
            _ => return Err(anyhow!("Unexpected protocol")),
        })
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct GatewayId([u8; 8]);

impl GatewayId {
    pub fn as_bytes_le(&self) -> [u8; 8] {
        let mut out = self.0;
        out.reverse(); // BE => LE
        out
    }

    pub fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }

    pub fn from_hex(s: &str) -> Result<Self> {
        let bytes = hex::decode(s)?;
        if bytes.len() != 8 {
            return Err(anyhow!("Gateway ID must be 8 bytes"));
        }
        let mut gateway_id: [u8; 8] = [0; 8];
        gateway_id.copy_from_slice(&bytes);
        Ok(GatewayId(gateway_id))
    }
}

impl TryFrom<&[u8]> for GatewayId {
    type Error = anyhow::Error;

    fn try_from(v: &[u8]) -> Result<GatewayId> {
        if v.len() < 12 {
            return Err(anyhow!("At least 12 bytes are expected"));
        }

        let mut gateway_id: [u8; 8] = [0; 8];
        gateway_id.copy_from_slice(&v[4..12]);
        Ok(GatewayId(gateway_id))
    }
}

impl fmt::Display for GatewayId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

pub fn get_random_token(v: &[u8]) -> Result<u16> {
    if v.len() < 3 {
        return Err(anyhow!("At least 3 bytes are expected"));
    }

    Ok(u16::from_le_bytes([v[1], v[2]]))
}

#[derive(Clone)]
pub struct PushData {
    pub protocol_version: u8,
    pub random_token: u16,
    pub gateway_id: [u8; 8],
    pub payload: PushDataPayload,
}

impl PushData {
    pub fn from_slice(b: &[u8]) -> Result<Self> {
        if b.len() < 14 {
            return Err(anyhow!("At least 14 bytes are expected"));
        }

        // Parse JSON via serde_json::Value first to handle duplicate keys.
        // Some gateways send malformed JSON with duplicate "stat" fields.
        // serde_json::Value keeps the last value for duplicates.
        let json_value: serde_json::Value = serde_json::from_slice(&b[12..])?;
        let payload: PushDataPayload = serde_json::from_value(json_value)?;

        Ok(PushData {
            protocol_version: b[0],
            random_token: u16::from_le_bytes([b[1], b[2]]),
            gateway_id: {
                let mut gateway_id: [u8; 8] = [0; 8];
                gateway_id.copy_from_slice(&b[4..12]);
                gateway_id
            },
            payload,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = vec![self.protocol_version];

        b.append(&mut self.random_token.to_le_bytes().to_vec());
        b.push(0x00); // PUSH_DATA identifier
        b.append(&mut self.gateway_id.to_vec());

        let mut j = serde_json::to_vec(&self.payload).unwrap();
        b.append(&mut j);

        b
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct PushDataPayload {
    pub rxpk: Vec<RxPk>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stat: Option<Stat>,

    // Capture all the other fields.
    #[serde(flatten)]
    other: HashMap<String, serde_json::Value>,
}

/// Gateway status/stats from PUSH_DATA
#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct Stat {
    /// UTC time of the gateway
    pub time: Option<String>,
    /// GPS latitude
    pub lati: Option<f64>,
    /// GPS longitude
    pub long: Option<f64>,
    /// GPS altitude (meters)
    pub alti: Option<i32>,
    /// Number of radio packets received
    pub rxnb: Option<u32>,
    /// Number of radio packets received with valid CRC
    pub rxok: Option<u32>,
    /// Number of radio packets forwarded
    pub rxfw: Option<u32>,
    /// Percentage of upstream datagrams acknowledged
    pub ackr: Option<f64>,
    /// Number of downlink datagrams received
    pub dwnb: Option<u32>,
    /// Number of packets emitted
    pub txnb: Option<u32>,
}

impl PushDataPayload {
    /// Create a new PushDataPayload with the given rxpk entries.
    pub fn new(rxpk: Vec<RxPk>) -> Self {
        PushDataPayload {
            rxpk,
            stat: None,
            other: HashMap::new(),
        }
    }

    pub fn filter_rxpk(&mut self, filter: &lrwn_filters::Filters) {
        self.rxpk = self
            .rxpk
            .drain(..)
            .filter(|v| lrwn_filters::matches(&v.data, filter))
            .collect();
    }

    /// Filter rxpk entries using allow/deny filter logic.
    pub fn filter_rxpk_allow_deny(&mut self, filter: &AllowDenyFilters) {
        self.rxpk = self
            .rxpk
            .drain(..)
            .filter(|v| filter.matches(&v.data))
            .collect();
    }

    pub fn is_empty(&self) -> bool {
        self.rxpk.is_empty() && self.other.is_empty()
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RxPk {
    // PHY payload (base64 encoded)
    #[serde(with = "base64_codec")]
    pub data: Vec<u8>,

    // Timestamp (internal concentrator counter)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tmst: Option<u32>,

    // UTC time of pkt RX (ISO 8601 compact format with microsecond precision)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,

    // GPS time of pkt RX (milliseconds since GPS epoch 1980-01-06)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tmms: Option<i64>,

    // Frequency in MHz
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freq: Option<f64>,

    // Concentrator RF chain
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rfch: Option<u32>,

    // Modulation ("LORA" or "FSK")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modu: Option<String>,

    // Data rate (LoRa: "SF7BW125", FSK: bitrate, LR-FHSS: "M0CW137")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datr: Option<serde_json::Value>,

    // Coding rate (LoRa only, e.g., "4/5", "4/6", "4/7", "4/8")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codr: Option<String>,

    // RSSI in dBm (signed integer)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rssi: Option<i32>,

    // SNR in dB (LoRa only, 0.1 dB precision)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsnr: Option<f64>,

    // Concentrator IF channel used for RX
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chan: Option<u32>,

    // CRC status: 1 = OK, -1 = fail, 0 = no CRC
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stat: Option<i8>,

    // Concentrator board used for RX
    #[serde(skip_serializing_if = "Option::is_none")]
    pub brd: Option<u32>,

    // Fine timestamp (nanoseconds) [0..999999999] for SX1302/SX1303
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ftime: Option<u32>,

    // LR-FHSS hopping grid number of steps
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hpw: Option<u8>,

    // AES key index for encrypted fine timestamps
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aesk: Option<u8>,

    // RF packet payload size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u16>,

    // Per-antenna received signal information (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rsig: Option<Vec<RSig>>,

    // Custom metadata (key-value pairs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, String>>,

    // Capture all the other fields.
    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

/// Per-antenna received signal information (from RSig array in RXPK)
#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RSig {
    // Antenna number
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ant: Option<u8>,

    // Concentrator IF channel used
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chan: Option<u8>,

    // RSSI in dBm of the channel
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rssic: Option<i16>,

    // LoRa SNR in dB
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsnr: Option<f32>,

    // Encrypted fine timestamp (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "base64_codec_opt")]
    pub etime: Option<Vec<u8>>,
}

mod base64_codec_opt {
    use base64::{Engine as _, engine::general_purpose};
    use serde::{Deserialize, Serialize};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(bytes) => {
                let base64 = general_purpose::STANDARD.encode(bytes);
                String::serialize(&base64, s)
            }
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let opt: Option<String> = Option::deserialize(d)?;
        match opt {
            Some(base64) => general_purpose::STANDARD
                .decode(base64.as_bytes())
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

mod base64_codec {
    use base64::{Engine as _, engine::general_purpose};
    use serde::{Deserialize, Serialize};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = general_purpose::STANDARD.encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        general_purpose::STANDARD
            .decode(base64.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

impl PushData {
    /// Convert Semtech PUSH_DATA to ChirpStack UplinkFrame messages.
    /// Returns one UplinkFrame per rxpk entry.
    pub fn to_uplink_frames(&self) -> Vec<gw::UplinkFrame> {
        let gateway_id = hex::encode(self.gateway_id);
        let uplink_id = self.random_token as u32;
        self.payload
            .rxpk
            .iter()
            .map(|rxpk| rxpk.to_uplink_frame_with_id(&gateway_id, uplink_id))
            .collect()
    }

    /// Convert stats to ChirpStack GatewayStats (if present).
    pub fn to_gateway_stats(&self) -> Option<gw::GatewayStats> {
        let stat = self.payload.stat.as_ref()?;
        let gateway_id = hex::encode(self.gateway_id);

        // Parse time
        let time = stat.time.as_ref().and_then(|t| {
            chrono::DateTime::parse_from_rfc3339(t)
                .or_else(|_| chrono::DateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S %Z"))
                .ok()
                .map(|dt| pbjson_types::Timestamp {
                    seconds: dt.timestamp(),
                    nanos: dt.timestamp_subsec_nanos() as i32,
                })
        });

        // Parse location
        let location = if stat.lati.is_some() || stat.long.is_some() || stat.alti.is_some() {
            Some(chirpstack_api::common::Location {
                latitude: stat.lati.unwrap_or(0.0),
                longitude: stat.long.unwrap_or(0.0),
                altitude: stat.alti.unwrap_or(0) as f64,
                source: chirpstack_api::common::LocationSource::Gps.into(),
                accuracy: 0.0,
            })
        } else {
            None
        };

        Some(gw::GatewayStats {
            gateway_id,
            time,
            location,
            rx_packets_received: stat.rxnb.unwrap_or(0),
            rx_packets_received_ok: stat.rxok.unwrap_or(0),
            tx_packets_received: stat.dwnb.unwrap_or(0),
            tx_packets_emitted: stat.txnb.unwrap_or(0),
            ..Default::default()
        })
    }
}

impl RxPk {
    /// Convert a single RxPk to a ChirpStack UplinkFrame.
    pub fn to_uplink_frame(&self, gateway_id: &str) -> gw::UplinkFrame {
        self.to_uplink_frame_with_id(gateway_id, 0)
    }

    /// Convert a single RxPk to a ChirpStack UplinkFrame with uplink_id.
    /// If rsig data is present, the first antenna's data is used (call to_uplink_frames_with_rsig
    /// for multi-antenna support).
    pub fn to_uplink_frame_with_id(&self, gateway_id: &str, uplink_id: u32) -> gw::UplinkFrame {
        let modulation = self.parse_modulation();

        // Parse CRC status
        let crc_status = match self.stat {
            Some(1) => gw::CrcStatus::CrcOk,
            Some(-1) => gw::CrcStatus::BadCrc,
            _ => gw::CrcStatus::NoCrc,
        };

        // Parse gw_time from time field (supports multiple formats)
        let gw_time = self.time.as_ref().and_then(|t| {
            chrono::DateTime::parse_from_rfc3339(t)
                .or_else(|_| chrono::DateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S %Z"))
                .or_else(|_| chrono::DateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.fZ"))
                .ok()
                .map(|dt| pbjson_types::Timestamp {
                    seconds: dt.timestamp(),
                    nanos: dt.timestamp_subsec_nanos() as i32,
                })
        });

        // Parse time_since_gps_epoch from tmms field (milliseconds since GPS epoch)
        let time_since_gps_epoch = self.tmms.map(|ms| {
            pbjson_types::Duration {
                seconds: ms / 1000,
                nanos: ((ms % 1000) * 1_000_000) as i32,
            }
        });

        // Plain fine-timestamp (SX1302 / SX1303)
        // Combine GPS time (seconds) with fine timestamp (nanoseconds)
        // This matches chirpstack-gateway-bridge behavior exactly
        let fine_time_since_gps_epoch = match (self.tmms, self.ftime) {
            (Some(ms), Some(ftime)) => {
                // Keep only seconds from GPS time (truncate milliseconds)
                let secs = ms / 1000;
                // Add the nanoseconds from fine-timestamp
                Some(pbjson_types::Duration {
                    seconds: secs,
                    nanos: ftime as i32,
                })
            }
            _ => None,
        };

        // Get RSSI, SNR, antenna, and channel from rsig if present, otherwise from top-level fields
        let (rssi, snr, antenna, channel) = if let Some(rsig) = &self.rsig {
            if let Some(first_rsig) = rsig.first() {
                (
                    first_rsig.rssic.map(|r| r as i32).unwrap_or(self.rssi.unwrap_or(0)),
                    first_rsig.lsnr.map(|s| s as f64).unwrap_or(self.lsnr.unwrap_or(0.0)),
                    first_rsig.ant.unwrap_or(0) as u32,
                    first_rsig.chan.map(|c| c as u32).unwrap_or(self.chan.unwrap_or(0)),
                )
            } else {
                (self.rssi.unwrap_or(0), self.lsnr.unwrap_or(0.0), 0, self.chan.unwrap_or(0))
            }
        } else {
            (self.rssi.unwrap_or(0), self.lsnr.unwrap_or(0.0), 0, self.chan.unwrap_or(0))
        };

        // Build metadata map from rxpk meta field
        let metadata = self.meta.clone().unwrap_or_default();

        gw::UplinkFrame {
            phy_payload: self.data.clone(),
            tx_info: Some(gw::UplinkTxInfo {
                frequency: self.freq.map(|f| (f * 1_000_000.0) as u32).unwrap_or(0),
                modulation: Some(gw::Modulation {
                    parameters: Some(modulation),
                }),
            }),
            rx_info: Some(gw::UplinkRxInfo {
                gateway_id: gateway_id.to_string(),
                uplink_id,
                rssi,
                snr: snr as f32,
                channel,
                rf_chain: self.rfch.unwrap_or(0),
                gw_time,
                ns_time: None,
                time_since_gps_epoch,
                fine_time_since_gps_epoch,
                context: self.tmst.map(|t| t.to_be_bytes().to_vec()).unwrap_or_default(),
                crc_status: crc_status.into(),
                metadata,
                antenna,
                board: self.brd.unwrap_or(0),
                location: None,
            }),
            tx_info_legacy: None,
            rx_info_legacy: None,
        }
    }

    /// Convert a single RxPk to multiple ChirpStack UplinkFrames, one per antenna (rsig entry).
    /// If no rsig data is present, returns a single frame.
    /// This matches chirpstack-gateway-bridge's behavior of creating separate frames per antenna.
    pub fn to_uplink_frames_with_rsig(&self, gateway_id: &str, uplink_id: u32) -> Vec<gw::UplinkFrame> {
        // If rsig is present, create one frame per antenna
        if let Some(rsig) = &self.rsig {
            if !rsig.is_empty() {
                return rsig.iter().map(|rs| {
                    self.to_uplink_frame_with_rsig_entry(gateway_id, uplink_id, rs)
                }).collect();
            }
        }

        // Otherwise return single frame
        vec![self.to_uplink_frame_with_id(gateway_id, uplink_id)]
    }

    /// Convert RxPk to UplinkFrame using a specific RSig entry for antenna-specific values.
    fn to_uplink_frame_with_rsig_entry(&self, gateway_id: &str, uplink_id: u32, rsig: &RSig) -> gw::UplinkFrame {
        let modulation = self.parse_modulation();

        // Parse CRC status
        let crc_status = match self.stat {
            Some(1) => gw::CrcStatus::CrcOk,
            Some(-1) => gw::CrcStatus::BadCrc,
            _ => gw::CrcStatus::NoCrc,
        };

        // Parse gw_time from time field
        let gw_time = self.time.as_ref().and_then(|t| {
            chrono::DateTime::parse_from_rfc3339(t)
                .or_else(|_| chrono::DateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S %Z"))
                .or_else(|_| chrono::DateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.fZ"))
                .ok()
                .map(|dt| pbjson_types::Timestamp {
                    seconds: dt.timestamp(),
                    nanos: dt.timestamp_subsec_nanos() as i32,
                })
        });

        // Parse time_since_gps_epoch from tmms field
        let time_since_gps_epoch = self.tmms.map(|ms| {
            pbjson_types::Duration {
                seconds: ms / 1000,
                nanos: ((ms % 1000) * 1_000_000) as i32,
            }
        });

        // Fine-timestamp handling
        let fine_time_since_gps_epoch = match (self.tmms, self.ftime) {
            (Some(ms), Some(ftime)) => {
                let secs = ms / 1000;
                Some(pbjson_types::Duration {
                    seconds: secs,
                    nanos: ftime as i32,
                })
            }
            _ => None,
        };

        // Override with rsig-specific values
        let rssi = rsig.rssic.map(|r| r as i32).unwrap_or(self.rssi.unwrap_or(0));
        let snr = rsig.lsnr.unwrap_or(self.lsnr.unwrap_or(0.0) as f32);
        let antenna = rsig.ant.unwrap_or(0) as u32;
        let channel = rsig.chan.map(|c| c as u32).unwrap_or(self.chan.unwrap_or(0));

        let metadata = self.meta.clone().unwrap_or_default();

        gw::UplinkFrame {
            phy_payload: self.data.clone(),
            tx_info: Some(gw::UplinkTxInfo {
                frequency: self.freq.map(|f| (f * 1_000_000.0) as u32).unwrap_or(0),
                modulation: Some(gw::Modulation {
                    parameters: Some(modulation),
                }),
            }),
            rx_info: Some(gw::UplinkRxInfo {
                gateway_id: gateway_id.to_string(),
                uplink_id,
                rssi,
                snr,
                channel,
                rf_chain: self.rfch.unwrap_or(0),
                gw_time,
                ns_time: None,
                time_since_gps_epoch,
                fine_time_since_gps_epoch,
                context: self.tmst.map(|t| t.to_be_bytes().to_vec()).unwrap_or_default(),
                crc_status: crc_status.into(),
                metadata,
                antenna,
                board: self.brd.unwrap_or(0),
                location: None,
            }),
            tx_info_legacy: None,
            rx_info_legacy: None,
        }
    }

    fn parse_modulation(&self) -> gw::modulation::Parameters {
        let modu = self.modu.as_deref().unwrap_or("LORA");

        if modu == "FSK" {
            // FSK modulation
            let bitrate = match &self.datr {
                Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(50000) as u32,
                _ => 50000,
            };
            gw::modulation::Parameters::Fsk(gw::FskModulationInfo {
                datarate: bitrate,
                frequency_deviation: 0,
            })
        } else if self.is_lr_fhss() {
            // LR-FHSS modulation
            let ocw = self.parse_lr_fhss_datr();
            let cr = self.parse_coding_rate();
            gw::modulation::Parameters::LrFhss(gw::LrFhssModulationInfo {
                operating_channel_width: ocw,
                code_rate: cr.into(),
                code_rate_legacy: String::new(),
                grid_steps: self.hpw.unwrap_or(0) as u32,
            })
        } else {
            // LoRa modulation (default)
            let (sf, bw) = self.parse_lora_datr();
            let cr = self.parse_coding_rate();
            gw::modulation::Parameters::Lora(gw::LoraModulationInfo {
                bandwidth: bw,
                spreading_factor: sf,
                code_rate: cr.into(),
                code_rate_legacy: String::new(),
                polarization_inversion: false,
                preamble: 0,
                no_crc: false,
            })
        }
    }

    fn is_lr_fhss(&self) -> bool {
        // LR-FHSS datarate format: "M0CW137" (Operating Channel Width in kHz)
        match &self.datr {
            Some(serde_json::Value::String(s)) => s.starts_with("M0CW"),
            _ => false,
        }
    }

    fn parse_lr_fhss_datr(&self) -> u32 {
        // Parse datarate string like "M0CW137" (Operating Channel Width in kHz)
        let datr = match &self.datr {
            Some(serde_json::Value::String(s)) => s.as_str(),
            _ => return 137000, // Default OCW
        };

        datr.strip_prefix("M0CW")
            .and_then(|s| s.parse::<u32>().ok())
            .map(|ocw| ocw * 1000) // Convert kHz to Hz
            .unwrap_or(137000)
    }

    fn parse_lora_datr(&self) -> (u32, u32) {
        // Parse datarate string like "SF7BW125" or "SF12BW500"
        let datr = match &self.datr {
            Some(serde_json::Value::String(s)) => s.as_str(),
            _ => "SF7BW125",
        };

        let sf = datr
            .strip_prefix("SF")
            .and_then(|s| s.chars().take_while(|c| c.is_ascii_digit()).collect::<String>().parse().ok())
            .unwrap_or(7);

        let bw = datr
            .find("BW")
            .and_then(|i| datr[i + 2..].parse::<u32>().ok())
            .map(|bw| bw * 1000) // Convert kHz to Hz
            .unwrap_or(125000);

        (sf, bw)
    }

    fn parse_coding_rate(&self) -> gw::CodeRate {
        match self.codr.as_deref() {
            Some("4/5") => gw::CodeRate::Cr45,
            Some("4/6") => gw::CodeRate::Cr46,
            Some("4/7") => gw::CodeRate::Cr47,
            Some("4/8") => gw::CodeRate::Cr48,
            Some("3/8") => gw::CodeRate::Cr38,
            Some("1/3") | Some("2/6") => gw::CodeRate::Cr26,
            Some("1/4") => gw::CodeRate::Cr14,
            Some("1/6") => gw::CodeRate::Cr16,
            Some("5/6") => gw::CodeRate::Cr56,
            Some("4/5LI") => gw::CodeRate::CrLi45,
            Some("4/6LI") => gw::CodeRate::CrLi46,
            Some("4/8LI") => gw::CodeRate::CrLi48,
            _ => gw::CodeRate::CrUndefined,
        }
    }

    /// Get a debug string describing the frame type, DevAddr, and FPort (if applicable).
    /// Returns format like "01337001:10" for data uplinks (DevAddr:FPort) or "join-req" for join requests.
    pub fn get_frame_info(&self) -> String {
        if self.data.is_empty() {
            return "empty".to_string();
        }

        let mhdr = self.data[0];
        let mtype = mhdr >> 5;

        match mtype {
            // Data uplink (unconfirmed=2, confirmed=4)
            2 | 4 => {
                if self.data.len() >= 5 {
                    // DevAddr is bytes 1-4 (little-endian, so reverse for display)
                    let dev_addr = format!("{:02x}{:02x}{:02x}{:02x}",
                        self.data[4], self.data[3], self.data[2], self.data[1]);
                    // FPort is at byte 8 (after MHDR[1] + DevAddr[4] + FCtrl[1] + FCnt[2])
                    // FPort is only present when there's a payload (data.len() > 8 + MIC)
                    // Minimum frame with FPort: MHDR(1) + DevAddr(4) + FCtrl(1) + FCnt(2) + FPort(1) + MIC(4) = 13
                    if self.data.len() >= 13 {
                        let fport = self.data[8];
                        format!("{}:{}", dev_addr, fport)
                    } else {
                        dev_addr
                    }
                } else {
                    "data:invalid".to_string()
                }
            }
            // Join request
            0 => "join-req".to_string(),
            // Join accept
            1 => "join-accept".to_string(),
            // Other
            _ => format!("mtype:{}", mtype),
        }
    }
}

/// Convert a ChirpStack DownlinkFrame to Semtech PULL_RESP bytes.
pub fn downlink_frame_to_pull_resp(frame: &gw::DownlinkFrame, _gateway_id: &GatewayId) -> Result<Vec<u8>> {
    // Get the first item from the downlink frame
    let item = frame.items.first().ok_or_else(|| anyhow!("No items in downlink frame"))?;
    let tx_info = item.tx_info.as_ref().ok_or_else(|| anyhow!("No tx_info in downlink item"))?;

    let txpk = build_txpk(item, tx_info)?;
    let txpk_json = serde_json::to_string(&serde_json::json!({ "txpk": txpk }))?;

    // Build PULL_RESP packet: [version, token1, token2, 0x03, txpk_json]
    // downlink_id is a u32, use lower 16 bits as token (little-endian)
    let token = (frame.downlink_id as u16).to_le_bytes();

    let mut b = vec![0x02]; // Protocol version 2
    b.extend_from_slice(&token);
    b.push(0x03); // PULL_RESP identifier
    b.extend_from_slice(txpk_json.as_bytes());

    Ok(b)
}

fn build_txpk(item: &gw::DownlinkFrameItem, tx_info: &gw::DownlinkTxInfo) -> Result<serde_json::Value> {
    let mut txpk = serde_json::Map::new();

    // Immediate or timestamped - use the Timing struct's parameters field
    if let Some(timing) = &tx_info.timing {
        match &timing.parameters {
            Some(gw::timing::Parameters::Delay(delay)) => {
                // Delayed transmission
                let ctx = &tx_info.context;
                if ctx.len() >= 4 {
                    let tmst = u32::from_be_bytes([ctx[0], ctx[1], ctx[2], ctx[3]]);
                    // Convert delay to microseconds (seconds * 1_000_000 + nanos / 1_000)
                    let delay_us = delay.delay.as_ref().map(|d| {
                        (d.seconds as u32) * 1_000_000 + (d.nanos as u32 / 1_000)
                    }).unwrap_or(0);
                    txpk.insert("tmst".into(), serde_json::json!(tmst.wrapping_add(delay_us)));
                }
            }
            Some(gw::timing::Parameters::Immediately(_)) => {
                txpk.insert("imme".into(), serde_json::json!(true));
            }
            Some(gw::timing::Parameters::GpsEpoch(gps)) => {
                if let Some(ts) = &gps.time_since_gps_epoch {
                    // Convert to GPS time
                    let gps_time = ts.seconds as f64 + ts.nanos as f64 / 1_000_000_000.0;
                    txpk.insert("tmms".into(), serde_json::json!((gps_time * 1000.0) as u64));
                }
            }
            None => {}
        }
    }

    // Frequency in MHz
    txpk.insert("freq".into(), serde_json::json!(tx_info.frequency as f64 / 1_000_000.0));

    // RF chain
    txpk.insert("rfch".into(), serde_json::json!(0));

    // Antenna
    txpk.insert("ant".into(), serde_json::json!(tx_info.antenna));

    // Board
    txpk.insert("brd".into(), serde_json::json!(tx_info.board));

    // Power in dBm
    txpk.insert("powe".into(), serde_json::json!(tx_info.power));

    // Modulation
    if let Some(modulation) = &tx_info.modulation {
        match &modulation.parameters {
            Some(gw::modulation::Parameters::Lora(lora)) => {
                txpk.insert("modu".into(), serde_json::json!("LORA"));
                let sf = lora.spreading_factor;
                let bw = lora.bandwidth / 1000; // Hz to kHz
                txpk.insert("datr".into(), serde_json::json!(format!("SF{}BW{}", sf, bw)));
                let codr = match gw::CodeRate::try_from(lora.code_rate).unwrap_or(gw::CodeRate::Cr45) {
                    gw::CodeRate::Cr45 => "4/5",
                    gw::CodeRate::Cr46 => "4/6",
                    gw::CodeRate::Cr47 => "4/7",
                    gw::CodeRate::Cr48 => "4/8",
                    gw::CodeRate::CrLi45 => "4/5LI",
                    gw::CodeRate::CrLi46 => "4/6LI",
                    gw::CodeRate::CrLi48 => "4/8LI",
                    _ => "4/5",
                };
                txpk.insert("codr".into(), serde_json::json!(codr));
                txpk.insert("ipol".into(), serde_json::json!(lora.polarization_inversion));
                if lora.preamble > 0 {
                    txpk.insert("prea".into(), serde_json::json!(lora.preamble));
                }
            }
            Some(gw::modulation::Parameters::Fsk(fsk)) => {
                txpk.insert("modu".into(), serde_json::json!("FSK"));
                txpk.insert("datr".into(), serde_json::json!(fsk.datarate));
                txpk.insert("fdev".into(), serde_json::json!(fsk.frequency_deviation));
            }
            Some(gw::modulation::Parameters::LrFhss(_)) => {
                return Err(anyhow!("LR-FHSS modulation not supported"));
            }
            None => {}
        }
    }

    // PHY payload (base64 encoded)
    txpk.insert("data".into(), serde_json::json!(general_purpose::STANDARD.encode(&item.phy_payload)));
    txpk.insert("size".into(), serde_json::json!(item.phy_payload.len()));

    Ok(serde_json::Value::Object(txpk))
}

/// Parsed PULL_RESP packet
pub struct PullResp {
    pub protocol_version: u8,
    pub random_token: u16,
    pub txpk: TxPk,
}

/// Transmit packet from PULL_RESP
#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TxPk {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imme: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tmst: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tmms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freq: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rfch: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub powe: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modu: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datr: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fdev: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipol: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prea: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ncrc: Option<bool>,
    #[serde(with = "base64_codec")]
    pub data: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u32>,
}

#[derive(Deserialize)]
struct PullRespPayload {
    txpk: TxPk,
}

impl PullResp {
    pub fn from_slice(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(anyhow!("PULL_RESP too short"));
        }

        if data[3] != 0x03 {
            return Err(anyhow!("Not a PULL_RESP packet"));
        }

        let protocol_version = data[0];
        let random_token = u16::from_le_bytes([data[1], data[2]]);

        let json_data = &data[4..];
        let payload: PullRespPayload = serde_json::from_slice(json_data)?;

        Ok(PullResp {
            protocol_version,
            random_token,
            txpk: payload.txpk,
        })
    }

    pub fn to_downlink_frame(&self, gateway_id: &GatewayId) -> Result<gw::DownlinkFrame> {
        self.to_downlink_frame_with_context(gateway_id, None, None)
    }

    /// Build a DownlinkFrame, optionally using original uplink context instead of
    /// the synthetic tmst-based context. When `original_context` is provided, the
    /// frame uses the gateway bridge's opaque context and computes the RxDelay from
    /// the difference between the target tmst and the original uplink tmst.
    pub fn to_downlink_frame_with_context(
        &self,
        gateway_id: &GatewayId,
        original_context: Option<Vec<u8>>,
        original_tmst: Option<u32>,
    ) -> Result<gw::DownlinkFrame> {
        let txpk = &self.txpk;

        // Determine context and timing based on whether we have original uplink info
        let (context, timing) = if txpk.imme == Some(true) {
            (
                Vec::new(),
                Some(gw::Timing {
                    parameters: Some(gw::timing::Parameters::Immediately(gw::ImmediatelyTimingInfo {})),
                }),
            )
        } else if let Some(target_tmst) = txpk.tmst {
            match (&original_context, original_tmst) {
                (Some(ctx), Some(orig_tmst)) if !ctx.is_empty() => {
                    // We have original uplink context — use it with computed delay
                    let delay_us = target_tmst.wrapping_sub(orig_tmst);
                    (
                        ctx.clone(),
                        Some(gw::Timing {
                            parameters: Some(gw::timing::Parameters::Delay(gw::DelayTimingInfo {
                                delay: Some(pbjson_types::Duration {
                                    seconds: (delay_us / 1_000_000) as i64,
                                    nanos: ((delay_us % 1_000_000) * 1_000) as i32,
                                }),
                            })),
                        }),
                    )
                }
                _ => {
                    // Fallback: use tmst as context with zero delay (original behavior)
                    (
                        target_tmst.to_be_bytes().to_vec(),
                        Some(gw::Timing {
                            parameters: Some(gw::timing::Parameters::Delay(gw::DelayTimingInfo {
                                delay: Some(pbjson_types::Duration {
                                    seconds: 0,
                                    nanos: 0,
                                }),
                            })),
                        }),
                    )
                }
            }
        } else if let Some(_tmms) = txpk.tmms {
            // GPS time not fully implemented
            (Vec::new(), None)
        } else {
            (Vec::new(), None)
        };

        // Build modulation
        let modulation = match txpk.modu.as_deref() {
            Some("LORA") => {
                let (sf, bw) = parse_datr_lora(&txpk.datr)?;
                let code_rate = match txpk.codr.as_deref() {
                    Some("4/5") => gw::CodeRate::Cr45,
                    Some("4/6") => gw::CodeRate::Cr46,
                    Some("4/7") => gw::CodeRate::Cr47,
                    Some("4/8") => gw::CodeRate::Cr48,
                    Some("3/8") => gw::CodeRate::Cr38,
                    Some("1/3") | Some("2/6") => gw::CodeRate::Cr26,
                    Some("1/4") => gw::CodeRate::Cr14,
                    Some("1/6") => gw::CodeRate::Cr16,
                    Some("5/6") => gw::CodeRate::Cr56,
                    Some("4/5LI") => gw::CodeRate::CrLi45,
                    Some("4/6LI") => gw::CodeRate::CrLi46,
                    Some("4/8LI") => gw::CodeRate::CrLi48,
                    _ => gw::CodeRate::Cr45,
                };
                Some(gw::Modulation {
                    parameters: Some(gw::modulation::Parameters::Lora(gw::LoraModulationInfo {
                        bandwidth: bw * 1000,
                        spreading_factor: sf,
                        code_rate: code_rate.into(),
                        polarization_inversion: txpk.ipol.unwrap_or(false),
                        ..Default::default()
                    })),
                })
            }
            Some("FSK") => {
                let datarate = match &txpk.datr {
                    Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(50000) as u32,
                    _ => 50000,
                };
                Some(gw::Modulation {
                    parameters: Some(gw::modulation::Parameters::Fsk(gw::FskModulationInfo {
                        datarate,
                        frequency_deviation: txpk.fdev.unwrap_or(25000),
                    })),
                })
            }
            _ => None,
        };

        let tx_info = gw::DownlinkTxInfo {
            frequency: (txpk.freq.unwrap_or(868.1) * 1_000_000.0) as u32,
            power: txpk.powe.unwrap_or(14),
            modulation,
            timing,
            context,
            ..Default::default()
        };

        let item = gw::DownlinkFrameItem {
            phy_payload: txpk.data.clone(),
            tx_info: Some(tx_info),
            ..Default::default()
        };

        Ok(gw::DownlinkFrame {
            gateway_id: gateway_id.to_string(),
            downlink_id: self.random_token as u32,
            items: vec![item],
            ..Default::default()
        })
    }
}

fn parse_datr_lora(datr: &Option<serde_json::Value>) -> Result<(u32, u32)> {
    match datr {
        Some(serde_json::Value::String(s)) => {
            // Parse "SF7BW125" format
            let s = s.to_uppercase();
            if let Some(sf_start) = s.find("SF") {
                if let Some(bw_start) = s.find("BW") {
                    let sf: u32 = s[sf_start + 2..bw_start].parse().unwrap_or(7);
                    let bw: u32 = s[bw_start + 2..].parse().unwrap_or(125);
                    return Ok((sf, bw));
                }
            }
            Err(anyhow!("Invalid LORA datr format: {}", s))
        }
        _ => Ok((7, 125)), // Default
    }
}

/// Parsed TX_ACK packet from gateway
pub struct TxAck {
    pub protocol_version: u8,
    pub random_token: u16,
    pub gateway_id: [u8; 8],
    pub payload: Option<TxAckPayload>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct TxAckPayload {
    pub txpk_ack: TxPkAck,
}

#[derive(Default, Serialize, Deserialize)]
pub struct TxPkAck {
    pub error: Option<String>,
}

impl TxAck {
    pub fn from_slice(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(anyhow!("TX_ACK too short"));
        }

        if data[3] != 0x05 {
            return Err(anyhow!("Not a TX_ACK packet"));
        }

        let protocol_version = data[0];
        let random_token = u16::from_le_bytes([data[1], data[2]]);

        // Gateway ID is in bytes 4-11 (if present)
        let gateway_id = if data.len() >= 12 {
            let mut gw_id: [u8; 8] = [0; 8];
            gw_id.copy_from_slice(&data[4..12]);
            gw_id
        } else {
            [0; 8]
        };

        // Parse optional JSON payload (if present)
        let payload = if data.len() > 12 {
            serde_json::from_slice(&data[12..]).ok()
        } else {
            None
        };

        Ok(TxAck {
            protocol_version,
            random_token,
            gateway_id,
            payload,
        })
    }

    /// Convert to ChirpStack DownlinkTxAck
    pub fn to_downlink_tx_ack(&self, gateway_id: &GatewayId, downlink_id: u32) -> gw::DownlinkTxAck {
        let status = match &self.payload {
            Some(p) => match p.txpk_ack.error.as_deref() {
                None | Some("") | Some("NONE") | Some("OK") => gw::TxAckStatus::Ok,
                Some("TOO_LATE") => gw::TxAckStatus::TooLate,
                Some("TOO_EARLY") => gw::TxAckStatus::TooEarly,
                Some("COLLISION_PACKET") => gw::TxAckStatus::CollisionPacket,
                Some("COLLISION_BEACON") => gw::TxAckStatus::CollisionBeacon,
                Some("TX_FREQ") => gw::TxAckStatus::TxFreq,
                Some("TX_POWER") => gw::TxAckStatus::TxPower,
                Some("GPS_UNLOCKED") => gw::TxAckStatus::GpsUnlocked,
                _ => gw::TxAckStatus::InternalError,
            },
            None => gw::TxAckStatus::Ok,
        };

        gw::DownlinkTxAck {
            gateway_id: gateway_id.to_string(),
            downlink_id,
            items: vec![gw::DownlinkTxAckItem {
                status: status.into(),
            }],
            downlink_id_legacy: Vec::new(),
            gateway_id_legacy: Vec::new(),
        }
    }

    /// Convert to UDP TX_ACK bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = vec![self.protocol_version];
        b.extend_from_slice(&self.random_token.to_le_bytes());
        b.push(0x05); // TX_ACK identifier
        b.extend_from_slice(&self.gateway_id);

        if let Some(payload) = &self.payload {
            if let Ok(json) = serde_json::to_vec(payload) {
                b.extend_from_slice(&json);
            }
        }

        b
    }
}

/// PULL_DATA packet structure
pub struct PullData {
    pub protocol_version: u8,
    pub random_token: u16,
    pub gateway_id: [u8; 8],
}

impl PullData {
    /// Create a new PULL_DATA packet
    pub fn new(gateway_id: &GatewayId) -> Self {
        PullData {
            protocol_version: 0x02,
            random_token: rand::random(),
            gateway_id: *gateway_id.as_bytes(),
        }
    }

    /// Convert to UDP PULL_DATA bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = vec![self.protocol_version];
        b.extend_from_slice(&self.random_token.to_le_bytes());
        b.push(0x02); // PULL_DATA identifier
        b.extend_from_slice(&self.gateway_id);
        b
    }
}

/// Convert a ChirpStack DownlinkTxAck to UDP TX_ACK packet
pub fn downlink_tx_ack_to_tx_ack(ack: &gw::DownlinkTxAck, gateway_id: &GatewayId, token: u16) -> TxAck {
    // Map ChirpStack status to Semtech error string
    let error = ack.items.first().map(|item| {
        match gw::TxAckStatus::try_from(item.status).unwrap_or(gw::TxAckStatus::Ignored) {
            gw::TxAckStatus::Ok => None,
            gw::TxAckStatus::TooLate => Some("TOO_LATE".to_string()),
            gw::TxAckStatus::TooEarly => Some("TOO_EARLY".to_string()),
            gw::TxAckStatus::CollisionPacket => Some("COLLISION_PACKET".to_string()),
            gw::TxAckStatus::CollisionBeacon => Some("COLLISION_BEACON".to_string()),
            gw::TxAckStatus::TxFreq => Some("TX_FREQ".to_string()),
            gw::TxAckStatus::TxPower => Some("TX_POWER".to_string()),
            gw::TxAckStatus::GpsUnlocked => Some("GPS_UNLOCKED".to_string()),
            _ => Some("INTERNAL_ERROR".to_string()),
        }
    }).flatten();

    let payload = Some(TxAckPayload {
        txpk_ack: TxPkAck { error },
    });

    TxAck {
        protocol_version: 0x02,
        random_token: token,
        gateway_id: *gateway_id.as_bytes(),
        payload,
    }
}
