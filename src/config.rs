use std::collections::HashMap;
use std::time::Duration;
use std::{env, fs};

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Configuration {
    pub logging: Logging,
    pub gwmp: Gwmp,
    pub monitoring: Monitoring,
    pub mqtt: MqttConfig,
    pub basics: BasicsConfig,
}

impl Configuration {
    pub fn get(filenames: &[String]) -> Result<Configuration> {
        let mut content = String::new();

        for file_name in filenames {
            content.push_str(&fs::read_to_string(file_name)?);
        }

        // Replace environment variables in config.
        for (k, v) in env::vars() {
            content = content.replace(&format!("${}", k), &v);
        }

        let config: Configuration = toml::from_str(&content)?;
        Ok(config)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Logging {
    pub level: String,
}

impl Default for Logging {
    fn default() -> Self {
        Logging {
            level: "info".into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Gwmp {
    #[serde(rename = "input")]
    pub inputs: Vec<GwmpInput>,
    #[serde(rename = "output")]
    pub outputs: Vec<GwmpOutput>,
}

impl Default for Gwmp {
    fn default() -> Self {
        Gwmp {
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct GwmpInput {
    pub bind: String,
    pub topic_prefix: String,
}

#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct GwmpOutput {
    pub server: String,
    pub uplink_only: bool,
    // Allow list (prefixes that pass the filter)
    pub gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    // Deny list (prefixes that are rejected, takes precedence over allow)
    pub gateway_id_deny: Vec<lrwn_filters::EuiPrefix>,
    pub filters: Filters,
    // Mesh relay virtual gateway prefix (8 hex chars / 4 bytes).
    // When set, uplinks with relay_id in metadata get gateway_id = prefix + relay_id.
    pub relay_gateway_id_prefix: String,
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default)]
pub struct Filters {
    // Allow lists (prefixes that pass the filter)
    pub dev_addr_prefixes: Vec<lrwn_filters::DevAddrPrefix>,
    pub join_eui_prefixes: Vec<lrwn_filters::EuiPrefix>,
    // Deny lists (prefixes that are rejected, takes precedence over allow)
    pub dev_addr_deny: Vec<lrwn_filters::DevAddrPrefix>,
    pub join_eui_deny: Vec<lrwn_filters::EuiPrefix>,
}

#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct Monitoring {
    pub bind: String,
}

#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct MqttConfig {
    #[serde(rename = "input")]
    pub inputs: Vec<MqttInput>,
    #[serde(rename = "output")]
    pub outputs: Vec<MqttOutput>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct MqttInput {
    pub json: bool,
    pub server: String,
    pub username: String,
    pub password: String,
    pub qos: u8,
    pub clean_session: bool,
    pub client_id: String,
    #[serde(with = "humantime_serde")]
    pub keep_alive_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub reconnect_interval: Duration,
    pub ca_cert: String,
    pub tls_cert: String,
    pub tls_key: String,
    // Allow list (prefixes that pass the filter)
    pub gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    // Deny list (prefixes that are rejected, takes precedence over allow)
    pub gateway_id_deny: Vec<lrwn_filters::EuiPrefix>,
    pub filters: Filters,
}

impl Default for MqttInput {
    fn default() -> Self {
        MqttInput {
            json: false,
            server: "tcp://localhost:1883".into(),
            username: String::new(),
            password: String::new(),
            qos: 0,
            clean_session: true,
            client_id: String::new(),
            keep_alive_interval: Duration::from_secs(30),
            reconnect_interval: Duration::from_secs(1),
            ca_cert: String::new(),
            tls_cert: String::new(),
            tls_key: String::new(),
            gateway_id_prefixes: Vec::new(),
            gateway_id_deny: Vec::new(),
            filters: Filters::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct MqttOutput {
    pub json: bool,
    pub server: String,
    pub username: String,
    pub password: String,
    pub qos: u8,
    pub clean_session: bool,
    pub client_id: String,
    #[serde(with = "humantime_serde")]
    pub keep_alive_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub reconnect_interval: Duration,
    pub ca_cert: String,
    pub tls_cert: String,
    pub tls_key: String,
    pub uplink_only: bool,
    pub analyzer: bool,
    pub subscribe_application: bool,
    pub forward_application: bool,
    // Allow list (prefixes that pass the filter)
    pub gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    // Deny list (prefixes that are rejected, takes precedence over allow)
    pub gateway_id_deny: Vec<lrwn_filters::EuiPrefix>,
    pub filters: Filters,
    // Mesh relay virtual gateway prefix (8 hex chars / 4 bytes).
    // When set, uplinks with relay_id in metadata get gateway_id = prefix + relay_id.
    pub relay_gateway_id_prefix: String,
}

impl Default for MqttOutput {
    fn default() -> Self {
        MqttOutput {
            json: false,
            server: "tcp://localhost:1883".into(),
            username: String::new(),
            password: String::new(),
            qos: 0,
            clean_session: true,
            client_id: String::new(),
            keep_alive_interval: Duration::from_secs(30),
            reconnect_interval: Duration::from_secs(1),
            ca_cert: String::new(),
            tls_cert: String::new(),
            tls_key: String::new(),
            uplink_only: false,
            analyzer: false,
            subscribe_application: false,
            forward_application: false,
            gateway_id_prefixes: Vec::new(),
            gateway_id_deny: Vec::new(),
            filters: Filters::default(),
            relay_gateway_id_prefix: String::new(),
        }
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct BasicsConfig {
    #[serde(rename = "input")]
    pub inputs: Vec<BasicsInput>,
    #[serde(rename = "output")]
    pub outputs: Vec<BasicsOutput>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct BasicsInput {
    pub bind: String,
    pub topic_prefix: String,
    pub tls_cert: String,
    pub tls_key: String,
    pub ca_cert: String,
    #[serde(with = "humantime_serde")]
    pub ping_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub read_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub write_timeout: Duration,
    pub gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    pub gateway_id_deny: Vec<lrwn_filters::EuiPrefix>,
    pub filters: Filters,
    pub router_config: RouterConfig,
}

impl Default for BasicsInput {
    fn default() -> Self {
        BasicsInput {
            bind: "0.0.0.0:3001".into(),
            topic_prefix: String::new(),
            tls_cert: String::new(),
            tls_key: String::new(),
            ca_cert: String::new(),
            ping_interval: Duration::from_secs(30),
            read_timeout: Duration::from_secs(90),
            write_timeout: Duration::from_secs(10),
            gateway_id_prefixes: Vec::new(),
            gateway_id_deny: Vec::new(),
            filters: Filters::default(),
            router_config: RouterConfig::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct BasicsOutput {
    pub server: String,
    pub tls_cert: String,
    pub tls_key: String,
    pub ca_cert: String,
    pub uplink_only: bool,
    #[serde(with = "humantime_serde")]
    pub ping_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub reconnect_interval: Duration,
    pub gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    pub gateway_id_deny: Vec<lrwn_filters::EuiPrefix>,
    pub filters: Filters,
    pub gateway_tokens: HashMap<String, String>,
}

impl Default for BasicsOutput {
    fn default() -> Self {
        BasicsOutput {
            server: String::new(),
            tls_cert: String::new(),
            tls_key: String::new(),
            ca_cert: String::new(),
            uplink_only: false,
            ping_interval: Duration::from_secs(30),
            reconnect_interval: Duration::from_secs(5),
            gateway_id_prefixes: Vec::new(),
            gateway_id_deny: Vec::new(),
            filters: Filters::default(),
            gateway_tokens: HashMap::new(),
        }
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RouterConfig {
    pub net_ids: Vec<u32>,
    pub join_euis: Vec<[u64; 2]>,
    pub freq_range: [u32; 2],
    pub drs: Vec<[i32; 3]>,
}
