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

        let mut config: Configuration = toml::from_str(&content)?;
        config.normalize_filters();
        Ok(config)
    }

    /// Fold the deprecated nested `[...filters]` table into the flattened
    /// top-level filter fields. If a nested table was provided, it takes
    /// precedence (old configs kept working exactly as before).
    fn normalize_filters(&mut self) {
        for o in &mut self.gwmp.outputs {
            if let Some(nested) = o.filters_nested.take() {
                o.filters = nested;
            }
        }
        for i in &mut self.mqtt.inputs {
            if let Some(nested) = i.filters_nested.take() {
                i.filters = nested;
            }
        }
        for o in &mut self.mqtt.outputs {
            if let Some(nested) = o.filters_nested.take() {
                o.filters = nested;
            }
        }
        for i in &mut self.basics.inputs {
            if let Some(nested) = i.filters_nested.take() {
                i.filters = nested;
            }
        }
        for o in &mut self.basics.outputs {
            if let Some(nested) = o.filters_nested.take() {
                o.filters = nested;
            }
        }
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
    pub name: String,
    pub bind: String,
    pub topic_prefix: String,
}

#[derive(Default, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct GwmpOutput {
    pub name: String,
    pub server: String,
    pub uplink_only: bool,
    // Allow list (prefixes that pass the filter).
    // `gateway_id_allow` is the preferred name; `gateway_id_prefixes` is a
    // deprecated alias kept for backwards compatibility.
    #[serde(alias = "gateway_id_allow")]
    pub gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    // Deny list (prefixes that are rejected, takes precedence over allow)
    pub gateway_id_deny: Vec<lrwn_filters::EuiPrefix>,
    #[serde(flatten)]
    pub filters: Filters,
    // Deprecated nested form `[...filters]`. Merged into `filters` after parsing
    // for backwards compatibility. Do not read this directly; use `filters`.
    #[serde(default, rename = "filters")]
    pub filters_nested: Option<Filters>,
    // Mesh relay virtual gateway prefix (8 hex chars / 4 bytes).
    // When set, uplinks with relay_id in metadata get gateway_id = prefix + relay_id.
    pub relay_gateway_id_prefix: String,
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default)]
pub struct Filters {
    // Allow lists (prefixes that pass the filter).
    // `*_allow` is the preferred name; `*_prefixes` is a deprecated alias
    // kept for backwards compatibility (both deserialize to the same field).
    #[serde(alias = "dev_addr_allow")]
    pub dev_addr_prefixes: Vec<lrwn_filters::DevAddrPrefix>,
    #[serde(alias = "join_eui_allow")]
    pub join_eui_prefixes: Vec<lrwn_filters::EuiPrefix>,
    // Deny lists (prefixes that are rejected, takes precedence over allow)
    pub dev_addr_deny: Vec<lrwn_filters::DevAddrPrefix>,
    pub join_eui_deny: Vec<lrwn_filters::EuiPrefix>,
    // Allow only uplinks originating from inputs with one of these names.
    pub input_name_allow: Vec<String>,
    // Deny uplinks originating from inputs with any of these names.
    pub input_name_deny: Vec<String>,
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
    pub name: String,
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
    // Allow list (prefixes that pass the filter).
    // `gateway_id_allow` is the preferred name; `gateway_id_prefixes` is a
    // deprecated alias kept for backwards compatibility.
    #[serde(alias = "gateway_id_allow")]
    pub gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    // Deny list (prefixes that are rejected, takes precedence over allow)
    pub gateway_id_deny: Vec<lrwn_filters::EuiPrefix>,
    #[serde(flatten)]
    pub filters: Filters,
    // Deprecated nested form `[...filters]`. Merged into `filters` after parsing
    // for backwards compatibility. Do not read this directly; use `filters`.
    #[serde(default, rename = "filters")]
    pub filters_nested: Option<Filters>,
}

impl Default for MqttInput {
    fn default() -> Self {
        MqttInput {
            name: String::new(),
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
            filters_nested: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct MqttOutput {
    pub name: String,
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
    // Allow list (prefixes that pass the filter).
    // `gateway_id_allow` is the preferred name; `gateway_id_prefixes` is a
    // deprecated alias kept for backwards compatibility.
    #[serde(alias = "gateway_id_allow")]
    pub gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    // Deny list (prefixes that are rejected, takes precedence over allow)
    pub gateway_id_deny: Vec<lrwn_filters::EuiPrefix>,
    #[serde(flatten)]
    pub filters: Filters,
    // Deprecated nested form `[...filters]`. Merged into `filters` after parsing
    // for backwards compatibility. Do not read this directly; use `filters`.
    #[serde(default, rename = "filters")]
    pub filters_nested: Option<Filters>,
    // Mesh relay virtual gateway prefix (8 hex chars / 4 bytes).
    // When set, uplinks with relay_id in metadata get gateway_id = prefix + relay_id.
    pub relay_gateway_id_prefix: String,
}

impl Default for MqttOutput {
    fn default() -> Self {
        MqttOutput {
            name: String::new(),
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
            filters_nested: None,
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
    pub name: String,
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
    #[serde(alias = "gateway_id_allow")]
    pub gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    pub gateway_id_deny: Vec<lrwn_filters::EuiPrefix>,
    #[serde(flatten)]
    pub filters: Filters,
    // Deprecated nested form `[...filters]`. Merged into `filters` after parsing
    // for backwards compatibility. Do not read this directly; use `filters`.
    #[serde(default, rename = "filters")]
    pub filters_nested: Option<Filters>,
    pub router_config: RouterConfig,
}

impl Default for BasicsInput {
    fn default() -> Self {
        BasicsInput {
            name: String::new(),
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
            filters_nested: None,
            router_config: RouterConfig::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct BasicsOutput {
    pub name: String,
    pub server: String,
    pub tls_cert: String,
    pub tls_key: String,
    pub ca_cert: String,
    pub uplink_only: bool,
    #[serde(with = "humantime_serde")]
    pub ping_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub reconnect_interval: Duration,
    #[serde(alias = "gateway_id_allow")]
    pub gateway_id_prefixes: Vec<lrwn_filters::EuiPrefix>,
    pub gateway_id_deny: Vec<lrwn_filters::EuiPrefix>,
    #[serde(flatten)]
    pub filters: Filters,
    // Deprecated nested form `[...filters]`. Merged into `filters` after parsing
    // for backwards compatibility. Do not read this directly; use `filters`.
    #[serde(default, rename = "filters")]
    pub filters_nested: Option<Filters>,
    pub gateway_tokens: HashMap<String, String>,
}

impl Default for BasicsOutput {
    fn default() -> Self {
        BasicsOutput {
            name: String::new(),
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
            filters_nested: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Both the new flat form and the deprecated nested `[...filters]` table
    /// must parse, and `input_name_deny` must be readable top-level.
    #[test]
    fn test_filters_flat_and_nested() {
        let toml = r#"
[gwmp]
[[gwmp.input]]
  bind = "0.0.0.0:1699"
  name = "openlns"

# New flat form
[[gwmp.output]]
  server = "flat:1700"
  input_name_deny = ["openlns", "zzzztest"]
  dev_addr_deny = ["01337000/20"]

# Deprecated nested form
[[gwmp.output]]
  server = "nested:1700"
  [gwmp.output.filters]
    dev_addr_deny = ["780001B8/29"]
    input_name_deny = ["oldstyle"]
"#;
        let mut config: Configuration = toml::from_str(toml).unwrap();
        config.normalize_filters();

        let flat = &config.gwmp.outputs[0];
        assert_eq!(flat.filters.input_name_deny, vec!["openlns", "zzzztest"]);
        assert_eq!(flat.filters.dev_addr_deny.len(), 1);
        assert!(flat.filters_nested.is_none());

        let nested = &config.gwmp.outputs[1];
        assert_eq!(nested.filters.input_name_deny, vec!["oldstyle"]);
        assert_eq!(nested.filters.dev_addr_deny.len(), 1);
        assert!(nested.filters_nested.is_none());
    }

    /// The new `*_allow` names must be accepted as aliases of the legacy
    /// `*_prefixes` names, for both the flattened filter fields and the
    /// top-level gateway_id field.
    #[test]
    fn test_allow_aliases() {
        let toml = r#"
[gwmp]
[[gwmp.output]]
  server = "new:1700"
  gateway_id_allow = ["0102030400000000/32"]
  dev_addr_allow = ["0000ff00/24"]
  join_eui_allow = ["0000ff0000000000/24"]

[[gwmp.output]]
  server = "old:1700"
  gateway_id_prefixes = ["0102030400000000/32"]
  dev_addr_prefixes = ["0000ff00/24"]
"#;
        let mut config: Configuration = toml::from_str(toml).unwrap();
        config.normalize_filters();

        // New `*_allow` spellings land in the same fields the code reads.
        let new = &config.gwmp.outputs[0];
        assert_eq!(new.gateway_id_prefixes.len(), 1);
        assert_eq!(new.filters.dev_addr_prefixes.len(), 1);
        assert_eq!(new.filters.join_eui_prefixes.len(), 1);

        // Legacy `*_prefixes` still works unchanged.
        let old = &config.gwmp.outputs[1];
        assert_eq!(old.gateway_id_prefixes.len(), 1);
        assert_eq!(old.filters.dev_addr_prefixes.len(), 1);
    }
}