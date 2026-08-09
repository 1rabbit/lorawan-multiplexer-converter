use handlebars::Handlebars;

use crate::config::Configuration;

pub fn run(config: &Configuration) {
    let template = r#"
# Logging settings.
[logging]

  # Log level.
  #
  # Valid options are:
  #   * TRACE
  #   * DEBUG
  #   * INFO
  #   * WARN
  #   * ERROR
  level="{{ logging.level }}"


# GWMP configuration.
[gwmp]

  # UDP inputs.
  #
  # Each input binds to an interface:port and tags incoming packets with
  # a topic_prefix (region). Multiple inputs can be configured for
  # different regions.
  #
  # An optional "name" can be set to identify the input. This name is used in
  # logs and can be referenced by an output's "input_name_deny" filter to drop
  # uplinks originating from this input.
  {{#each gwmp.input}}
  [[gwmp.input]]
    name="{{this.name}}"
    bind="{{this.bind}}"
    topic_prefix="{{this.topic_prefix}}"
  {{/each}}

  # Outputs to forward gateway data to.
  #
  # Example configuration:
  # [[gwmp.output]]

  #   # Hostname:port of the server.
  #   server="example.com:1700"

  #   # Only allow uplink.
  #   #
  #   # If set to true, any downlink will be discarded.
  #   uplink_only=false

  #   # Gateway ID prefix filters (allow list).
  #   #
  #   # If not set, data of all gateways will be forwarded. If set, only data
  #   # from gateways with a matching Gateway ID will be forwarded.
  #   #
  #   # `gateway_id_allow` is the preferred name; `gateway_id_prefixes` is a
  #   # deprecated alias. Don't set both.
  #   #
  #   # Example:
  #   # * "0102030405060708/64": Exact match (all 64 bits of the filter must match)
  #   # * "0102030400000000/32": All gateway IDs starting with "01020304" (filter on 32 most significant bits)
  #   gateway_id_allow=[]

  #   # Gateway ID deny list.
  #   #
  #   # If set, gateways matching any prefix will be rejected.
  #   # Deny takes precedence over allow.
  #   gateway_id_deny=[]
  #
  #   # DevAddr / JoinEUI / input-name filters.
  #   #
  #   # These are configured directly on the output. (A legacy nested
  #   # `[gwmp.output.filters]` table is still accepted for backwards
  #   # compatibility and takes precedence when present.)
  #   #
  #   # Allow lists use `*_allow` (preferred); `*_prefixes` are deprecated
  #   # aliases kept for backwards compatibility. Don't set both.

  #   # DevAddr allow list.
  #   #
  #   # Example configuration:
  #   # dev_addr_allow=["0000ff00/24"]
  #   #
  #   # The above filter means that the 24MSB of 0000ff00 will be used to
  #   # filter DevAddrs. Uplinks with DevAddrs that do not match any of the
  #   # configured filters will not be forwarded. Leaving this option empty
  #   # disables filtering on DevAddr.
  #   dev_addr_allow=[]

  #   # DevAddr deny list.
  #   #
  #   # If set, uplinks with DevAddrs matching any prefix will be rejected.
  #   # Deny takes precedence over allow.
  #   dev_addr_deny=[]

  #   # JoinEUI allow list.
  #   #
  #   # Example configuration:
  #   # join_eui_allow=["0000ff0000000000/24"]
  #   #
  #   # The above filter means that the 24MSB of 0000ff0000000000 will be used
  #   # to filter JoinEUIs. Uplinks with JoinEUIs that do not match any of the
  #   # configured filters will not be forwarded. Leaving this option empty
  #   # disables filtering on JoinEUI.
  #   join_eui_allow=[]

  #   # JoinEUI deny list.
  #   #
  #   # If set, join requests with JoinEUIs matching any prefix will be rejected.
  #   # Deny takes precedence over allow.
  #   join_eui_deny=[]

  #   # Input name allow / deny lists.
  #   #
  #   # Filter uplinks by the "name" of the input they arrived on. Same
  #   # allow/deny logic as the other filters: deny takes precedence, and a
  #   # non-empty allow list restricts forwarding to matching input names.
  #   # Unnamed inputs pass a deny-only config but are rejected when an
  #   # allow list is set.
  #   #
  #   # Example:
  #   # input_name_allow=["helium"]
  #   # input_name_deny=["openlns", "zzzztest"]
  #   input_name_allow=[]
  #   input_name_deny=[]
  {{#each gwmp.output}}
  [[gwmp.output]]
    name="{{this.name}}"
    server="{{this.server}}"
    uplink_only={{this.uplink_only}}
    relay_gateway_id_prefix="{{this.relay_gateway_id_prefix}}"
    gateway_id_allow=[
      {{#each this.gateway_id_prefixes}}
      "{{this}}",
      {{/each}}
    ]
    gateway_id_deny=[
      {{#each this.gateway_id_deny}}
      "{{this}}",
      {{/each}}
    ]
    dev_addr_allow=[
      {{#each this.dev_addr_prefixes}}
      "{{this}}",
      {{/each}}
    ]
    dev_addr_deny=[
      {{#each this.dev_addr_deny}}
      "{{this}}",
      {{/each}}
    ]
    join_eui_allow=[
      {{#each this.join_eui_prefixes}}
      "{{this}}",
      {{/each}}
    ]
    join_eui_deny=[
      {{#each this.join_eui_deny}}
      "{{this}}",
      {{/each}}
    ]
    input_name_allow=[
      {{#each this.input_name_allow}}
      "{{this}}",
      {{/each}}
    ]
    input_name_deny=[
      {{#each this.input_name_deny}}
      "{{this}}",
      {{/each}}
    ]
  {{/each}}


# Monitoring configuration.
[monitoring]

  # Interface:port.
  #
  # If set, this will enable the monitoring endpoints. If not set, the endpoint
  # will be disabled. Endpoints:
  #
  # * /metrics: Exposes Prometheus metrics.
  bind="{{ monitoring.bind }}"


# MQTT backend configuration.
[mqtt]

  # MQTT inputs (subscribe to uplinks from an MQTT broker).
  {{#each mqtt.input}}
  [[mqtt.input]]
    server="{{this.server}}"
    json={{this.json}}
    username="{{this.username}}"
    password="{{this.password}}"
    qos={{this.qos}}
    clean_session={{this.clean_session}}
    client_id="{{this.client_id}}"
    keep_alive_interval="{{this.keep_alive_interval}}"
    reconnect_interval="{{this.reconnect_interval}}"
    ca_cert="{{this.ca_cert}}"
    tls_cert="{{this.tls_cert}}"
    tls_key="{{this.tls_key}}"
    gateway_id_allow=[
      {{#each this.gateway_id_prefixes}}
      "{{this}}",
      {{/each}}
    ]
    gateway_id_deny=[
      {{#each this.gateway_id_deny}}
      "{{this}}",
      {{/each}}
    ]
  {{/each}}

  # MQTT outputs (publish uplinks to an MQTT broker, subscribe to downlinks).
  {{#each mqtt.output}}
  [[mqtt.output]]
    name="{{this.name}}"
    server="{{this.server}}"
    json={{this.json}}
    username="{{this.username}}"
    password="{{this.password}}"
    qos={{this.qos}}
    clean_session={{this.clean_session}}
    client_id="{{this.client_id}}"
    keep_alive_interval="{{this.keep_alive_interval}}"
    reconnect_interval="{{this.reconnect_interval}}"
    ca_cert="{{this.ca_cert}}"
    tls_cert="{{this.tls_cert}}"
    tls_key="{{this.tls_key}}"
    uplink_only={{this.uplink_only}}
    analyzer={{this.analyzer}}
    forward_application={{this.forward_application}}
    relay_gateway_id_prefix="{{this.relay_gateway_id_prefix}}"
    gateway_id_allow=[
      {{#each this.gateway_id_prefixes}}
      "{{this}}",
      {{/each}}
    ]
    gateway_id_deny=[
      {{#each this.gateway_id_deny}}
      "{{this}}",
      {{/each}}
    ]

    dev_addr_allow=[
      {{#each this.dev_addr_prefixes}}
      "{{this}}",
      {{/each}}
    ]
    dev_addr_deny=[
      {{#each this.dev_addr_deny}}
      "{{this}}",
      {{/each}}
    ]
    join_eui_allow=[
      {{#each this.join_eui_prefixes}}
      "{{this}}",
      {{/each}}
    ]
    join_eui_deny=[
      {{#each this.join_eui_deny}}
      "{{this}}",
      {{/each}}
    ]
    input_name_allow=[
      {{#each this.input_name_allow}}
      "{{this}}",
      {{/each}}
    ]
    input_name_deny=[
      {{#each this.input_name_deny}}
      "{{this}}",
      {{/each}}
    ]
  {{/each}}
"#;

    let reg = Handlebars::new();
    println!(
        "{}",
        reg.render_template(template, config)
            .expect("Render configfile error")
    );
}
