# LoRaWAN Multiplexer Converter

A LoRaWAN packet multiplexer and protocol converter written in Rust. Connects gateways
and network servers across Semtech UDP (GWMP), Basic Station (LNS WebSocket), and ChirpStack
MQTT — simultaneously, in any combination.

Based on [chirpstack-packet-multiplexer](https://github.com/chirpstack/chirpstack-packet-multiplexer)
but substantially rewritten and extended.

## Features

- **Semtech UDP (GWMP)**: Accept UDP packet-forwarder connections from gateways, forward to UDP servers
- **MQTT backend**: Publish uplinks to MQTT brokers, subscribe to downlinks (ChirpStack Gateway Bridge topic convention). Protobuf (default) or JSON encoding, TLS, multiple brokers simultaneously
- **MQTT input**: Subscribe to uplinks from an MQTT broker and forward them onward (MQTT→UDP, MQTT→MQTT)
- **Basic Station (LNS)**: Accept WebSocket connections from Basic Station gateways; connect to Basic Station network servers as a client
- **Protocol conversion**: UDP↔MQTT↔Basic Station in any direction simultaneously
- **Mesh relay virtualization**: Make ChirpStack mesh relays appear as independent gateways to any LNS. Configurable per output with a gateway ID prefix
- **Allow/deny filtering**: Gateway ID, DevAddr, and JoinEUI prefix filters with explicit deny lists on every output. Deny takes precedence
- **Analyzer mode**: Passive MQTT output that receives all traffic (including `application/#`) without affecting routing
- **Environment variable substitution**: `$ENV_VAR` in config values
- **Prometheus metrics**: `/metrics` endpoint for all backends

## Forwarding paths

| Source | Destination |
|--------|-------------|
| UDP gateway | UDP server |
| UDP gateway | MQTT broker |
| UDP gateway | Basic Station server |
| MQTT broker | UDP server |
| MQTT broker | MQTT broker |
| Basic Station gateway | Basic Station server |
| Basic Station gateway | UDP server |
| Basic Station gateway | MQTT broker |

Any combination of the above can run simultaneously.

## Running

```bash
cp config.toml.example config.toml
$EDITOR config.toml
docker compose build && docker compose up -d
```

The image builds from source inside Docker — no local Rust toolchain needed.

Ports exposed by default:
- `1700/udp` — Semtech UDP (GWMP)
- `3001/tcp` — Basic Station LNS (WebSocket)

```bash
docker compose logs -f                              # logs
docker compose down && docker compose build && docker compose up -d   # rebuild after code change
docker compose restart                              # after config-only change
```

## Configuration

See [`config.toml.example`](config.toml.example) for the full annotated reference.

### GWMP (Semtech UDP)

```toml
[gwmp]

  [[gwmp.input]]
    bind = "0.0.0.0:1700"
    topic_prefix = "eu868"   # used as MQTT topic prefix

  [[gwmp.output]]
    server = "example.com:1700"
    uplink_only = false
    gateway_id_prefixes = []  # allow list (empty = all)
    gateway_id_deny = []      # deny list (takes precedence)
    [gwmp.output.filters]
      dev_addr_prefixes = []
      dev_addr_deny = []
      join_eui_prefixes = []
      join_eui_deny = []
```

### MQTT

```toml
[mqtt]

  # Subscribe to uplinks from a broker (MQTT input).
  [[mqtt.input]]
    server = "tcp://localhost:1883"
    username = ""
    password = ""

  # Publish uplinks, subscribe to downlinks (MQTT output).
  [[mqtt.output]]
    server = "tcp://localhost:1883"
    uplink_only = false

    # Optional: subscribe to application/# on this broker and republish
    # those messages to all outputs that have forward_application = true.
    subscribe_application = false

    # Optional: receive application/# messages from any subscribe_application
    # broker and publish them here. Combine with analyzer = true for a mirror.
    forward_application = false

    # analyzer: output-only mode. Does NOT subscribe to any topics.
    # Receives everything the multiplexer sees:
    #   - all uplinks (gateway events)
    #   - all downlink commands (command/down)
    #   - all application messages (if forward_application = true)
    # Use this for a passive traffic analyzer / logger that never
    # injects anything back into the network.
    analyzer = false

    [mqtt.output.filters]
      dev_addr_prefixes = []
      dev_addr_deny = []
      join_eui_prefixes = []
      join_eui_deny = []
```

**Typical analyzer setup:**

Pairs well with [lorawan-analyzer](https://github.com/1rabbit/lorawan-analyzer) — captures
uplinks, downlinks, join requests, and TX acks via MQTT, stores them in Postgres/TimescaleDB,
and serves a real-time web dashboard. It runs its own MQTT broker; point it as an analyzer output
to passively mirror all traffic (including application messages) without affecting your network:

```toml
# Primary ChirpStack broker — mirror application messages from here
[[mqtt.output]]
  server = "tcp://chirpstack:1883"
  subscribe_application = true

# lorawan-analyzer — receives all gateway + application traffic, publishes nothing back
[[mqtt.output]]
  server = "tcp://lorawan-analyzer:1883"
  analyzer = true
  forward_application = true
```

For running multiple independent MQTT brokers (one per ChirpStack instance, one for the analyzer, etc.),
[multi-mqtt-docker-compose](https://github.com/1rabbit/multi-mqtt-docker-compose) provides a ready-made
Docker Compose setup for spinning up multiple Mosquitto brokers with isolated credentials and ports.

### Basic Station

```toml
[basics]

  # Accept connections from Basic Station gateways.
  [[basics.input]]
    bind = "0.0.0.0:3001"
    topic_prefix = "eu868"
    [basics.input.router_config]
      net_ids = []
      join_euis = []
      freq_range = [0, 0]
      drs = []

  # Connect to a Basic Station LNS as a client.
  [[basics.output]]
    server = "wss://lns.example.com:3001"
    # gateway_tokens = { "0016c001f184aa22" = "Authorization: Bearer ..." }
    [basics.output.filters]
      dev_addr_prefixes = []
      dev_addr_deny = []
```

### Mesh relay virtualization

ChirpStack's [mesh relay](https://www.chirpstack.io/docs/chirpstack-mesh/) feature lets lightweight
relay devices forward end-device packets through a border gateway. The relay's identity (`relay_id`)
is carried in the protobuf `rx_info.metadata["relay_id"]` field of each uplink.

ChirpStack natively understands this metadata, but other LNS platforms only see the border gateway.
With `relay_gateway_id_prefix`, each mesh relay appears as its own gateway:

- **Uplink**: The multiplexer detects `relay_id` in MQTT-in uplinks and replaces the gateway ID with
  `prefix + relay_id` on outputs that have the prefix configured
- **Downlink**: Downlinks targeting a virtual gateway are routed back through the original border gateway

The prefix is configured **per output**, so you can choose which outputs see virtual gateways.
Outputs without a prefix pass traffic through unchanged with the border gateway ID.

```toml
[[mqtt.input]]
  server = "tcp://chirpstack:1883"    # border gateways report here

[[gwmp.output]]
  server = "other-lns.example.com:1700"
  relay_gateway_id_prefix = "aabb0000"  # 8 hex chars (4 bytes)
  # Relay with relay_id "11223344" → virtual gateway "aabb000011223344"

[[mqtt.output]]
  server = "tcp://other-lns:1883"
  relay_gateway_id_prefix = "aabb0000"  # same or different prefix per output

[[mqtt.output]]
  server = "tcp://chirpstack-mirror:1883"
  # No prefix → border gateway ID preserved
```

### Allow/Deny filter logic

| `*_prefixes` (allow) | `*_deny` | Result |
|---|---|---|
| `[]` | `[]` | Pass everything |
| `["01000000/8"]` | `[]` | Only matching prefix |
| `[]` | `["01020304/32"]` | Everything except denied |
| `["01000000/8"]` | `["01020304/32"]` | Prefix, minus denied |

### Monitoring

```toml
[monitoring]
  bind = "0.0.0.0:8080"   # exposes /metrics (Prometheus)
```

## License

MIT. See [LICENSE](https://github.com/1rabbit/lorawan-multiplexer-converter/blob/master/LICENSE).
