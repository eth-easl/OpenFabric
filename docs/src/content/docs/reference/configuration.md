---
title: Configuration
description: Configure ports, bootstrap, and runtime settings.
---

# Location

OCF reads a YAML config by default from:

```
$HOME/.config/ocf/cfg.yaml
```

You can override with `--config <path>`. CLI flags override config values.

# Defaults

```yaml
path: ""
port: "8092"       # HTTP API
name: "relay"
p2p:
  port: "8093"      # reserved; not required for LibP2P
vacuum:
  interval: 10
queue:
  port: "8094"
account:
  wallet: ""       # Wallet account address for node identification
seed: "0"          # 0 = persisted/random key
tcp_port: "43905"  # LibP2P TCP/WS
udp_port: "59820"  # LibP2P QUIC
```

# Common options

- `bootstrap.addr`: Legacy single-source value. Accepts an HTTP URL or one multiaddr.
- `bootstrap.sources`: Ordered list of sources (HTTP URLs, `dnsaddr://host`, and multiaddrs). The resolver walks the list until it collects addresses.
- `bootstrap.static`: Convenience list of multiaddrs that are appended after dynamic sources.
- `public-addr`: Public IPv4 address to advertise (enables bootstrap).
- `mode`: `standalone`, `local`, or the default networked mode (set `node` or `full`).

# Example configs

Minimal dispatcher:

```yaml
port: "8092"
mode: node
bootstrap:
  sources:
    - "http://152.67.71.5:8092/v1/dnt/bootstraps"
    - "dnsaddr://bootstrap.ocf.example.com"
```

Public bootstrap:

```yaml
public-addr: "203.0.113.10"
tcp_port: "43905"
udp_port: "59820"
```

Worker registering an LLM service:

```yaml
service:
  name: llm
  port: "8080"
```

Node with wallet identification:

```yaml
account:
  wallet: "your_wallet_address_here"
```



