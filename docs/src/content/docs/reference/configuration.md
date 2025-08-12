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
  wallet: ""
seed: "0"          # 0 = persisted/random key
tcp_port: "43905"  # LibP2P TCP/WS
udp_port: "59820"  # LibP2P QUIC
```

# Common options

- `bootstrap.addr`: Either an HTTP URL returning `{ bootstraps: ["<multiaddr>"] }` or a single multiaddr like `/ip4/198.51.100.10/tcp/43905/p2p/<ID>`.
- `public-addr`: Public IPv4 address to advertise (enables bootstrap).
- `mode`: `standalone`, `local`, or the default networked mode (set `node` or `full`).

# Example configs

Minimal dispatcher:

```yaml
port: "8092"
mode: node
bootstrap:
  addr: "http://152.67.71.5:8092/v1/dnt/bootstraps"
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



