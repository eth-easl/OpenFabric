---
title: CLI
description: Command line options for the OCF node.
---

# Usage

The released binaries are named by architecture (e.g., `ocf-amd64`). The CLI root command is `ocfcore` in source. Examples below use the binary:

```bash
./ocf-amd64 --help | cat
```

# Commands

- start: Start the node and HTTP API.
- version: Print version information.
- init: Initialize config (placeholder).

# Flags (start)

- `--wallet.account` string: Wallet account.
- `--bootstrap.addr` string: Bootstrap source (HTTP URL returning `{"bootstraps": ["/ip4/x/tcp/43905/p2p/<ID>"]}` or a single multiaddr). Default: `http://152.67.71.5:8092/v1/dnt/bootstraps`.
- `--seed` string: Seed for deterministic peer key (use `0` to persist/load key).
- `--mode` string: `standalone`, `local`, or `node`/`full` (default `node`).
- `--tcpport` string: LibP2P TCP port. Default `43905`.
- `--udpport` string: LibP2P QUIC UDP port. Default `59820`.
- `--subprocess` string: Start a critical subprocess (kept alive by OCF).
- `--public-addr` string: Public IP address to advertise (enables bootstrap role).
- `--service.name` string: Local service name to register (e.g., `llm`).
- `--service.port` string: Local service port to register (e.g., `8080`).
- `--cleanslate` bool: Remove local CRDT db on start. Default `true`.

# Examples

Start a standalone dispatcher:

```bash
./ocf-amd64 start --mode standalone
```

Start a node using a known bootstrap multiaddr:

```bash
./ocf-amd64 start --bootstrap.addr=/ip4/1.2.3.4/tcp/43905/p2p/<BOOTSTRAP_PEER_ID>
```

Advertise as a public bootstrap:

```bash
./ocf-amd64 start --public-addr=203.0.113.10
```

Register a local LLM service (worker):

```bash
./ocf-amd64 start \
  --service.name=llm \
  --service.port=8080
```



