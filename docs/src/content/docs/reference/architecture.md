---
title: Architecture
description: How the Open Compute Framework works under the hood.
---

# Overview

Open Compute Framework (OCF) forms a decentralized compute fabric using LibP2P for peer connectivity and a CRDT-backed registry to discover services and route requests.

## Key components

- LibP2P Host: Each node runs a LibP2P host with TCP, WebSocket, and QUIC transports, NAT traversal, and a dual DHT for peer routing.
- CRDT Node Table: A distributed datastore (Badger + go-ds-crdt) maintaining a map of peers and their advertised services. Updates propagate via pubsub.
- HTTP Gateway: Each node exposes an HTTP API on port 8092 and a P2P HTTP listener over LibP2P.
- Service Registry: Workers can register services (e.g., `llm`) and identity groups (e.g., `model=gpt2`). Global routing uses these identities.

## Bootstrapping and modes

- standalone: No bootstrap peers; local-only network.
- local: Bootstrap to `127.0.0.1`.
- node (default): Fetch bootstrap addresses from `bootstrap.addr` (HTTP endpoint or multiaddr).

If `--public-addr` is set, the node records its public address and can act as a bootstrap.

## Routing model

- Direct service: `GET/POST /v1/_service/:service/*path` forwards to the locally registered service at `host:port`.
- Global service: `GET/POST /v1/service/:service/*path` selects a provider from the node table that matches the request’s identity group (e.g., `model` in the JSON body) and forwards over LibP2P HTTP.
- P2P forwarding: `GET/PATCH/POST /v1/p2p/:peerId/*path` forwards directly to another peer via LibP2P.

## LLM service registration

Start a worker with flags `--service.name=llm --service.port=<PORT>`. OCF will:

1. Health check `http://localhost:<PORT>/health` (retrying up to 6000 times).
2. Fetch `http://localhost:<PORT>/v1/models` and extract model IDs.
3. Register the `llm` service with identity groups like `model=<id>` so global requests can be routed by model.

## Observability

OCF emits structured events (e.g., service forwards, P2P forwards) to Axiom if configured. CORS is permissive by default for API consumption.



