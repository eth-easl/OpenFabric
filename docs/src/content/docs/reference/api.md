---
title: API Reference
description: REST and LibP2P-forwarding endpoints exposed by OCF.
---

# Base URL

- Local HTTP: `http://<host>:8092/v1`
- P2P HTTP: exposed via LibP2P; use the `p2p` and `service` endpoints below to reach peers and services.

# Health

```http
GET /v1/health
```

Returns:

```json
{ "status": "ok" }
```

# Distributed Node Table (DNT)

```http
GET /v1/dnt/table          # Full node table (CRDT view)
GET /v1/dnt/peers          # Currently connected peers
GET /v1/dnt/peers_status   # All known peers with connectedness
GET /v1/dnt/bootstraps     # Discoverable bootstrap multiaddrs
POST /v1/dnt/_node         # Update local node entry
DELETE /v1/dnt/_node       # Remove local node entry
```

Notes:
- `bootstraps` returns multiaddr strings like `/ip4/<ip>/tcp/43905/p2p/<peerId>` for reachable bootstrap nodes.

# P2P forward

Forward an HTTP request directly to a specific peer over LibP2P:

```http
GET|POST|PATCH /v1/p2p/:peerId/*path
```

Example:

```bash
curl -sS http://localhost:8092/v1/p2p/<PEER_ID>/v1/health
```

# Local service forward

Forward to a local service by name (registered on this node):

```http
GET|POST|PATCH /v1/_service/:service/*path
```

Example:

```bash
curl -sS http://localhost:8092/v1/_service/llm/v1/models
```

# Global service forward (load balanced)

Forward to a provider in the network that offers the named service. Selection uses identity groups (e.g., `model`) parsed from the JSON body.

```http
GET|POST|PATCH /v1/service/:service/*path
```

Example (OpenAI-compatible chat):

```bash
curl -sS -X POST \
  http://localhost:8092/v1/service/llm/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt2",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

Behavior:
- The dispatcher collects all providers that registered `service=llm` and whose identity groups include `model=gpt2`.
- One candidate is selected at random (internal load balancing).
- The request is forwarded over LibP2P HTTP to the chosen peer’s local service endpoint: `/v1/_service/llm/*path`.



