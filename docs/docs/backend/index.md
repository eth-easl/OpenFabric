# Backend Overview

The Open Compute Framework (OCF) backend is a robust, decentralized computing node implemented in Go. It leverages the LibP2P networking stack to facilitate peer-to-peer communication, service discovery, and distributed resource management.

## Key Features

- **Decentralized Networking**: Built on `libp2p`, supporting TCP, WebSocket, and QUIC transports.
- **Service Discovery**: Uses DHT and PubSub for dynamic peer and service discovery.
- **Distributed Storage**: Integrates with IPFS components for resilient data storage.
- **API Gateway**: Provides a RESTful HTTP interface for external interaction.
- **Multi-Modal Operation**: Can run in standalone, local, or networked modes.

## Architecture

The backend is modularized into several key components:

- **`src/entry/cmd`**: Entry points for the CLI application.
- **`src/internal/protocol`**: Core P2P protocol implementation.
- **`src/internal/server`**: HTTP gateway and P2P server logic.
- **`src/internal/wallet`**: Cryptographic wallet management.

## Getting Started

To run the backend, you can use the provided Makefile:

```bash
cd src
make build
make run
```

Refer to the [Architecture](architecture.md) and [API Reference](api.md) for more details.
