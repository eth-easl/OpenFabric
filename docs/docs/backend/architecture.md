# Backend Architecture

The OCF backend is designed as a modular, layered system.

## Core Components

### 1. Networking Layer (`libp2p`)
The foundation of the node is the LibP2P host. It manages:
- **Transport**: Handling connections via TCP, WebSocket, or QUIC.
- **Security**: Encrypting communications using Noise or TLS.
- **Multiplexing**: Running multiple streams over a single connection (Yamux/Mplex).
- **Peer Discovery**: Finding other nodes via mDNS (local) or DHT (global).

### 2. Protocol Layer (`src/internal/protocol`)
This layer defines the application-specific protocols used by OCF nodes to communicate.
- **Service Advertisement**: Nodes broadcast their available compute resources.
- **Job Negotiation**: Protocols for requesting and accepting compute jobs.
- **Status Updates**: Real-time updates on job progress.

### 3. Service Layer (`src/internal/server`)
The service layer exposes the node's functionality to the outside world.
- **HTTP Gateway**: A Gin-based REST API listening on port 8092 (default).
- **P2P HTTP**: Allows tunneling HTTP requests over LibP2P streams, enabling access to services behind NATs.

### 4. Data Layer
- **Datastore**: Uses `go-datastore` interfaces to abstract storage backends.
- **CRDTs**: Conflict-Free Replicated Data Types are used for shared state that requires eventual consistency, such as the service registry.

## Data Flow

1.  **Initialization**: The node starts, generates/loads identity keys, and initializes the LibP2P host.
2.  **Discovery**: The node connects to bootstrap peers and joins the DHT.
3.  **Advertisement**: The node publishes its capabilities to the network.
4.  **Request Handling**:
    - **Direct**: A client sends an HTTP request to the gateway.
    - **P2P**: A peer sends a request over a LibP2P stream.
5.  **Execution**: The request is routed to the appropriate internal handler or forwarded to another peer.
