# OpenComputeFramework (OCF) - Future Development Roadmap

## Core Architecture & Protocol

- [ ] **CRDT State Synchronization Improvements**
  - Implement conflict resolution policies for service registration (e.g., last-write-wins vs. merge policies)
  - Add version vectors or vector clocks to detect and resolve concurrent updates to peer records
  - [x] Introduce tombstone compaction for deleted peers to prevent datastore bloat
  - Add CRDT health monitoring and automatic repair mechanism (e.g., periodic state sync with bootstrap nodes)

- [ ] **P2P Network Resilience**
  - Implement peer scoring and reputation system to prioritize connections with reliable nodes
  - [x] Add automatic reconnection logic with exponential backoff for transient network failures
  - Implement connection rate limiting to prevent DDoS or misbehaving peers
  - Add support for rendezvous servers for NAT traversal in restrictive networks

- [ ] **Bootstrap Discovery Enhancements**
  - Implement DNS-based bootstrap discovery (e.g., `_ocf-bootstrap._tcp.example.com`)
  - [x] Add support for multiple bootstrap sources (HTTP, DNS, static list) with fallback logic
  - Implement bootstrap node validation to prevent malicious bootstrap lists
  - Add bootstrap node rotation and load balancing for high availability

## Service Registry & Routing

- [ ] **Service Discovery & Registration**
  - Implement service health checks (HTTP/TCP) for registered services to auto-remove unhealthy providers
  - Add service metadata (e.g., latency, cost, region) to enable intelligent routing decisions
  - Implement service versioning and compatibility checks for API endpoints
  - Add support for dynamic service deregistration (e.g., on process exit)

- [ ] **Intelligent Request Routing**
  - Implement load balancing algorithms (round-robin, least-connections, weighted) for global service routing
  - Add geographic routing based on peer location (IP geolocation or explicit location metadata)
  - Implement cost-aware routing (e.g., prioritize free or low-cost providers)
  - Add request prioritization and QoS support (e.g., high-priority requests bypass queues)

- [ ] **Identity & Access Control**
  - Implement JWT-based authentication for service access (e.g., `model=resnet50` requires valid token)
  - Add role-based access control (RBAC) for service registration and routing
  - Implement peer identity verification using public key signatures
  - Add audit logging for all service access attempts

## Server & API Layer

- [ ] **HTTP API Enhancements**
  - Add OpenAPI/Swagger documentation for all API endpoints
  - Implement rate limiting and request throttling per peer/IP
  - Add pagination and filtering for `/v1/dnt/table` and `/v1/dnt/peers` endpoints
  - Implement WebSocket support for real-time event streaming (e.g., peer join/leave)

- [ ] **Observability & Monitoring**
  - Add Prometheus metrics endpoint (`/metrics`) for key metrics (peers, requests, latency, errors)
  - Implement structured logging with trace IDs for end-to-end request tracing
  - Add support for OpenTelemetry integration (beyond Axiom)
  - Implement alerting for critical events (e.g., no active peers, high error rate)

- [ ] **Security Hardening**
  - Restrict CORS origins to trusted domains instead of `*`
  - Implement mutual TLS (mTLS) for P2P communication
  - Add request signature validation for all forwarded requests
  - Implement input sanitization and validation for all API endpoints

## Entry Point & Configuration

- [ ] **CLI & Configuration Management**
  - Add config validation and schema checking (e.g., using JSON Schema)
  - Implement config hot-reload without restarting the node
  - Add config export/import functionality for backup and migration
  - Implement environment variable overrides for all config options

- [ ] **Deployment & Orchestration**
  - Create Helm chart for Kubernetes deployment
  - Add Docker Compose templates for local development and testing
  - Implement systemd service files for production deployment
  - Add health check endpoints for container orchestration (e.g., `/healthz`)

## Documentation & Developer Experience

- [ ] **Documentation Updates**
  - Update architecture.md with latest routing logic and CRDT details
  - Add sequence diagrams for service registration and request routing
  - Document all CLI flags and config options in a single reference page
  - Add troubleshooting guide for common issues (e.g., connection failures, bootstrap errors)

- [ ] **Developer Tooling**
  - Create a CLI tool for debugging and inspecting the local node state
  - Add unit test coverage for all core packages (protocol, server, common)
  - Implement integration tests for P2P communication and service routing
  - Add a local development mode with mock services for testing

## Performance & Scalability

- [ ] **Performance Optimization**
  - Profile and optimize CRDT serialization/deserialization performance
  - Implement connection pooling for HTTP proxying to reduce overhead
  - Add memory usage monitoring and limits to prevent OOM crashes
  - Optimize DHT lookups and reduce latency for global service routing

- [ ] **Scalability Improvements**
  - Implement sharding of the node table for large networks (>10k peers)
  - Add support for hierarchical routing (e.g., regional clusters)
  - Implement caching of frequently accessed service records
  - Add support for federated networks (multiple independent OCF networks)

## Integration & Ecosystem

- [ ] **Third-Party Integrations**
  - Add support for integrating with existing ML platforms (e.g., vLLM, TGI)
  - Implement OpenAI-compatible API endpoint for seamless client integration
  - Add support for Kubernetes CSI for dynamic resource provisioning
  - Integrate with blockchain for decentralized payment and incentive mechanisms

- [ ] **Community & Ecosystem**
  - Create a public registry of known bootstrap nodes
  - Implement a plugin system for custom service types
  - Add a web-based dashboard for monitoring the network
  - Create a developer SDK for Go, Python, and JavaScript
