# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The Open Compute Framework (OCF) is a decentralized computing platform that combines LibP2P-based peer-to-peer networking with Web3 blockchain integration. The platform enables distributed compute services with token-based incentives and modern web interfaces.

## Architecture Overview

### Core Components

1. **Backend (Go)** - `src/`: LibP2P-based decentralized compute fabric
   - Implements CRDT-backed service registry and peer discovery
   - HTTP gateway (port 8092) and P2P HTTP over LibP2P
   - Multi-modal operation: standalone, local, and networked modes
   - Entry points in `src/entry/cmd/` with wallet management capabilities

2. **Frontend (Next.js)** - `apps/web/`: Modern React web application
   - Web3 wallet integration with wagmi and ethers
   - Responsive UI with Radix UI and Tailwind CSS
   - TypeScript with strict configuration

3. **Proxy Service (Python)** - `apps/proxy/`: FastAPI-based OpenAI-compatible proxy
   - Async request handling with timeout management
   - Routes to OpenAI-compatible endpoints (/v1/chat/completions, etc.)
   - Docker containerization support

4. **Blockchain (Solana)** - `tokens/`: Token economics and incentives
   - Anchor framework for Solana smart contracts
   - Rust-based token implementation

5. **Documentation (Astro)** - `docs/`: Comprehensive project documentation
   - Starlight theme with professional styling

## Development Commands

### Backend (Go)

From the `src/` directory:

```bash
# Build and development
make build           # Build all applications
make build-debug     # Build with debugging capabilities
make build-release   # Build release binaries (no debug info)
make run             # Build and execute all applications

# Testing and quality
make test            # Run tests with coverage
make lint            # Run linters (golangci-lint)
make check           # Run both tests and linters

# Release management
make patch           # Release new patch version
make minor           # Release new minor version
make major           # Release new major version
```

### Frontend (Next.js)

From the `apps/web/` directory:

```bash
npm run dev          # Start development server
npm run build        # Build for production
npm run start        # Start production server
npm run lint         # Run ESLint
```

### Blockchain (Solana/Anchor)

From the `tokens/` directory:

```bash
npm run lint         # Check code formatting
npm run lint:fix     # Fix code formatting
yarn run ts-mocha    # Run TypeScript tests
```

### Proxy Service (Python)

From the `apps/proxy/` directory:

```bash
# Local development
pip install -r requirements.txt
python main.py

# Docker build and run
./scripts/build_docker.sh
docker run -p 8000:8000 researchcomputer/ocf-proxy

# Environment variables
TARGET_SERVICE_URL=http://localhost:8000/v1  # Target service URL
TIMEOUT=30.0                                 # Request timeout in seconds
```

### Documentation (Astro)

From the `docs/` directory:

```bash
npm run dev          # Start development server
npm run build        # Build documentation
npm run preview      # Preview built site
```

## Key Technical Details

### Backend Architecture

- **LibP2P Networking**: TCP, WebSocket, and QUIC transports
- **CRDT Registry**: Conflict-free replicated data types for distributed state
- **Service Discovery**: Identity-based service registration and routing
- **Multi-Transport**: HTTP gateway and P2P HTTP over LibP2P

### Routing Models

1. **Direct Service**: Forward to locally registered services
2. **Global Service**: Route to matching providers by identity groups
3. **P2P Forwarding**: Direct peer-to-peer communication

### Frontend Integration

- **Web3 Stack**: wagmi v2.16.9, ethers v6.15.0, web3modal
- **State Management**: TanStack Query for data fetching
- **UI Components**: Radix UI with Tailwind CSS
- **Type Safety**: Strict TypeScript configuration

### Blockchain Integration

- **Solana Program ID**: `xTRCFBHAfjepfKNStvWQ7xmHwFS7aJ85oufa1BoXedL`
- **Anchor Framework**: Rust smart contract development
- **Token Economics**: Platform incentive mechanisms

## Important Build Information

### Go Backend

- **Version**: Go 1.23.0 required
- **Build Output**: `build/` directory
- **Entry Points**: `src/entry/cmd/` with CLI commands (start, config, wallet, etc.)
- **Linting**: golangci-lint v1.61.0
- **Testing**: gotestsum v0.4.2 with coverage reports
- **Wallet Management**: Integrated wallet functionality for node owner identification

### Multi-Architecture Support

- **AMD64**: Default build target
- **ARM64**: Available via `make arm` target
- **Release Builds**: Stripped binaries in `build/release/`

### Environment Variables

Key build-time environment variables:
- `AUTH_URL`, `AUTH_CLIENT_ID`, `AUTH_CLIENT_SECRET`
- `SENTRY_DSN` for error tracking
- `VERBOSE=1` for verbose build output

## Development Workflow

1. **Backend Changes**: Work in `src/`, use `make build && make run` for testing
2. **Frontend Changes**: Work in `apps/web/`, use `npm run dev` for hot reload
3. **Proxy Service**: Work in `apps/proxy/`, use `python main.py` for local testing
4. **Blockchain Changes**: Work in `tokens/`, use Anchor tooling for deployment
5. **Documentation**: Work in `docs/`, use Astro dev server for preview

## Code Style and Conventions

### Go Backend
- Follow standard Go formatting and conventions
- Use golangci-lint for code quality
- Comprehensive test coverage with gotestsum

### Frontend
- TypeScript strict mode enabled
- ESLint configuration for code quality
- Tailwind CSS utility classes for styling
- Radix UI components for accessibility

### Proxy Service
- FastAPI with async request handling
- Python type hints with pydantic validation
- Docker containerization for deployment
- Environment-based configuration

### Blockchain
- Rust smart contracts with Anchor framework
- TypeScript tests for contract verification
- Prettier formatting for JavaScript/TypeScript files