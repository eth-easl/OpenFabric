# Setup & Installation

This guide covers how to set up your development environment for OCF.

## Prerequisites

- **Go**: v1.23.0 or later
- **Node.js**: v18 or later
- **Rust**: Latest stable
- **Docker**: For running containerized services

## Backend Setup

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/researchcomputer/OpenComputeFramework.git
    cd OpenComputeFramework
    ```

2.  **Build the backend**:
    ```bash
    cd src
    make build
    ```

3.  **Run the node**:
    ```bash
    make run
    ```

## Blockchain Setup

1.  **Install Anchor**: Follow the [Anchor installation guide](https://www.anchor-lang.com/docs/installation).

2.  **Install dependencies**:
    ```bash
    cd tokens
    yarn install
    ```

3.  **Run tests**:
    ```bash
    yarn run ts-mocha
    ```

## Local Demo

To spin up a full local environment:

```bash
docker compose -f local-demo/docker-compose.yml up
```
