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

- **start**: Start the node and HTTP API.
- **wallet**: Wallet management commands for node owner identification.
  - **create**: Generate a new Solana account managed by OCF.
  - **list**: Display all accounts managed by OCF (default account marked with `*`).
  - **info**: Show the default account information.
- **version**: Print version information.
- **init**: Initialize config (placeholder).
- **update**: Update the node (placeholder).

# Flags (start)

- `--wallet.account` string: Wallet account for node identification (defaults to the first managed account).
- `--account.wallet` string: Path to the keypair file for the managed account (auto-populated when using managed wallets).
- `--bootstrap.addr` string: Legacy single-source bootstrap value (HTTP URL or one multiaddr). Default: `http://152.67.71.5:8092/v1/dnt/bootstraps`.
- `--bootstrap.source` stringSlice: Ordered bootstrap sources (HTTP URL, `dnsaddr://host`, or multiaddr). Repeat the flag to add more entries.
- `--bootstrap.static` stringSlice: Static bootstrap multiaddrs that are appended after dynamic sources.
- `--seed` string: Seed for deterministic peer key (use `0` to persist/load key). Default: `0`.
- `--mode` string: `standalone`, `local`, or `node`/`full` (default `node`).
- `--tcpport` string: LibP2P TCP port. Default `43905`.
- `--udpport` string: LibP2P QUIC UDP port. Default `59820`.
- `--subprocess` string: Start a critical subprocess (kept alive by OCF).
- `--public-addr` string: Public IP address to advertise (enables bootstrap role).
- `--service.name` string: Local service name to register (e.g., `llm`).
- `--service.port` string: Local service port to register (e.g., `8080`).
- `--solana.rpc` string: Solana RPC endpoint used for SPL token verification. Default: `https://api.mainnet-beta.solana.com`.
- `--solana.mint` string: SPL token mint the node must hold (default `EsmcTrdLkFqV3mv4CjLF3AmCx132ixfFSYYRWD78cDzR`).
- `--solana.skip_verification` bool: Skip SPL token balance checks (testing only). Default `false`.
- `--cleanslate` bool: Remove local CRDT db on start. Default `true`.

# CRDT Maintenance

Tombstone compaction prevents the on-disk CRDT datastore from growing indefinitely as peers churn. Configure the compactor via the config file (`$HOME/.config/ocf/cfg.yaml`) or environment variables:

- `crdt.tombstone_retention` (duration, default `24h`): How long to retain tombstones before they become eligible for compaction. Set to `0` to disable compaction.
- `crdt.tombstone_compaction_interval` (duration, default `1h`): How frequently to attempt compaction.
- `crdt.tombstone_compaction_batch` (int, default `512`): Maximum number of tombstone records removed per run.

# Global Flags

- `--config` string: Config file path (default is `$HOME/.config/ocf/cfg.yaml`).

# Examples

Start a standalone dispatcher:

```bash
./ocf-amd64 start --mode standalone
```

Start a node using a known bootstrap multiaddr:

```bash
./ocf-amd64 start --bootstrap.addr=/ip4/1.2.3.4/tcp/43905/p2p/<BOOTSTRAP_PEER_ID>
```

Start a node with fallback bootstrap sources:

```bash
./ocf-amd64 start \
  --bootstrap.source=https://bootstrap.ocf.example.com/v1/dnt/bootstraps \
  --bootstrap.source=dnsaddr://bootstrap.ocf.example.com \
  --bootstrap.static=/ip4/198.51.100.10/tcp/43905/p2p/<BOOTSTRAP_PEER_ID>
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

# Wallet Management

Create a new wallet for node identification:

```bash
./ocf-amd64 wallet create
```

List managed wallets:

```bash
./ocf-amd64 wallet list
```

Show the default wallet information:

```bash
./ocf-amd64 wallet info
```

After creating a wallet, start the node (the CLI automatically wires the default account):

```bash
./ocf-amd64 start
```

To point at a different managed account, pass its public key explicitly:

```bash
./ocf-amd64 start --wallet.account=<wallet_address>
```
