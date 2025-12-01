# Configuration

The OCF backend can be configured via command-line flags, environment variables, or a configuration file.

## CLI Flags

- `--config`: Path to config file (default: `$HOME/.ocf/config.yaml`)
- `--mode`: Operation mode (`standalone`, `local`, `networked`)
- `--port`: HTTP API port (default: `8092`)
- `--p2p-port`: LibP2P listening port (default: `4001`)

## Environment Variables

Prefix all variables with `OCF_`.

- `OCF_AUTH_URL`: URL for the authentication server.
- `OCF_AUTH_CLIENT_ID`: Client ID for OAuth.
- `OCF_LOG_LEVEL`: Logging level (`debug`, `info`, `warn`, `error`).

## Configuration File (`config.yaml`)

```yaml
node:
  mode: "networked"
  identity_file: "~/.ocf/identity.key"

network:
  listen_addrs:
    - "/ip4/0.0.0.0/tcp/4001"
    - "/ip4/0.0.0.0/udp/4001/quic"
  bootstrap_peers:
    - "/dns4/bootstrap.ocf.network/tcp/4001/p2p/Qm..."

api:
  port: 8092
  cors_allowed_origins: ["*"]
```
