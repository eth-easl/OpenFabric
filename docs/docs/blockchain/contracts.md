# Smart Contracts

The OCF smart contracts are implemented using the Anchor framework on Solana.

## Program Structure

The `tokens/programs` directory contains the Rust source code for the on-chain logic.

### Instructions

Common instructions supported by the program:

- `initialize`: Sets up the global state of the protocol.
- `register_provider`: Registers a new compute provider with a stake.
- `create_job`: A consumer initiates a compute job and locks funds.
- `complete_job`: A provider signals job completion.
- `dispute_job`: A consumer or validator raises a dispute regarding the result.

## Accounts

- **GlobalState**: Stores protocol-wide parameters.
- **ProviderAccount**: Stores provider details, stake balance, and reputation.
- **JobAccount**: Stores the state of a specific compute job.

## Integration

The frontend and backend interact with these contracts via the Solana JSON RPC API, typically using client libraries like `@solana/web3.js` or the Anchor client.
