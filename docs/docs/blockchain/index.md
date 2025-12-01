# Blockchain Overview

The OCF blockchain layer handles the economic incentives and identity management for the network. It is built on the **Solana** blockchain using the **Anchor** framework.

## Key Concepts

- **Tokenomics**: A native token is used to pay for compute resources and reward providers.
- **Staking**: Providers must stake tokens to advertise their services, ensuring commitment and quality of service.
- **Payment Channels**: Micro-payments are facilitated through on-chain channels to minimize transaction costs and latency.
- **Identity**: On-chain identities are linked to off-chain P2P nodes via cryptographic signatures.

## Smart Contracts

The smart contracts (programs) are written in Rust and reside in the `tokens/` directory.

- **Program ID**: `xTRCFBHAfjepfKNStvWQ7xmHwFS7aJ85oufa1BoXedL`

## Development

To work with the blockchain components, you need the Solana CLI and Anchor installed.

```bash
cd tokens
npm run lint
yarn run ts-mocha
```
