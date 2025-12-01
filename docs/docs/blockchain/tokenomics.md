# Tokenomics

The OCF token model is designed to align incentives between compute providers, consumers, and the network protocol.

## Roles

- **Providers**: Entities that offer compute resources (CPU, GPU, Storage). They earn tokens by fulfilling jobs.
- **Consumers**: Users or applications that require compute resources. They pay tokens to access services.
- **Validators**: Nodes that verify the correctness of compute results (if applicable).

## Mechanisms

### Staking
Providers are required to stake a minimum amount of OCF tokens to register their service.
- **Purpose**: Sybil resistance and quality assurance.
- **Slashing**: If a provider acts maliciously or fails to deliver, a portion of their stake may be slashed.

### Payments
Payments are settled on-chain but may utilize state channels for high-frequency transactions.
1.  **Escrow**: Consumer locks tokens in a contract before job execution.
2.  **Execution**: Provider performs the task.
3.  **Settlement**: Upon verification, tokens are released to the provider.

### Governance
Token holders may participate in the governance of the protocol, voting on parameter updates (e.g., minimum stake, fees) and upgrades.
