# Contributing

We welcome contributions to the Open Compute Framework!

## Workflow

1.  **Fork the repository**.
2.  **Create a feature branch**: `git checkout -b feature/my-new-feature`.
3.  **Commit your changes**: `git commit -am 'Add some feature'`.
4.  **Push to the branch**: `git push origin feature/my-new-feature`.
5.  **Submit a Pull Request**.

## Code Style

### Go
- Follow standard Go conventions.
- Use `golangci-lint` to check your code:
    ```bash
    make lint
    ```

### Rust
- Use `cargo fmt` to format your code.
- Use `cargo clippy` for linting.

### TypeScript/JavaScript
- Use `eslint` and `prettier`.

## Testing

Please ensure all tests pass before submitting a PR.

- **Backend**: `make test`
- **Blockchain**: `yarn run ts-mocha`
