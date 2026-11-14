# Contributing

Use synthetic payment data only. Run `cargo fmt`, `cargo test --locked --all-features` and `cargo clippy --locked --all-features --all-targets -- -D warnings` before proposing changes. Add tests for new results, validation rules, report fields and HTTP behavior. GPU/Kafka hardware acceptance is explicit; see [the streaming guide](docs/KAFKA_STREAMING.md).

Keep file/network code outside the core engine. Preserve one report entry per payment ID. Update generated OpenAPI and API/migration documents whenever the public contract changes. Run the benchmark harness when changing reconciliation algorithms; record hardware and workload rather than asserting unmeasured speedups.

Contributions are under the repository's MIT licence. Submit changes through GitHub pull requests with a concise explanation and validation results.
