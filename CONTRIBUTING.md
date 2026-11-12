# Contributing

Use synthetic payment data only. Run cargo fmt, cargo test --locked and cargo clippy --locked --all-targets -- -D warnings before proposing changes. Add tests for new results, validation rules, report fields and HTTP behavior.

Keep file/network code outside the core engine. Preserve one report entry per payment ID. Update generated OpenAPI and API/migration documents whenever the public contract changes. Run the benchmark harness when changing reconciliation algorithms; record hardware and workload rather than asserting unmeasured speedups.

Contributions are under the repository's MIT licence. GitHub pull requests become available after the repository is published.