# Rust GPU Finance

GPU-resident payment reconciliation in Rust. Compare bank/internal and provider
payment evidence, identify disagreements, and recover committed state after restart.
This engine reviews payments; it does not execute or settle them.

## What it does

- Exact integer amounts, currencies, versions and duplicate detection.
- A resident GPU worker behind an authenticated HTTP API.
- Durable acknowledgements and background checkpoints with verified recovery.
- Kafka ingestion and independent Rust payment generators for streaming experiments.
- Reproducible correctness, recovery and performance tests.

## Get started

Requires Rust 1.99.1. GPU mode needs a supported discrete GPU; Kafka streaming also
needs a running broker. The measured development system uses an RTX 5070 Laptop GPU.

```powershell
cargo test --locked --all-features
# Set RECONCILE_API_KEY, then follow the GPU API configuration guide:
.\scripts\run-gpu-api.ps1
```

- **[GPU API setup](docs/GPU_API.md)** — configuration, requests and restart recovery.
- **[Kafka streaming guide](docs/KAFKA_STREAMING.md)** — broker, generators, ingestion and fault tests.
- **[Architecture](docs/GPU_ARCHITECTURE.md)** — state ownership and durability boundaries.
- **[Documentation index](docs/README.md)** — existing library/CLI, deployment and integration guides.

## Performance and validation

[Latest checkpoint measurements](docs/performance/gpu-api-background-checkpoints.md)
cover one million payments, durable HTTP ingestion and forced restart recovery.
[Kafka streaming results](docs/performance/kafka-gpu-streaming.md) cover sustained
traffic, bursts and consumer/API restart. These are workload-specific local results,
not a claim of bank deployment capacity. Raw measurements and methodology accompany
each report; older experiments remain available for comparison.

## Project layout

| Path | Purpose |
|---|---|
| `src/bin/api/` | HTTP service and durable GPU owner |
| `perf/gpu/` | Rust GPU compute library and WGSL shader |
| `src/streaming.rs`, `src/streaming/` | Kafka producer, ingestion and durable cursors |
| `src/lib.rs` | Existing snapshot reconciliation library |
| `tests/` | Correctness, integration and explicit hardware tests |
| `docs/` | Interfaces, operations and measured results |

GPU integration currently serves one tenant on one GPU per process. The Kafka lab
uses fixed partitions and a local broker; distributed failover and production Kafka
security are outside this implementation. GPU endpoints never silently use CPU
reconciliation.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for checks and contribution guidance,
[SECURITY.md](SECURITY.md) for reporting vulnerabilities, and [LICENSE](LICENSE)
for the MIT license. Dependencies retain their own licenses.
