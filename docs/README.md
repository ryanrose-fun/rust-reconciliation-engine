# Documentation

Start with the [project README](../README.md). Each topic below has one primary guide.

## GPU system

- [GPU API](GPU_API.md): setup, authentication, endpoints, checkpoints and recovery.
- [GPU architecture](GPU_ARCHITECTURE.md): guarantees, limitations and roadmap.
- [Kafka streaming](KAFKA_STREAMING.md): synthetic sources, durable ingestion and fault testing.
- [GPU OpenAPI](openapi-gpu.json): machine-readable interface.

## Measurements

- [Background checkpoints](performance/gpu-api-background-checkpoints.md): latest million-payment API comparison.
- [Kafka streaming](performance/kafka-gpu-streaming.md): sustained flow, burst and restart results.
- [Earlier checkpoint comparison](performance/gpu-api-checkpoint-comparison.md).
- [Resident GPU experiments](performance/gpu-resident.md).
- [CPU/GPU comparison](performance/cpu-gpu-comparison.md): historical baseline, retained for traceability.
- [Benchmark methodology](BENCHMARKS.md).

## Existing library and event service

The snapshot library, file CLI and CPU-backed event service remain separate from the
GPU API. They are retained for existing integrations, not presented as GPU endpoints.

- [API contract](API.md) and [OpenAPI](openapi.json).
- [Deployment](DEPLOYMENT.md), [integration](INTEGRATION.md), [operations](OPERATIONS.md).
- [Partitioned integration](partitioned-integration.md).
- [Engineering readiness](ENGINEERING-READINESS.md).

File reconciliation remains available with:

```powershell
cargo run -- data/internal.json data/provider.json report.json
```

Performance JSON files are evidence attached to named reports. Local databases,
process logs, generated reports and build outputs are not source releases.
