# Group commit foundation (work in progress)

This document records the initial storage foundation and its exploratory benchmark. The subsequent [partitioned API integration](partitioned-integration.md) connects group commit and fixed partitions to live HTTP ingestion; it does not complete the broader six-part scaling programme.

## Implemented

- `EventStore::apply_batch`: 1–256 ordered events, one SQLite IMMEDIATE transaction and FULL synchronous commit. All receipts are returned only after commit. An invalid/conflicting event rolls back the entire batch, including earlier history and webhook inserts. Callers must retry the original event IDs after an uncertain result.
- Single-event writes use the same transaction implementation, preventing divergent rules.
- `PartitionedStore`: fixed tenant-specific directories with 1–64 databases, SHA-256 routing using a length-prefixed tenant ID and payment ID. The layout is persisted transactionally; changing tenant, count or routing algorithm is rejected. Existing single-file databases are not migrated automatically.
- Tests cover batch rollback, history/source rollback, duplicate handling, event ordering, size bounds and layout restart/mismatch rejection.

Partitioning is an embedded building block. `for_payment` returns a store; callers must route every batch member to that same partition. Independent stores can run concurrently. There is no cross-partition transaction or cross-partition event-ID uniqueness check. Existing global event-ID semantics must be addressed before this is wired into the HTTP API. This is not replicated storage or automatic failover.

## Reproduce

```powershell
cargo fmt --check
cargo test --locked --all-features
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo test --locked --manifest-path perf/Cargo.toml
cargo run --release --example group_commit_benchmark
```

The benchmark uses fresh local temporary databases, 4,096 unique internal-only events per case, three rounds, and batch sizes 1/8/32/128. It reopens every database and checks every stored payment. Temporary databases are retained for inspection and their directory is printed. The timing includes event cloning and transaction work; restart validation is outside the timer.

Exploratory Windows results in `group-commit-local.json`: one-event commits 603–706 events/s; batches of 32: 4,980–5,936; batches of 128: 6,072–8,367. Compilation overlapped part of this run. These figures support investigating amortized commit costs, not a controlled speedup claim. They do not measure batch accumulation latency, HTTP costs, mixed events, partition scaling, long-term growth or machine failure. Graceful reopen is not a power-loss test.

## Outstanding acceptance gates

1. HTTP group commit is implemented; runtime validation and p99 measurements including accumulation delay remain.
2. Opt-in partitioned service routing, source authorization, payment-bound event identity, per-partition pagination, deadlines and metrics are implemented. Migration tooling and concurrent benchmarks remain.
3. Kafka ingestion: real broker integration, durable offset handling, rebalance/replay tests and bounded buffering.
4. Replication/failover: select and implement a replicated storage design, fencing and failure-injection tests. SQLite partition files alone do not provide this.
5. Retention: verified archival plus safe reclamation while retaining idempotency and audit guarantees, tested against aged large datasets.
6. Enterprise operations: hot credential rotation, service identity, tenant quotas, tracing, alerts, deployment and security checks.

Do not label these remaining items complete or advertise bank-scale throughput from this branch.
