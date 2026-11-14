# Background GPU checkpoints: million-payment results

Measured 2026-11-14 on the RTX 5070 Laptop GPU. Background publication removes the
large ingestion stall while preserving commit-before-acknowledgement. It does not
eliminate checkpoint I/O or materially increase sustained throughput in these runs.

## Same workload, different publication path

Each run reconciles 1,000,000 payments / 2,000,000 source events in 489 HTTP batches,
up to 2,048 payments per request. Data, GPU shader, batching and automatic checkpoint
interval (256 committed batches) match the preceding synchronous-checkpoint runs.
SQLite WAL synchronous FULL remains enabled. Requests are acknowledged only after
their journal transaction commits. Client and API share the same Windows laptop.

| Measurement | Sync C1 | Background C1 | Sync C4 | Background C4 |
|---|---:|---:|---:|---:|
| Durable ingestion, seconds | 27.103 | 26.488 | 13.032 | 12.949 |
| Payments/second | 36,896 | 37,753 | 76,731 | 77,228 |
| Batch p50, ms | 40.64 | 37.70 | 88.74 | 91.37 |
| Batch p95, ms | 61.53 | 62.64 | 110.55 | 118.08 |
| Batch p99, ms | 81.70 | 82.27 | 156.65 | 161.91 |
| Worst batch, ms | 640.60 | 111.41 | 675.14 | 197.60 |
| Final checkpoint wait, seconds | 1.105 | 1.402 | 1.080 | 1.350 |
| Ingestion + final checkpoint, seconds | 28.208 | 27.890 | 14.112 | 14.299 |
| Restart to HTTP ready, seconds | 4.070 | 4.038 | 4.089 | 4.325 |

C1/C4 mean one/four concurrent HTTP requests, not GPU cores. Latencies describe
whole batches, not individual payment latency. Worst batch latency fell 82.6% at
C1 and 70.7% at C4. Throughput increased 2.3% / 0.6%; those small differences should
not be treated as statistically established gains. There is one measured run per
case, the OS cache was not flushed, and background activity can affect timings.

The new p95/p99 and C4 ingestion-plus-final-checkpoint time are slightly worse.
The improvement is removal of the extreme publication pause, not a claim that all
latencies or total completion times improved.

## Where the pause went

The owner captures immutable GPU buffers and IDs at a committed journal boundary.
Encoding, SHA-256, identity-prefix verification and SQLite publication then run on
one background thread, using a separate `gpu-checkpoints.sqlite3` database. This
avoids competing for the ingestion journal's SQLite writer lock. GPU reconciliation
continues on the existing resident GPU worker; no CPU reconciliation path was added.

| Checkpoint | C1 capture | C1 total publication | C4 capture | C4 total publication |
|---|---:|---:|---:|---:|
| Automatic, sequence 256 (35,127,308 bytes) | 22.71 ms | 686.46 ms | 22.81 ms | 740.84 ms |
| Final, sequence 489 (67,000,012 bytes) | 45.20 ms | 1,381.35 ms | 51.41 ms | 1,328.55 ms |

Previously the automatic publication occupied the owner for about 590–597 ms.
Only capture now occupies it, about 23 ms here. Total publication includes capture,
encoding, hashing, identity scan, FULL commit and connection-close work. Background
durations overlap ingestion and must not be added to ingestion wall time.

Only one snapshot can be in flight. Manual checkpoint requests have at most 32
waiters and can share a later publication that covers their requested boundary.
They receive success only after publication completes. Shutdown joins the publisher
before releasing the exclusive journal ownership lock.

## Durability and recovery evidence

Both million-payment runs passed: all 489 batches and 2,000,000 events were recovered
after forced API termination; 2,001 payment snapshots per run were checked; stored
receipts, duplicate retry behavior, checkpoint boundary and database integrity were
validated. Recovery restored checkpoint 489 with no GPU tail replay. Worker startup
itself took 3.197 / 3.241 seconds; process-to-HTTP-ready includes additional overhead.

The discrete-GPU recovery acceptance test also passed, covering ingestion while the
checkpoint writer is deliberately blocked, publication failure retaining the prior
checkpoint, replay after the checkpoint boundary, legacy embedded checkpoint loading,
new checkpoint corruption rejection even when a legacy checkpoint exists, ambiguous
retry, journal failure, and exclusive ownership. Physical power failure and disk loss
were not simulated. FULL durability still depends on the storage/OS honoring flushes.

Portable regression checks passed 133 tests, plus both GPU checkpoint codec tests.
Root and GPU crate formatting and strict all-target Clippy checks passed.

The main journal is authoritative. A published snapshot references an already
committed boundary, checksum chain and identity prefix. Startup verifies these before
serving; it rejects corrupt published state rather than silently falling back. Failed
publication stops the worker when observed, leaving the previous snapshot and durable
journal available for recovery. Existing embedded checkpoints remain readable.

## Remaining costs and next improvements

At C4, accumulated owner timings were 4.50 s for durable commits, 4.51 s for identity
work, 1.90 s for journal insertion and 1.14 s for GPU apply. Preparation ran outside
the owner (0.99 s accumulated). These stage sums are not independent wall-time totals.
The next GPU-focused checkpoint work should investigate incremental snapshots or
chunked publication to reduce full-state copying and I/O, while retaining atomic
publication and a verified recovery boundary. Recovery still scans retained history;
it is not constant-time.

Both databases plus WAL/SHM occupied about 0.957 GB (C1) / 0.982 GB (C4) before kill.
The separate checkpoint file was 102 MB, including reusable pages from replacement.
Nothing is pruned by this change. Back up the complete data directory after a clean
shutdown, including both databases and any WAL/SHM files.

## Evidence and reproduction

- [Background C1 raw measurements](gpu-api-million-background-c1.json)
- [Background C4 raw measurements](gpu-api-million-background-c4.json)
- [Build hashes and hardware provenance](gpu-api-background-checkpoints.provenance.json)
- [Previous synchronous checkpoint comparison](gpu-api-checkpoint-comparison.md)

```powershell
$env:GPU_BENCH_PAYMENTS = '1000000'
$env:GPU_BENCH_CONCURRENCY = '1' # repeat with '4'
cargo test --release --locked --features gpu,client --test gpu_api_million -- --ignored --nocapture
cargo test --release --locked --features gpu,client --test gpu_api_recovery -- --ignored --nocapture
```

Hardware tests require the discrete GPU and use isolated local temporary directories.
They retain evidence and stop their API processes. This is a local performance and
process-recovery measurement, not multi-node or bank deployment qualification.
