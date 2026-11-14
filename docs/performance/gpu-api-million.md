# Durable GPU API: one million payments

Measured on 2026-11-14 with the RTX 5070 Laptop GPU. This is an HTTP and durable
storage benchmark, not a shader-only benchmark. The run passed recovery validation.

## Result

| Measurement | Result |
|---|---:|
| Distinct payments | 1,000,000 |
| Source events durably committed | 2,000,000 |
| HTTP batches | 489 |
| Full batch | 2,048 payments / 4,096 events |
| Concurrent requests | 1 |
| Ingestion wall time, including client preparation and validation | 30.479 s |
| End-to-end payment throughput | 32,809 payments/s |
| End-to-end source-event throughput | 65,619 events/s |
| HTTP batch latency p50 / p95 / p99 | 47.86 / 74.76 / 90.43 ms |
| Maximum batch latency | 216.76 ms |
| Forced restart to listening/ready | 8.565 s |
| Instrumented worker initialization plus recovery | 7.740 s |
| GPU submission portion of replay | 0.761 s |
| Post-restart payment snapshots verified through HTTP | 2,001 |
| Retained GPU database + WAL + shared memory | 843,635,848 bytes (0.844 GB) |

Latency is per **batch**, not single-payment latency. Acknowledgements follow a
SQLite WAL transaction committed with `synchronous=FULL`. Rates count reconciled
payment evidence, not money movements or settlement transactions.

## Workload and correctness

The client generates deterministic unique payment and event IDs, exact integer
amounts, GBP currency and version 1 on each source. Both source events for a payment
are in the same request. There are 900,000 matches, 50,000 amount mismatches and
50,000 combined amount/status mismatches. The last batch has 576 payments.

The harness checks every returned payment ID, both applied-source outcomes and the
expected result, and requires contiguous commit sequences. It then force-kills the
API rather than requesting a graceful shutdown. Restart replays every committed
batch on the GPU, compares its computed receipts/changes with the journal and
checks every persisted event identity before opening the listener.

After readiness, the harness checks 2,001 distributed payment snapshots, including
the first and last IDs, exact amounts, versions and reconciliation results. An
exact retry of the final batch must return `duplicate=true` with the original
sequence. SQLite counts confirm 489 requests and 2,000,000 identities, and
`PRAGMA integrity_check` returns `ok`.

The 2,001 individual HTTP reads took 60.13 seconds. This separate correctness audit
is excluded from ingestion and restart-to-ready timings. It is not a full
million-snapshot readback; the full-data check is the journal replay verification.

## Where the time went

The actor processes requests serially, so its measured stage totals can be summed.

| Stage, summed over 489 batches | Time |
|---|---:|
| Validation, canonical request encoding and identity queries | 6.987 s |
| GPU submission, including host packing, transfer, synchronization and result handling | 1.197 s |
| Transaction begin, journal/identity inserts, hashing and persisted-result encoding | 4.099 s |
| Commit, including any synchronous checkpoint work | 4.638 s |
| Total measured actor service | 16.925 s |
| HTTP round trips minus actor service (combined residual) | 8.588 s |
| Client dataset construction and request JSON encoding | 3.855 s |
| Client response parsing and assertions | 0.815 s |

The residual includes HTTP parsing/encoding, scheduling/queueing, logging and
transport; these measurements do not isolate each of those costs. Small remaining
wall time includes client loop overhead, copies and progress output.

Identity and persistence stages account for about **93% of actor service time**.
The GPU submission stage is about **4% of the complete ingestion wall time**.
It is not a pure kernel timing. Even eliminating this whole stage would only remove
about 1.2 seconds from this particular sequential run.

The slowest request was batch 403: 216.76 ms end to end, with 165.47 ms in the
journal-write stage and only 2.41 ms in GPU submission. This locates the spike but
does not establish whether its underlying cause was file growth, scheduling,
storage or security scanning. Average actor service remained roughly 33–36 ms
across the four quarters of the run; there was no large sustained degradation.

Recovery's 7.740 s worker time comprises 1.644 s before replay (database setup and
integrity checks plus GPU initialization), 0.761 s of GPU replay submission, and
approximately 5.334 s of other replay work. The remaining 0.826 s of restart-to-ready
time sits outside that worker timer. Initial cold process launch took 27.534 s even
though its empty worker startup took 0.424 s; the smoke run also showed a long
launch delay. The cause of that launch delay has not been established.

## Next improvements

1. **Reduce identity/journal overhead while preserving durability.** Reuse prepared
   statements, measure per-event lookup/encoding costs, and evaluate a compact
   append journal with a recoverable identity index. Do not remove conflict checks
   or weaken FULL commit semantics to improve benchmark numbers.
2. **Measure bounded pipelining and response costs.** Compare a small concurrency
   sweep on the same workload, then consider compact receipts and overlapping
   input preparation with GPU work. This run is a single-client baseline, not a
   saturation benchmark. Preserve commit-before-ack and failure isolation.
3. **Add verified checkpoints for GPU recovery.** Restore a durable checkpoint and
   replay only its journal tail, with atomic checkpoint publication and complete
   identity/version recovery. Full replay currently grows with retained history.
4. **Refine latency profiling.** Separate HTTP decode, queue wait, response encode,
   SQLite work and checkpoint costs; investigate the 217 ms tail and process-launch
   delays before setting an operational latency objective.

These improve the surrounding GPU system. The evidence does not yet justify buying
more GPUs or making the shader the first optimization target.

## Reproduction and limits

From the repository root on a machine with a real discrete GPU:

```powershell
$env:GPU_BENCH_PAYMENTS = '1000000'
cargo test --release --locked --features gpu,client --test gpu_api_million -- --ignored --nocapture
Remove-Item Env:GPU_BENCH_PAYMENTS
```

For a smoke run, use `10000`. The harness creates a unique local temporary directory
and prints its location; it does not touch the normal service database. It retains
the journal, logs and `report.json`, and terminates its server processes. The test
is ignored during ordinary `cargo test` and refuses debug-profile execution.

Hardware: Intel i7-14650HX (16 cores / 24 logical processors), approximately 16 GB
RAM, RTX 5070 Laptop GPU (8,151 MiB reported), driver 617.14, Windows. Client and
server share the machine; HTTP uses loopback without TLS. Release build, capacity
1,000,000, 16 MiB body limit, 120 s request timeout, 10,000 requests/s admission
limit. Actual batch rate is only about 16 requests/s, so the rate limit is not the
measured bottleneck. GPU timing logs and normal request logs are enabled.

This is one measured million-payment run following a 10,000-payment smoke check.
The OS file cache was not flushed before restart. It tests process crash recovery,
not disk failure, machine power loss, replication, long-running ingestion, or
multiple tenants. No new CPU reconciliation performance benchmark was run.

Coarse resource sampling covered startup, ingestion, recovery and validation:
peak observed API working set was 402.7 MB and sampled GPU utilization peaked at
3%. These one-second samples can miss short kernels and are not isolated kernel
utilization or an energy-per-payment measurement.

Evidence: [raw results and per-batch profiles](gpu-api-million.json),
[source/binary hashes and environment](gpu-api-million.provenance.json),
[resource samples](gpu-api-million.resources.json), and
[benchmark implementation](../../tests/gpu_api_million.rs).

Validation: the smoke and million-payment hardware runs passed; all 129 portable
regression tests passed; strict targeted Clippy and formatting checks passed.
