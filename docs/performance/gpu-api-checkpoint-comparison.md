# GPU API: bounded preparation, journal reuse and verified checkpoints

Measured 2026-11-14 on the same RTX 5070 Laptop GPU and deterministic million-payment
dataset as the [original durable API benchmark](gpu-api-million.md). Both new runs
passed durable-write and restart validation. No CPU reconciliation benchmark was run.

## Before and after

Every case ingested 1,000,000 distinct payments / 2,000,000 source events in 489
requests, with up to 2,048 payments per request. Acknowledgements still follow
SQLite WAL commits with `synchronous=FULL`.

| Measurement | Original, concurrency 1 | Updated, concurrency 1 | Updated, concurrency 4 |
|---|---:|---:|---:|
| Ingestion wall time | 30.479 s | 27.103 s | **13.032 s** |
| Payments/s | 32,809 | 36,896 | **76,731** |
| Source events/s | 65,619 | 73,793 | **153,463** |
| Batch latency p50 | 47.86 ms | 40.64 ms | 88.74 ms |
| Batch latency p95 | 74.76 ms | 61.53 ms | 110.55 ms |
| Batch latency p99 | 90.43 ms | 81.70 ms | 156.65 ms |
| Maximum batch latency | 216.76 ms | 640.60 ms | 675.14 ms |
| Final checkpoint publication | Not available | 1.105 s | 1.080 s |
| Ingestion plus final checkpoint | 30.479 s (no checkpoint) | 28.208 s | 14.112 s |
| Restart process to ready | 8.565 s | 4.070 s | 4.089 s |
| Instrumented worker startup/recovery | 7.740 s | 3.211 s | 3.232 s |

The updated single-request case isolates the implementation change at the original
concurrency: throughput improved about **12.5%**, despite including an automatic
checkpoint. Four in-flight requests achieved **2.34x the original throughput**, or
about **2.08x the updated single-request throughput**. Including the separately
timed final checkpoint, the four-request run finished in 14.112 seconds.

These are observed single runs, not confidence intervals or a maximum-capacity
claim. Concurrency increases queueing: p95 batch latency is worse at four requests
than at one. Periodic checkpoints pause the owner, explaining the approximately
0.6-second tail spikes. Batch latency is not individual-payment latency.

## What changed

### 1. Identity and journal overhead

The actor now reuses prepared SQLite statements across the per-event lookup and
insert loops. Canonical event JSON is generated once and reused for conflict checks
and persistence. Input encoding/hashing is prepared before entering the GPU owner.
Request and event ID conflict semantics, exact integer values and FULL commits are
unchanged. There is no unbounded in-memory identity cache replacing durable checks.

### 2. Bounded pipeline around the GPU owner

Up to 32 preparation jobs are admitted; each permit belongs to its prepared command
until that command finishes. Cancellation of an HTTP waiter does not release the
permit while work remains queued or executing. Preparation uses blocking tasks so
encoding does not block an async executor thread. HTTP admission/body limits and
the 32-command actor queue remain enforced.

This overlaps preparation and HTTP processing with earlier GPU/durable-write work.
The GPU owner still processes state changes and commits serially. It does not
speculatively serve reads from GPU state while an asynchronous commit is pending.
Source versions continue to govern concurrent/out-of-order updates.

### 3. Verified GPU checkpoints

The owner reads occupied source/result buffers directly from the GPU and stores a
compact little-endian checkpoint with IDs in slot order. Payload SHA-256, committed
sequence, journal-prefix hash chain and identity-index manifest are published in
one SQLite FULL transaction. No CPU reconciliation is used to reconstruct results.
A failed publication retains the previous complete checkpoint.

Startup validates metadata, SQLite integrity, payload and manifests, restores GPU
buffers and compares a complete GPU readback. It scans retained journal checksums
and the prefix chain, then replays only batches after the checkpoint. Identity
checks remain durable across restore. Corruption fails startup; it does not silently
fall back to a potentially inconsistent checkpoint or CPU computation.

Automatic publication defaults to every 256 committed batches. The measured runs
include a checkpoint at batch 256 in ingestion time and request latency. A final
explicit checkpoint at batch 489 is timed separately, then the process is force-killed.
The complete checkpoint has a **67,000,012-byte payload**. Restore replays zero tail
batches for these complete-checkpoint benchmark cases. Separate hardware acceptance
exercises a checkpoint followed by later committed batches and verifies tail replay.

The retained journal is not deleted: startup still scans integrity, so recovery is
not constant-time with arbitrary history. Checkpoint storage increased retained
database/WAL/shared-memory files from about **0.844 GB to 1.010 GB** in these runs.
Do not compare the recovery reduction without also accounting for publication cost
and retained history.

## Remaining bottlenecks

Accumulated actor timings in the four-request run:

| Stage | Time |
|---|---:|
| Identity lookups | 4.432 s |
| GPU submission, transfers, synchronization and result handling | 1.151 s |
| Journal transaction work | 1.856 s |
| Durable commits | 4.159 s |
| Actor service total, excluding checkpoint pauses | 11.603 s |
| Preparation tasks, overlapping actor service | 0.966 s |

Preparation moved outside the actor; identity-stage timing boundaries therefore
differ from the original benchmark. The actor's total fell from 16.925 s originally
to 11.849 s at concurrency 1. Concurrent request durations overlap and must not be
summed to derive wall time. GPU submission includes host packing and is not a pure
kernel timing.

The four-request actor is occupied for most of ingestion. Identity lookup and
durable commits remain the largest costs. More GPUs alone would not remove that
single durable-owner bottleneck. Next measurements should investigate incremental
checkpoint manifests/publication, finer SQL and queue timing, and a controlled
concurrency sweep; preserve commit ordering and failure isolation throughout.

## Validation and reproduction

- Every ingestion receipt is checked against deterministic expected IDs, outcomes
  and reconciliation results; all committed sequence numbers must be unique and
  contiguous even when concurrent responses arrive out of order.
- Both million-payment cases force-kill and restart, verify the checkpoint boundary,
  inspect 2,001 distributed GPU snapshots, retry the final input using its original
  identity/sequence, count all 2,000,000 stored event identities and run SQLite
  integrity checks. Repeated explicit checkpoints at the same boundary are no-ops.
- Portable tests cover malformed checkpoints, changed/missing identity rows,
  bounded preparation and cancellation retaining its admission permit.
- Hardware acceptance covers checkpoint restore of exact `u64::MAX` values, tail
  replay, stale/conflicting versions, post-GPU storage failure, failed checkpoint
  publication retaining its predecessor, and checkpoint/journal corruption rejection.
  The final hardware run also passed concurrent conflicting event-ID requests and
  a forced kill with checkpoint publication potentially in flight. The test accepts
  either complete checkpoint boundary; it does not assume a particular instruction
  was interrupted. Final hardware acceptance passed in 220.01 s (including process
  launches). All 132 portable regression tests, checkpoint codec tests, formatting
  and strict Clippy checks passed.

Run each case separately from the repository root:

```powershell
$env:GPU_BENCH_PAYMENTS = '1000000'
$env:GPU_BENCH_CONCURRENCY = '1' # repeat with '4'
cargo test --release --locked --features gpu,client --test gpu_api_million -- --ignored --nocapture
Remove-Item Env:GPU_BENCH_PAYMENTS, Env:GPU_BENCH_CONCURRENCY
```

The harness creates an isolated local temporary directory, retains its evidence and
stops its API process. It never modifies the ordinary service database. Release
mode, same laptop, HTTP loopback, synthetic data, unflushed OS file cache and one
measured run per concurrency. The cold-launch delay on this machine is variable;
the report distinguishes process-to-ready from instrumented worker recovery.
This is process-crash recovery, not a power-cut or disk-loss test.

Raw evidence: [original](gpu-api-million.json),
[updated concurrency 1](gpu-api-million-checkpoint-c1.json),
[updated concurrency 4](gpu-api-million-checkpoint-c4.json),
[source/binary provenance](gpu-api-checkpoint-comparison.provenance.json).
