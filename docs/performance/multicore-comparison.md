# Multicore CPU bulk reconciliation — 14 November 2026

**Sixteen workers reduced median complete-batch latency to about 129 ms**, roughly
2.3 times the throughput of the single-threaded CPU implementations for this workload.

| Implementation | First-run median | Confirmation median |
|---|---:|---:|
| Original direct CPU | 298.23 ms | 302.64 ms |
| Original packed CPU | 295.65 ms | 292.63 ms |
| Original GPU-assisted path | 313.94 ms | 315.14 ms |
| New sharded CPU, 1 worker | 441.73 ms | 445.31 ms |
| New sharded CPU, 4 workers | 182.75 ms | 182.18 ms |
| New sharded CPU, 8 workers | 140.44 ms | 135.25 ms |
| New sharded CPU, 16 workers | **128.40 ms** | **128.57 ms** |

Against the original direct CPU path, 16 workers took about **57% less time**
(2.32x and 2.35x throughput). Against the faster packed CPU baseline, improvement
was 2.30x and 2.28x. Against this GPU-assisted implementation, improvement was about
2.45x. These are ratios of measured medians, not production capacity guarantees.
The earlier direct CPU baseline was 298–305 ms, consistent with this rerun.

The new algorithm with one worker is slower: routing and order restoration add
overhead. Parallel work overcomes that overhead at higher worker counts. Gains from
8 to 16 workers diminish to roughly 5–9% less time; worker count should be chosen
under concurrent service load rather than assuming the maximum is always best.

Both runs finished successfully: **196 measured batch outputs** matched the oracle
exactly, in addition to warmup/first-use validation. No benchmark remains running.

The multicore implementation splits records by a randomized hash of the exact
payment ID, so both sources for an ID reach the same worker. Hashes only select
workers: full string equality decides identity. Each worker owns its lookup table,
checks duplicate IDs, compares exact integer amounts/currency/status and creates
owned result rows. A final position-based merge restores the same output order as
the single-threaded reference. Any duplicate rejects the whole batch.

This is an optimization candidate in the isolated performance crate. Production
HTTP routing, durable transactions, histories and recovery are unchanged by this
experiment. It is not a new durable-event throughput result.

## Equivalent work and verification

Same deterministic fixture as the previous CPU/GPU comparison: one million records
per source, two million total inputs. Both sources are independently shuffled;
missing IDs and all mismatch combinations are represented. Input and output SHA-256
hashes match the earlier `cpu-gpu-million.json` exactly.

All paths start with borrowed typed string-ID snapshots and return identical owned
ID/result rows and duplicate counters. Preparation, allocations, scratch destruction,
comparison and output creation are timed. Multicore additionally includes routing,
thread creation/join and final order restoration. GPU includes upload/readback and
fresh buffers, while reusing its device context. Fixture generation, result destruction
and independent parity checks are excluded equally. No JSON, HTTP, storage or full
evidence reports are included on any path.

Two runs, seven implementations, two warmups per implementation, fourteen timed
trials per implementation per run. Forward and reverse rotations put each path in
each execution position twice. Every timed output is checked against the independent
sorted-map oracle. Raw samples and source/executable/host provenance are retained.

Six release unit tests passed, covering shuffled result order across worker counts,
duplicates crossing input chunks, empty and one-sided inputs, case-distinct IDs in
one shard, and u64 boundaries. Strict release Clippy and formatting checks passed.

## GPU parallelism

The RTX 5070 implementation was already parallel. Its DirectX 12 compute shaders
use workgroups of 256 invocations across the GPU. A million records span thousands
of workgroups. This is not one CPU thread mapped to one GPU core, and occupancy or
the number of active hardware units was not measured.

The GPU path's host-side string-ID preparation and result creation remain
single-threaded. Improving those stages is a separate possible optimization; this
run keeps that path unchanged to preserve its value as a baseline.

CPU worker counts are software threads, not pinned physical cores. The i7-14650HX
has 16 physical cores and 24 logical processors; Windows chooses where threads run.
The sharded one-worker control reveals the extra routing/merging overhead. It should
not replace the simpler direct join for small or single-worker workloads.

## Applying the result

The candidate rebuilds all state and starts/joins scoped threads for every batch.
For API integration, use bounded shared CPU capacity and measure simultaneous
requests before choosing workers per request. Giving every request 16 workers could
oversubscribe the machine. The measured speedup does not establish sustained service
capacity, lower electricity cost or performance on other ID/data distributions.

Reproduce from the repository root:

```powershell
.\scripts\run-gpu-bulk.ps1 -Multicore -ReportName multicore-my-run.json
```

- [First run](multicore-million.json)
- [First-run provenance](multicore-million.json.provenance.json)
- [Confirmation](multicore-million-confirmation.json)
- [Confirmation provenance](multicore-million-confirmation.json.provenance.json)
- [Implementation](../../perf/gpu/src/comparison/multicore.rs)
