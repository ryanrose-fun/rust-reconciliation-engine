# Resident GPU reconciliation — 14 November 2026

Development now focuses on the GPU path. This implementation keeps reconciliation
state in GPU memory for the worker's lifetime, uploads only submitted updates and
downloads receipts plus compact result changes. No CPU performance benchmarks were
run for this work.

## What was built

The Rust host maintains an exact string-ID dictionary and assigns dense GPU slots.
It validates and coalesces the two source updates for a payment into one patch.
The GPU holds both sources' amounts, currencies, statuses and versions, plus the
current reconciliation code, for up to the configured resident capacity.

Each compute invocation owns one payment in a batch. It performs version checks,
applies accepted source changes, compares both sources and emits a receipt. When
the reconciliation code changes, an atomic counter reserves a compact change row.
No worker touches another invocation's payment state. Batches are serialized via
the Rust worker's mutable submission method; there are no overlapping submissions.

The host first reads receipts and the changed-row count, then reads **only that
many changed rows**. A retry with no changes needs no second readback. Full resident
payment arrays stay on the card between submissions. All buffers are allocated once.

This shader uses GPU parallelism with 256-invocation workgroups. The host still
performs I/O coordination, validation and ID lookup; there is no CPU reconciliation
fallback or new CPU optimization in this mode.

## Results on the RTX 5070 Laptop GPU

One million resident payments, with two million source values, initialized as
matched. Provider updates alternate between a pending/amount disagreement and
completed/matching evidence. Each update batch is immediately retried exactly.

| Updates per batch | Update median | Exact-retry median |
|---|---:|---:|
| 1 | 0.185 ms | 0.127 ms |
| 100 | 0.190 ms | 0.128 ms |
| 1,000 | 0.378 ms | 0.271 ms |
| 10,000 | 1.895 ms | 1.453 ms |
| 65,536 | 19.056 ms | 16.856 ms |

Each row has 20 measured update submissions and 20 measured retries after two
update/retry warmup rounds: **200 measured submissions** in total. Every receipt's
ID, outcome and result is checked. Every changed ID and before/after code is checked;
exact retries must produce zero changed rows. The final GPU audit checks all one
million resident payments, including the untouched remainder, by submitting original
source versions and asserting duplicate/stale outcomes and unchanged matched results.

Timings include exact ID lookup, validation/coalescing, packing, upload, GPU version
checks/state updates/comparison/compaction, receipt/change readback, owned output ID
creation and per-call scratch cleanup. Test event construction, correctness checks
and result destruction are excluded. No HTTP, JSON or durable commits are measured.

Device/pipeline/state initialization took **2,425 ms**. Initial state loading took
**480 ms summed submit time**, or **681 ms wall time including fixture construction
and loading checks**. These are separate one-time observations, not included in the
warm incremental medians. The GPU allocates approximately 60 MB of application buffers
for this capacity and batch limit; driver overhead and host dictionary memory are
additional and were not measured.

This is **not a speedup comparison** with the earlier full-snapshot CPU/GPU runs:
it reuses state and touches only changed payments. No equivalent persistent CPU
workload was run, as requested. Nor are these short synchronous microbenchmarks a
sustained real-time service-capacity or multi-GPU result.

## Update semantics and checks

- Versions are exact, nonzero u64 integers, scoped to payment ID and source.
- A higher version is applied; a lower version is stale and does not mutate state.
- Equal version and identical payload is an exact retry; differing payload conflicts.
- Outcomes are per source: one source can apply while the other conflicts. This is
  not an atomic whole-batch or whole-payment transaction.
- At most one update per ID/source is allowed in a submission. Duplicate pairs,
  malformed fields, and batch/capacity overflow reject the submission before new
  IDs or GPU state are changed.
- Amounts and versions use two u32 words each; no floating-point money or truncated
  versions. Checks cover high-word version ordering and u64::MAX amounts/versions.
- Changes mean **reconciliation code changes**, not every change to evidence. A new
  version with the same outcome receives an Applied receipt but no change row.
- Missing sources are reported immediately; there is no waiting-period policy yet.

GPU checks passed for all mismatch combinations, one-sided records, exact retries,
version conflicts, stale updates, mixed apply/conflict outcomes, unchanged outcomes
at newer versions, duplicate pairs, invalid zero versions, capacity and batch limits,
and empty submissions. Strict release Clippy and formatting checks passed.

## Current integration boundary

This remains an isolated, single-GPU compute worker in the performance crate. It is
not wired into the production HTTP API. State persists **within a running process**,
not across restarts; a process exit/device loss requires replay/reload. Durable
logging, recovery, production event-ID deduplication, history, acknowledgement policy,
tenant isolation and multi-GPU routing remain outside this implementation. Its
version-plus-payload retry rule does not replace the production event-ID contract.

The next GPU development work can build a long-running worker interface, bounded
input queue and replay/bootstrap integration, then profile the GPU submission and
readback stages. Multi-card deployment remains a subsequent measurement, not an
assumed linear multiplier.

## Run and inspect

From the repository root in PowerShell:

```powershell
.\scripts\run-gpu-bulk.ps1 -Resident -ReportName gpu-resident-my-run.json
```

This runs GPU checks, loads one million payments, measures incremental batches and
audits final state. No API server or CPU performance benchmark is launched.

- [Audited results](gpu-resident-million-audited.json)
- [Executable/source/hardware provenance](gpu-resident-million-audited.json.provenance.json)
- [Initial GPU-only run](gpu-resident-million.json)
- [Rust worker](../../perf/gpu/src/resident.rs)
- [Compute shader](../../perf/gpu/src/resident.wgsl)
