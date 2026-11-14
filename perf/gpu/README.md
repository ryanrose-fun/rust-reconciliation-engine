# GPU bulk reconciliation prototype

**Current GPU-focused development: resident incremental worker.**

```powershell
.\scripts\run-gpu-bulk.ps1 -Resident -ReportName gpu-resident-my-run.json
```

This keeps one million payments' source values and versions on the GPU, uploads
incremental updates, checks versions on the GPU and returns receipts plus compact
reconciliation changes. No CPU performance benchmark runs in this mode. See
[resident worker results and semantics](../../docs/performance/gpu-resident.md).
The older bulk/CPU comparison modes below remain available as historical experiments.

For the **multicore CPU comparison**, run:

```powershell
.\scripts\run-gpu-bulk.ps1 -Multicore
```

This adds a hash-partitioned CPU implementation at 1, 4, 8 and 16 workers to the
existing direct CPU, packed CPU and GPU paths. Equal IDs route to the same worker;
full string equality, not hash equality, determines identity. Worker-owned lookup
tables avoid shared-map locks. Original result order is restored by input position.
Duplicate counters are combined across workers; any duplicate rejects the entire
batch. The one-worker partitioned run measures the routing/merge overhead independently
of parallel speedup. Workers are OS threads, not pinned physical cores.

All per-call routing, allocations, thread creation/join, classification, order
restoration and owned result creation are included. No worker pool or retained ID
dictionary is hidden outside the timer. The GPU still uses a reused device context
and fresh per-batch buffers, with all transfers measured.

Each of seven paths has two warmups and fourteen measured trials, using forward and
reverse rotations so every path occupies each execution position twice. All outputs
are checked against the sorted-map oracle, including order and duplicate counters.
Input/result hashes allow comparison with earlier equal-work runs. Reports default
to `docs/performance/multicore-million.json`; use `-ReportName` to keep separate runs.

This is a bulk optimization candidate in the isolated performance crate, not a
change to the production API or durable event writer. The GPU was already parallel:
its compute shaders dispatch 256-invocation workgroups. Only its host-side input
preparation and output creation are single-threaded in this comparison.

For the **equal-work CPU/GPU comparison**, run:

```powershell
.\scripts\run-gpu-bulk.ps1 -Compare
```

This mode benchmarks two single-threaded CPU implementations and the RTX GPU path
on exactly the same million records per source. All three return owned payment-ID
strings and result codes in the same order. Timings include fresh per-batch ID
indexing/preparation, scratch allocation/destruction, duplicate detection, comparison
and output creation; GPU timings additionally include fresh buffers, upload and
readback. The device/pipeline context is reused, with initialization reported
separately. Typed input fixture creation, output destruction and correctness checking
are excluded equally. This is an in-memory reconciliation comparison, not the
durable HTTP engine or its full evidence report.

The direct CPU join avoids GPU-specific packing and a second output hash lookup.
The packed CPU path uses the same host preparation as the GPU followed by CPU
indexing/comparison. Both input arrays are independently shuffled with fixed seeds.
There are two warmups and 24 measured trials per path, rotating through all six
execution orders equally. Every output is checked against an independent sorted-map
oracle. CPU unit tests and GPU fixture checks cover duplicates and exact results.
Neither CPU path is a tuned multicore implementation; this is not a claim about
maximum CPU hardware throughput.

Results default to `docs/performance/cpu-gpu-million.json` with provenance beside it.
Use `-ReportName another-comparison.json` to preserve an earlier run.

## Original GPU-only mode

Standalone Rust/wgpu (DirectX 12) experiment. Does not change the API, SQLite writer,
durability, production reconciliation or existing CPU benchmarks. Requires an NVIDIA
RTX 5070 hardware adapter; fails instead of falling back to software.

From the repository root in PowerShell:

```powershell
.\scripts\run-gpu-bulk.ps1
```

This builds an optimized executable, runs GPU correctness checks, then processes
1,000,000 payments **per source** (2,000,000 input records). No CPU benchmark runs.
The independent CPU oracle only checks correctness, outside all measured timings.
The output includes raw samples, adapter details, result and shader hashes; a sibling
provenance file records executable/source/lockfile hashes and working-tree status.

## What runs where

1. Host creates synthetic string IDs (fixture generation excluded).
2. Host maps exact string IDs to shared dense integers and packs 64-bit amounts into
   two 32-bit words. This preparation is measured separately, including allocations.
   No ID hashes are used as identities, so hash collisions cannot merge payments.
3. GPU scatters each source into an ID index, independently of source order. Atomic
   exchanges detect duplicate IDs; any duplicate or out-of-range ID invalidates the
   entire batch. The prototype records error counts, not lists of duplicate IDs.
4. GPU compares each ID's amount, currency and status. Integer word comparisons retain
   all 64 amount bits. Each ID has one result even when several fields disagree.
5. Results and error counters return to the host. Every output in every trial is
   compared with the untimed oracle.

The provider input is reversed. One percent of provider IDs are provider-only,
leaving corresponding internal-only payments. Remaining records include matches
and all seven combinations of amount/currency/status discrepancies. Separate GPU
checks cover zero amounts, u64::MAX, high-word and low-word differences, absent
IDs, duplicates in each source, and invalid dense IDs.

## Timing boundaries

- Initialization includes adapter/device selection and shader/pipeline creation.
- Buffer allocation and the cold first batch are reported separately.
- 21 warm upload trials include input copies, submission, clearing indexes,
  scatter/comparison, readback, waiting, and copying results into a host Vec.
- Interleaved resident trials omit input upload but still rebuild indexes and return
  every result. They are **not pure GPU kernel timings**.
- Exact-output validation is outside the measured interval.
- Host preparation is a separate single sample, not a repeated distribution. Add
  it to upload timing when estimating a new string-ID batch; include allocation
  and initialization as appropriate for a one-shot invocation.

The workload is an in-memory bulk join and comparison, not financial transaction
execution. It excludes JSON parsing, HTTP, source fingerprints, full evidence report
construction, SQLite commits, event histories, retries, and recovery. Output is a
compact ID-indexed array, unlike the production engine's ordered detailed report.
Both source snapshots must be nonempty in this prototype; supported input count is
100 to 2,000,000 per source. Dense IDs require host memory and normalization work.

Do not divide the durable million-payment API result by this GPU timing and call it
a speedup. Existing CPU report benchmarks also include more work and use other
fixtures. No equivalent CPU benchmark is rerun at the user's request.

## Cost interpretation

Existing local hardware incurs no additional cloud rental. Electricity and hardware
depreciation are not zero, and GPU board power alone does not measure whole-system
energy. No CPU-versus-GPU electricity savings are established by this experiment.

For an eventual comparable hosted workload:

`cost per million = full host hourly price * seconds per million / 3600`

Include CPU host, GPU, storage, transfer, idle time and required durability. Compare
equal outputs and service guarantees. A GPU service must be faster by more than its
full-host price ratio to reduce compute cost at equal utilization. Short warm bulk
trials do not establish sustained API capacity or bank-scale reliability.
