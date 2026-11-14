# Equal-work CPU/GPU comparison — 14 November 2026

Follow-up: [multicore CPU comparison](multicore-comparison.md) uses the identical
fixture and outputs; 16 workers reduced the complete batch to approximately 129 ms.
The results below remain the original single-threaded comparison.

**The direct CPU path had lower median time in both runs.** For a fresh million-
record snapshot in each source, GPU acceleration did not improve this complete
in-memory comparison path.

| Implementation | Run 1 median | Confirmation median |
|---|---:|---:|
| CPU direct string-ID join | **304.50 ms** | **298.32 ms** |
| CPU packed-data join | 312.29 ms | 299.97 ms |
| GPU packed-data join, including host preparation and transfers | 328.09 ms | 324.39 ms |

The direct CPU path used approximately **7.2% and 8.0% less time**, respectively,
than the GPU-assisted path. Equivalently the GPU path took approximately 7.7% and
8.7% longer. These are ratios of within-run medians, not a guarantee for other data.

## What makes this comparable

- Same immutable typed input arrays for every path: **1,000,000 payments per source**,
  2,000,000 input records, 1,010,000 distinct IDs. Both arrays are independently
  shuffled with reproducible seeds. Missing IDs, zero and large u64 amounts, and all
  amount/currency/status mismatch combinations occur in the fixture.
- Same outputs: fully owned `(String payment_id, u32 result_code)` rows in the same
  order, plus duplicate counters. Multiple disagreements remain one payment result.
  Duplicate IDs invalidate the batch for all paths.
- Each timed call starts from the same borrowed typed string-ID snapshots. It
  includes fresh indexing/preparation, scratch allocation and destruction, duplicate
  checks, comparisons, and allocating/copying every result ID. GPU calls also create
  fresh buffers, upload the records, execute both kernels, download results and wait
  for completion. No prepared data or GPU buffers are reused across calls.
- CPU direct avoids packing into a GPU representation. CPU packed and GPU share
  exactly the same preparation routine, helping distinguish algorithm choice from
  hardware choice. None is the full production report API.
- Fixture creation, independent correctness checks and destruction of the returned
  output are excluded equally. GPU device/shader initialization is separate; it is
  a persistent-context comparison, not a fresh-process comparison.
- Two warmups per path, followed by 24 trials per path in each run. All six execution
  orders occur equally. Medians are calculated from actual total-call durations,
  **not sums of separately measured stages**.
- An independent BTreeMap-based oracle verifies every output, including IDs and
  ordering. **144 measured outputs passed** across both runs. Additional CPU unit
  and GPU fixture checks cover duplicate source IDs and high-word amount changes.
  Release unit tests (2), strict release Clippy, and formatting checks passed.

## Startup and variation

GPU device/pipeline initialization took **2,397.06 ms** and **2,373.50 ms**, measured
once in each process. The first GPU batch then took 299.87 ms and 427.58 ms; adding
initialization gives roughly **2.70–2.80 seconds** before the first GPU result in
these runs (excluding fixture creation). A persistent service can amortize startup.
First-use direct CPU calls took 303.79 ms and 300.09 ms.

Warm ranges overlap:

| Implementation | Run 1 range | Confirmation range |
|---|---:|---:|
| CPU direct | 292.79–331.08 ms | 285.47–324.08 ms |
| CPU packed | 287.66–355.48 ms | 283.18–349.61 ms |
| GPU assisted | 275.20–354.78 ms | 280.44–346.45 ms |

Some individual GPU trials won. Both medians favored CPU. This is a local laptop
measurement with ordinary OS/background scheduling, randomized HashMap seeds and
shared power/thermal resources, not an isolated sustained-capacity experiment.
No cause is assigned to individual timing fluctuations without profiling.

Hardware: Intel Core i7-14650HX, RTX 5070 **Laptop GPU** (8 GB), DX12 driver 617.14.
CPU paths are **single-threaded**, not tuned multicore implementations. GPU work
also uses the CPU for preparation and output. No whole-system energy measurement
was made, so these results do not establish electricity savings.

## Why this differs from the earlier 5.40 ms GPU number

That number started with already prepared records and returned compact codes. This
comparison starts with string IDs, rebuilds preparation on every call, independently
shuffles both sources, creates fresh GPU buffers, produces owned string-ID results,
and includes scratch cleanup. The earlier figure remains a narrow GPU-stage result;
it is not a complete reconciliation latency.

Both sides here exclude JSON parsing/serialization, HTTP, source fingerprints,
full evidence reports, database commits, histories and recovery. Therefore neither
result can be substituted for the previous durable million-payment API benchmark.

## Decision

Keep CPU as the default for fresh string-ID bulk comparisons. There is no measured
latency benefit to justify moving this path to the GPU now. A persistent dictionary,
columnar inputs or incremental uploads could change the balance, but would need a
new equivalent comparison where the CPU gets the same reuse opportunities. A
multicore CPU implementation is another candidate, not measured here.

## Evidence and reproduction

- [Run 1 raw samples](cpu-gpu-million.json)
- [Run 1 provenance](cpu-gpu-million.json.provenance.json)
- [Confirmation raw samples](cpu-gpu-million-confirmation.json)
- [Confirmation provenance](cpu-gpu-million-confirmation.json.provenance.json)
- [Benchmark code](../../perf/gpu/src/comparison.rs)

From the repository root in PowerShell:

```powershell
.\scripts\run-gpu-bulk.ps1 -Compare -ReportName cpu-gpu-my-run.json
```

No API process is needed. Original GPU-only evidence and existing CPU/API benchmark
files have been retained separately. This task did not change production API logic.
