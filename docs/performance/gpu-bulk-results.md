# RTX 5070 bulk prototype — 14 November 2026

Follow-up: [equal-work CPU/GPU comparison](cpu-gpu-comparison.md) now measures both
paths including preparation and owned output creation. CPU had lower median time
in both comparison runs. The figures below retain the earlier GPU-only boundaries.

GPU-only benchmark requested; existing CPU benchmarks were not rerun. Raw results:
[gpu-million.json](gpu-million.json), with source/executable/hardware provenance in
[gpu-million.json.provenance.json](gpu-million.json.provenance.json).

## Measured results

NVIDIA GeForce RTX 5070 **Laptop GPU**, 8 GB, DirectX 12, driver 617.14;
Intel Core i7-14650HX host. Optimized standalone Rust executable using wgpu 24.0.5.

One million records in each source: two million inputs, 1,010,000 distinct payment
IDs after including source-only records. Output: 555,000 matched and 455,000
discrepancies. Multiple mismatched fields still count as one discrepant payment.

| Stage | Time | Interpretation |
|---|---:|---|
| Device and shader initialization | 3,270.61 ms | Single cold observation, amortizable in a persistent worker |
| String-ID mapping and packing | 162.52 ms | Single host preparation observation, includes allocations |
| GPU buffer allocation | 0.12 ms | Single observation; backend may defer actual allocation |
| First uploaded batch | 5.67 ms | After initialization and preparation |
| Warm upload + GPU work + full compact readback | **5.40 ms median** | 21 trials; range 5.12–5.71 ms |
| GPU-resident inputs + work + full compact readback | **1.34 ms median** | 21 trials; range 1.16–1.67 ms; not pure kernel time |

Preparation plus warm uploaded batch is approximately **167.93 ms**. This is the
sum of a single preparation observation and the median GPU trial, not a separately
measured end-to-end latency distribution. A fresh one-shot process also pays roughly
3.27 seconds of initialization. Fixture generation and correctness checks are not
part of these performance intervals.

The upload contains 40 MB of packed records; the returned result buffer is about
4.04 MB. The output is compact result codes, not a complete JSON/evidence report.
Each trial rebuilds GPU indexes and compares shuffled sources by exact mapped ID.
All output codes in all 42 measured trials matched the independent, untimed host
oracle. GPU fixtures also passed all seven mismatch combinations, missing sources,
64-bit amount boundaries, duplicate detection and invalid dense-ID detection.
Formatting and strict release Clippy checks passed for this standalone crate.

## Existing CPU evidence, without rerunning it

- The completed durable API test processed one million payments / two million
  events in **1,648.38 seconds**, or **606.66 completed payments/s**. It included
  HTTP, durable SQLite writes, histories and subsequent recovery validation.
- The historical optimized bulk engine benchmark at **100,000 per source** averaged
  **33.86 ms** for matched typed inputs to full report and destruction; mixed
  results averaged **36.87 ms**. See [IMPROVEMENTS.md](IMPROVEMENTS.md).

Neither is the same workload or output boundary as this GPU prototype. Do not
claim a CPU/GPU speedup ratio, extrapolate those CPU numbers to a million, or claim
that a 5.40 ms GPU batch makes durable event processing that fast. The useful new
evidence is that large exact comparisons fit comfortably on the GPU, while host
normalization consumes about 97% of this preparation-plus-GPU estimate.

## What this means for cost and the next implementation

No cloud resources were rented. Electricity and hardware wear are not measured;
there is no measured CPU/GPU energy or monetary saving. GPU power alone would omit
host CPU, memory, storage and platform consumption. Pricing must use the complete
host and equivalent output/durability guarantees.

At an assumed fully utilized host price H per hour, the 167.93 ms preparation-plus-
GPU estimate corresponds to `H * 0.00004665` per million records per source for
this narrow operation. This is a capacity-model coefficient, not a bill estimate:
it excludes startup, parsing, report creation, persistence, idle time and sustained
load effects. The existing durable CPU result cannot supply a comparable cost.

The most promising next step is a persistent bulk worker that retains its GPU
context and an exact ID dictionary, uploads only changed records, and produces
production-compatible reports. Then measure that complete path. A columnar/numeric
customer input format could also reduce repeated string preparation. None of those
savings is implemented or established by this experiment.

Keep the durable CPU event engine for acknowledgements, ordering, retries, audit
history and recovery. This prototype is a candidate accelerator for large snapshot
reconciliation; it does not replace those responsibilities.

## Run it yourself

From the repository root:

```powershell
.\scripts\run-gpu-bulk.ps1
```

To preserve the original evidence when rerunning:

```powershell
.\scripts\run-gpu-bulk.ps1 -ReportName gpu-million-rerun.json
```

See [prototype documentation](../../perf/gpu/README.md) for timing boundaries,
input restrictions and algorithm details. No API server is needed.
