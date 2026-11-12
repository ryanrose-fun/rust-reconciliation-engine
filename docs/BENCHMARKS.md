# Reproducible performance baseline

Run `./scripts/benchmark.ps1` on Windows to build an optimized binary and produce `docs/benchmark-results.json`. On other systems, run `cargo run --release --locked --bin benchmark` for timing results without the Windows process-memory wrapper.

The workload uses 1,000, 10,000 and 100,000 payments per source; each scenario has 3 warmups and 30 measured repetitions. Provider input is reversed. Scenarios are matching snapshots, mixed differences/missing payments, 240-character ID prefixes, and repeated IDs rejected by validation. The mixed workload changes amount, status, currency, both amount/status, and provider ID for five of every ten records.

Measured duration includes input validation, matching, report creation, UUID/timestamp generation, and normalized input SHA-256 fingerprints. It excludes input generation, JSON parsing, JSON output encoding, report destruction, HTTP, and disk. Duplicate results measure rejection, not completed reconciliation. Do not compare their throughput as though they were full reports.

The JSON records mean, p50 and p95 milliseconds, input records per second, sample counts, CPU, OS, Rust version and process working-set high-water mark. Memory is measured across the entire benchmark process and includes generated datasets and allocator retention; it is not per-request or isolated engine allocation. These are synthetic single-machine results, not an HTTP capacity guarantee or a claim of billions of transactions per second.

To compare changes, use the same machine, power settings, compiler, workload, sample counts, and idle conditions. Keep prior results with their engine versions. HTTP concurrency, response serialization, proxy overhead and persistence require separate workload-specific measurements before a company sets a capacity target.
## Measured baseline

Measured on Intel(R) Core(TM) i7-14650HX, rustc 1.99.1 (58cf8f9c3 2026-10-13). Release build; 100,000 payments per source.

| Scenario | Mean ms | p95 ms |
| --- | ---: | ---: |
| matched | 57.22 | 62.06 |
| mixed | 59.48 | 68.86 |
| long_ids | 353.83 | 642.56 |
| duplicates | 24.72 | 37.83 |

Whole benchmark process peak working set: 128.2 MiB. These figures cover all scenarios; they are not per-request memory measurements.
## Extended suite

The reproducible cross-implementation and full HTTP load suite is documented in [perf/README.md](../perf/README.md). Its timing boundaries differ from the original microbenchmark; do not directly compare headline timings.
