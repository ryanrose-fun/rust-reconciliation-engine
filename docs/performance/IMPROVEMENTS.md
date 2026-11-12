# Reliability and performance improvements: 10 November 2026

All three requested work items are implemented: bounded readable overload responses, HTTP/JSON profiling, and reduced hashing/allocation. Banking certification or production approval is not implied; see ../ENGINEERING-READINESS.md.

## What changed

- Early 429/503 rejections drain and discard the request body before responding. Draining has byte, deadline and concurrency limits and does not parse or accumulate the body. Retry-After: 1 and x-request-id are returned. Out-of-budget senders may still be disconnected intentionally.
- JSON decoding, reconciliation and JSON encoding now share a bounded blocking worker slot, keeping CPU work off asynchronous I/O threads. HTTP timeouts do not release slots while blocking work continues.
- Successful responses expose Server-Timing. Structured logs record body, queue, decode, reconciliation and encoding durations without payment data or credentials.
- Log records are encoded before writing to stderr. Directly displaying a JSON value generated many small writes and contention; the added profiling fields made this visible.
- JSON decoding uses serde_json directly. Rich error-path tracking was discarded by our public error messages, so retaining it added cost without useful output. Malformed JSON remains 400; schema/data failures remain 422; unknown/duplicate fields and trailing data remain rejected.
- Accepted sockets enable TCP_NODELAY via Axum's listener hook.
- Duplicate checking builds reusable indexes. Each internal ID gets one provider lookup, and provider-only membership uses position flags. Input ordering and error precedence remain unchanged.
- Production reconciliation writes final report entries directly into one preallocated vector. It no longer constructs an intermediate vector of reconciliation entries.
- Source fingerprints stream the same normalized JSON bytes through an 8 KiB buffer into SHA-256 rather than allocating a complete JSON copy of each source.

## Engine results

100,000 payments per source; means of 15 measurements, same benchmark boundaries and parity checks. The sorted-merge and SQLite comparisons run interleaved with the engine.

| Scenario | Boundary | Before ms | After ms | Change in time |
|---|---|---:|---:|---:|
| matched | typed_to_report_and_drop | 69.00 | 33.86 | -50.9% |
| matched | json_to_json_and_drop | 132.19 | 89.68 | -32.2% |
| mixed | typed_to_report_and_drop | 80.01 | 36.87 | -53.9% |
| mixed | json_to_json_and_drop | 133.88 | 96.34 | -28.0% |
| long_ids | typed_to_report_and_drop | 234.31 | 133.50 | -43.0% |
| long_ids | json_to_json_and_drop | 315.72 | 333.51 | 5.6% |

The matched typed workload improved from 69.00 to 33.86 ms, about 51% less time. Including JSON improved from 132.19 to 89.68 ms, about 32% less time. The new sorted-merge baseline averaged 56.54 ms at the matched typed boundary, versus 33.86 ms for the optimized engine.

Long-ID JSON-to-JSON time did not improve against the historical run. The other comparison implementations also slowed considerably in that phase. Preserve this result rather than claiming every workload improved: hardware/runtime variance and allocation behaviour need controlled follow-up. Whole-process benchmark working set includes all fixtures and implementations and cannot establish per-engine memory savings.

## Full API: final matrix and earlier run (provenance corrected)

Correction: the runner previously ignored ApiBinary and launched its default API executable. The file named http-before-recheck.json therefore does not establish an old-binary baseline. The table below is an earlier-run comparison only, not a controlled old/new binary comparison. The override is now fixed. These are 10-second local runs, not a capacity guarantee.

| Case | Payments/source | Clients | Earlier run success/s | Final success/s | Earlier run p95 ms | Final p95 ms | Final client statuses |
|---:|---:|---:|---:|---:|---:|---:|---|
| 7 | 1000 | 8 | 111.4 | 264.6 | 111.69 | 43.94 | 200:2651 |
| 11 | 5000 | 8 | 115.4 | 128.6 | 86.36 | 63.93 | 200:1293 |
| 12 | 5000 | 16 | 122.4 | 162.8 | 178.20 | 97.63 | 200:1646, 503:93 |
| 13 | 1000 | 8 | 96.4 | 100.0 | 100.19 | 27.54 | 200:1000, 429:977 |

Case 13 schedules 200 requests/second against the default 100/second rate cap. Other cases above use the capacity rate cap of 10,000/second. Server worker concurrency stays 8. A 503 is an intentional capacity rejection, not successful throughput.

Across the final 14 cases:
- 52,610 successful reports were validated in full.
- No incorrect successful reports and no client transport failures.
- Every server log counter matched client submissions plus warmups.
- The default-limit test delivered 1,000 successes and 977 readable 429 responses; 23 scheduled arrivals were dropped locally by the bounded generator.
- The 5,000/source, 16-client case delivered 1,646 successes and 93 readable 503 responses, with no resets.
- The 250 requests/second scheduled test achieved 243.4 successful requests/second but skipped 62 arrivals locally. It did not sustain the complete offered workload.

Historical API runs varied substantially. For example, the original 1,000/source, eight-client result was 262.7/s, the earlier run was 111.4/s (not a verified old-binary recheck), and the final result was 264.6/s. Do not claim a universal API throughput multiplier. The core comparison, readable rejections, complete report validation and measured stage timings are stronger evidence than selecting one favourable API run.

## What profiling showed

With fragmented logging, the 1,000/source, eight-client instrumented run averaged around 76 ms to receive a response while middleware work averaged around 3.2 ms. Encoding each log line first improved the targeted run from about 101 to 397 successes/second. This points to logging overhead; workstation variation prevents attributing every millisecond to one cause.

In the 5,000/source, eight-client case, direct decoding reduced mean decode time from about 6.21 ms in the log-fix check to 3.09 ms in the final matrix. Other stages varied between runs, so this is not a controlled CPU microbenchmark.

Final stage means for 5,000/source, eight clients:

| Stage | Mean ms |
|---|---:|
| body_ms | 2.084 |
| queue_ms | 0.031 |
| decode_ms | 3.094 |
| reconcile_ms | 2.645 |
| encode_ms | 2.907 |

The client still measures response delivery and scheduling costs outside those stages. At 5,000/source and eight clients, p95 receipt time was 63.93 ms, server sampled working set peaked around 38.9 MiB, and client working set around 68.1 MiB. The load generator validates large reports and shares the host; it can limit measured throughput.

## Verification

- 56 root-project tests passed: 24 library unit tests, 22 API/config/network tests and 10 integration tests.
- 5 benchmark tests passed, including independent output/fingerprint parity across 100 seeded scenarios and unordered long IDs.
- Root and benchmark strict Clippy checks passed; optimized binaries built.
- New real-network regression tests cover 900 KB uploads with Content-Length/chunked framing, complete 429/503 JSON, retry headers, keep-alive after rejection, oversized/stalled drains and bounded drain slots.
- JSON compatibility tests cover trailing content, invalid UTF-8, unknown/duplicate fields, incorrect array types, application/*+json and timing headers.
- Hosted CI and remote multi-host/long-duration resilience tests were not run.

## Rerun

Stop an existing development API with Ctrl+C before rebuilding its executable.

    cargo fmt
    cargo test
    cargo run --release --bin api

In another PowerShell window:

    .\examples\test-api.ps1

The isolated benchmark runners start/stop their own servers:

    .\scripts\compare-performance.ps1
    .\scripts\load-test.ps1 -Seconds 30 -ReportName http-rerun.json

Results and experiments retained:
- comparison-before.json / comparison.json: engine before/after.
- http-load.json: original historical API matrix.
- http-before-recheck.json: earlier run with unverified binary selection; not an old-binary baseline.
- http-instrumented-before-log-fix.json: profiling exposed fragmented logging overhead.
- http-log-fix-check.json: targeted logging correction measurements.
- http-after.json: final 14-case matrix.
- build-provenance.json: hashes of relevant source and measured API binaries.

The production contract is described in ../API.md. Bank-facing release requirements and remaining operational/security work are explicit in ../ENGINEERING-READINESS.md. The socket hook follows [Axum's official listener API](https://docs.rs/axum/latest/axum/serve/trait.ListenerExt.html).