# Reproducible comparison and HTTP load benchmarks

Run from the repository root in Windows PowerShell:

    .\scripts\compare-performance.ps1
    .\scripts\load-test.ps1

Each script builds optimized executables before measuring. Run them sequentially on an otherwise idle machine, plugged into power. The HTTP script creates its own authenticated, loopback-only API on free ports, stops its servers afterward, and restores your environment variables. It does not use or stop your existing API on port 3000.

For longer measurements:

    .\scripts\compare-performance.ps1 -Samples 30
    .\scripts\load-test.ps1 -Seconds 30

Results overwrite docs/performance/comparison.json and docs/performance/http-load.json. Preserve those files under another name before comparing revisions. The raw observations, hardware information, limits and timing boundaries are included. Build dependencies are isolated in perf/Cargo.toml and perf/Cargo.lock; these tools are not production API dependencies.

## What is compared

At 1,000, 10,000 and 100,000 payments **per source**:

1. Current engine: hash-based reconciliation.
2. Independent Rust sorted-merge implementation.
3. SQLite in-memory indexed ID join, with Rust validation and report assembly.

All three reject invalid/duplicate IDs, preserve output order, classify the same mismatches, retain the same evidence, generate source fingerprints, and produce equivalent report metadata. Report ID and timestamp are the only fields excluded from equality checks. SQLite includes opening the database, schema/index creation, loading IDs and ordinal positions, joining and closing the connection. It retains the original typed payment data in Rust. It is not a durable database benchmark or a claim about an optimized, preloaded database service.

Scenarios: all matched; mixed amount/status/currency mismatches and missing payments; 240+ character IDs; duplicate rejection. Provider order is deterministically shuffled; internal IDs are ordered. Duplicate rejection is reported separately from completed reconciliation.

Two boundaries:
- **typed_to_report_and_drop**: prebuilt typed inputs; validation, matching, complete report, fingerprints and report destruction.
- **json_to_json_and_drop**: also decode the input JSON, encode the report JSON and destroy temporary allocations. No HTTP or disk I/O.

There are two warmups and 15 measured samples by default, rotating implementation order. Fixture construction and parity validation are outside the timer. All samples are retained. Percentiles use nearest rank; 15 samples are useful for initial comparisons, not reliable p99 estimates. Memory for this suite is whole-process working set, not algorithm-specific allocation.

The old 57 ms number used another fixture and timing boundary. It should not be treated as directly comparable with this suite or as a transactions-per-second claim.

## Full HTTP load test

Batch sizes: 100, 1,000 and 5,000 payments per source. Client concurrency: 1, 4, 8 and 16. Each case gets a fresh server and 3 warmups, then a 1.1-second rate-window reset. Default measurement duration: 10 seconds.

The capacity profile raises only the request rate limit from 100 to 10,000 requests/second. Server concurrency remains 8, body limit 1 MiB, input limit 5,000/source, timeout 10 seconds. Auth and request logging remain enabled. Two scheduled-arrival cases test 200 requests/second with defaults and 250 requests/second with the capacity profile.

- **Closed loop**: each client waits for its response and validation before sending another request.
- **Scheduled arrivals**: requests are scheduled independently of response time. A bounded client concurrency limit rejects excess work locally; these are counted as client admission drops.
- HTTP latency includes sending the request and receiving the complete body.
- Scheduled-arrival latency also includes generator scheduling delay.
- Every HTTP 200 body is fully compared with the expected report, excluding only random ID/time, after the HTTP timer stops.
- Throughput counts only correct HTTP 200 responses. HTTP statuses, transport failures and skipped arrivals are separate.
- Input records/second counts both source arrays. These are reconciliation records, not executed financial transfers.
- Requests use prebuilt JSON, HTTP/1.1 pooled connections, no proxy, no TLS, no retries.
- CPU and working set samples span the client lifetime, including warmups/cooldown; exact scope is in resource_observation. Do not divide that CPU by only the shorter measurement window.

Client validation consumes CPU, and both processes share this machine. Large JSON reports can make the load generator itself a bottleneck. These results are achieved local throughput, not a proven maximum server capacity. Use a separate load-generator host before making deployment capacity claims. Short-run p99 values and runs below 1,000 successes are exploratory.

## Interpretation

Compare the same fixture, boundary, compiler, hardware and limits. Inspect successful throughput together with p95 latency and rejection counts. Fast 429/503 responses are protection working, not useful reconciliation throughput. Increasing concurrency is only helpful if successful throughput improves without unacceptable latency.

Use the typed-vs-JSON comparison to identify serialization overhead, then profile allocation and matching before changing production code. Longer IDs and mixed inputs reveal costs that an all-matched toy fixture hides. Persistent streaming state, event arrival delay, out-of-order events and replay are separate future benchmarks; the current API reconciles snapshots.

## Verify the tools

    cargo fmt --manifest-path perf/Cargo.toml --check
    cargo test --locked --manifest-path perf/Cargo.toml --target-dir target/perf
    cargo clippy --locked --manifest-path perf/Cargo.toml --target-dir target/perf --all-targets -- -D warnings

Parity tests cover all scenarios, empty input, blank and duplicate IDs, Unicode/case distinctions and u64::MAX amounts. API load tests fail if warmup fails, any successful body is wrong, or request counters are inconsistent. Raw server logs remain in target/perf/load; they contain request metadata rather than payment values or the API key.
A targeted rerun can preserve the main results file:

    .\scripts\load-test.ps1 -Seconds 30 -CaseIndex 12,13 -ReportName overload-recheck.json

Cases 1-4 use 100 payments/source, 5-8 use 1,000, and 9-12 use 5,000; concurrency within each group is 1, 4, 8, 16. Case 13 uses the default rate limit with 200 scheduled requests/second. Case 14 uses the capacity profile with 250 scheduled requests/second.

Server log counters are included separately, including the three warmups. client_count_consistent flags mismatches with client submissions; a mismatch needs investigation and must not be silently discarded. Transport errors can occur before a request reaches the server or before a rejection is received by the client. Detailed connection causes are retained without request bodies or credentials. Each run uses unique log filenames.
The latest fixed-API results, processing-stage breakdown and before/after comparison are in [docs/performance/IMPROVEMENTS.md](../docs/performance/IMPROVEMENTS.md). Server stage profiles are included in the HTTP report. All receive-side and overload-drain limits remain enabled during measurements.

## Windows PowerShell 5.1 runner correction

The runner now retains each native process handle before polling/Refresh, so a successful child process is not mistaken for failure when ExitCode becomes unavailable. Genuine failures include exit status and a log path even when stderr is empty. Cancellation also stops the runner-owned load client. A regression check covers successful, nonzero and unavailable exit statuses:

    .\scripts\test-process-runner.ps1

The fixed runner completed all 14 five-second cases under Windows PowerShell 5.1, producing 24,491 validated successful reports and zero transport errors in docs/performance/quick-test.json.

The ApiBinary override is now honoured. Earlier http-before-recheck.json results had unverified binary selection and are explicitly marked unsuitable as an old-binary baseline.Persistent event API testing: see [the event load guide](../docs/EVENT-PERFORMANCE.md).
