# Persistent event API load testing

Run from the project folder, with Windows PowerShell 5.1 or newer:

~~~powershell
.\scripts\load-test-events.ps1 -Seconds 5 -ReportName event-load-quick.json
~~~

This builds isolated release binaries, then runs five cases: concurrency 1, 4, 8 and 16 with a 10,000 requests/second limit, followed by concurrency 8 with the default 100 requests/second limit. The server's active-request limit remains 8. Five seconds means five seconds **per case**, plus startup, finishing admitted operations, auditing and restarting. Compilation adds time on the first run.

For a longer local baseline:

~~~powershell
.\scripts\load-test-events.ps1 -Seconds 15 -ReportName event-load-baseline.json
~~~

For a focused repeat:

~~~powershell
.\scripts\load-test-events.ps1 -Seconds 30 -CaseIndex 1,3 -ReportName event-load-repeat.json
~~~

Use -SkipBuild only when you intentionally want to reuse the existing binaries. The report records their SHA-256 hashes.

## Isolation

The runner starts its own hidden API processes on temporary loopback ports. Each case uses a fresh database under target/perf/events, a random bearer key, and a 60-second waiting deadline. It restores shell environment variables afterwards and never stops an existing API server.

Databases and raw logs are retained under target/perf/events for inspection. JSON summaries go to docs/performance. Repeated runs consume disk space. No real payment data is used.

## Workload

Each worker repeatedly completes the following lifecycle for a unique payment:

1. Internal version 1: Completed.
2. Provider version 1: Pending.
3. Provider version 2: Completed.
4. Exact retry of the original Internal event.
5. Stale Provider version 1: Failed, with a new event ID.
6. Read the final status, checking both source values and versions.

Each completed payment therefore produces six successful HTTP responses, four unique committed event-log rows, three applied source updates, one retained stale event, and one duplicate receipt. A retry is not counted as another unique commit.

The client checks every successful event's identity, outcome, duplicate flag and comparison status. It checks the complete latest payment values on the final GET. All 429/503 responses must be readable JSON with Retry-After; those requests are retried using their unchanged event ID and body, with a bounded retry budget. Unexpected responses, invalid results, exhausted retries and transport failures mark their case as failed. Failed cases are retained in the report, remaining cases continue, and the script exits unsuccessfully if any validation failed.

After the timed phase the runner forcibly stops its own server, checks SQLite integrity and row counts, and examines every source's stored version and payment values. It then starts a new server on the same database and checks a completed payment through HTTP. These checks occur outside the measured throughput interval.

## Reading the report

- unique_committed_events_per_second: verified new event-log records per elapsed second, including drain time. Includes retained stale updates; excludes duplicate receipts.
- applied_updates_per_second: new source-state updates per second; excludes stale events and duplicates.
- successful_post_attempt_latency: time from sending an accepted POST to receiving its full response body. Does not include time spent on previous rejected attempts.
- successful_post_latency_by_outcome: separates Applied, Stale and Duplicate receipts.
- logical_post_latency_including_retries: time through all attempts and retry waits for one logical POST. This is the more useful client-facing measure under contention.
- p95: 95% of measured operations completed within this time; p99 describes the slower tail.
- rejection_codes: distinguishes storage_busy from API worker/request limits and rate limits.
- drain_seconds: time spent completing admitted lifecycles after the admission window ended.
- storage: initial, sampled peak, and final main-database/WAL/shared-memory file sizes. WAL is a reusable journal, so these are allocated file sizes, not physical disk bytes written or exact bytes per event.
- resources: server CPU time and sampled working-set memory; not per-request allocation counts.
- database_audit and restart_lookup_passed: correctness and recovery results.

Server and client run on the same Windows machine over loopback, with a new database per case, normal server logging, and no event warmup. This is a closed-loop mixed-workload capacity test: workers wait for replies and backoff. It is **not** a fixed-arrival-rate test, an internet latency estimate, an hours-long soak, a high-availability test, or a banking certification. Run repeated trials and longer tests on the intended deployment hardware before setting service targets.

The default-rate case is intentionally limited and should not be interpreted as the engine's unrestricted storage capacity. Existing snapshot benchmarks do not measure durable event commits and must not be compared directly to these figures.

## Verification

~~~powershell
cargo test --locked --manifest-path perf/Cargo.toml
cargo clippy --locked --manifest-path perf/Cargo.toml --all-targets -- -D warnings
~~~

The harness's tests deliberately alter counts, stored versions and payment values to verify that its checks reject incorrect results.

The queued API also reports transaction-stage statistics by outcome; see [EVENT-QUEUE.md](EVENT-QUEUE.md). The queue-capacity setting is pinned to 32 by this harness. Global request concurrency stays at 8 for comparability.

If transport errors interrupt a lifecycle, the final-state audit's complete-lifecycle expectations may fail even when SQLite integrity is OK. An update can commit before its HTTP response is lost. Do not interpret an incomplete lifecycle or an extra committed row as proof of database corruption, and do not claim no data loss from row counts alone. The harness intentionally retains these failures rather than increasing timeouts or hiding transport errors.


### Admission fairness and harness deadlines

Event requests now wait in bounded FIFO order for handler capacity rather than racing an immediate try_acquire on every retry. The same HTTP deadline covers admission plus processing; queue overflow still returns readable 503 JSON with Retry-After. The benchmark still treats transport errors, retry exhaustion, and incomplete lifecycle audits as failures.

The PowerShell runner has an independent wall-clock limit of admission duration + 120 seconds for each load-client process, plus a 10-second restart lookup timeout. Exceeding either fails the run and retains raw evidence. This does not lengthen the client's 5-second HTTP timeout, its 10-second retry budget, or the server's 10-second request deadline.