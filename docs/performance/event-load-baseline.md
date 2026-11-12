# Durable event baseline — 10 November 2026

Raw observations: [event-load-baseline.json](event-load-baseline.json).
Initial five-second smoke run: [event-load-quick.json](event-load-quick.json).
Method and commands: [event benchmark guide](../EVENT-PERFORMANCE.md).

The baseline admitted lifecycles for 15 seconds per case, then finished admitted work. Rates include that drain time. API and client were optimized release builds on the same Windows machine. Each case used a fresh SQLite database with WAL and synchronous=FULL. This is a short local baseline, not a sustained production capacity guarantee.

| Profile | Concurrent workers | Unique committed events/s | Applied state updates/s | Successful POST p95 | POST p99 including retries | Rejections |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Capacity | 1 | 62.1 | 46.6 | 20.1 ms | 36.2 ms | None |
| Capacity | 4 | 63.2 | 47.4 | 38.8 ms | 1,042 ms | 44 storage_busy |
| Capacity | 8 | 84.2 | 63.1 | 33.2 ms | 2,027 ms | 104 storage_busy |
| Capacity | 16 | 101.8 | 76.3 | 15.6 ms | 2,036 ms | 222 storage_busy |
| Default 100 req/s limit | 8 | 49.3 | 37.0 | 14.7 ms | 2,048 ms | 92 storage_busy, 20 rate_limited |

Unique committed events include retained stale events, but exclude duplicate retries. Applied state updates exclude both stale events and duplicates. Workers perform a mixture of event posts and status reads, so these are not pure write-only benchmarks.

## Correctness and recovery

All five cases completed successfully:
- 1,478 completed payment lifecycles.
- 5,912 unique event-log rows, including 1,478 stale events.
- 1,478 duplicate receipts without duplicate event-log rows.
- Every final source state checked against expected amounts, currency, status and version.
- SQLite integrity checks passed.
- Database counts agreed with acknowledged lifecycle counts after forced server shutdown.
- Each restarted API successfully read a completed payment.
- No transport failures, malformed error responses, exhausted retries or incorrect successful responses were reported.

The 462 storage-busy responses and 20 rate-limit responses were readable and retried; they were not hidden from the measurements.

## Storage

Final main-database sizes were approximately 0.38–0.81 MiB per case, with total database/WAL/shared-memory allocation approximately 4.35–4.79 MiB. The WAL journal accounts for much of the difference and is reused. These values must not be treated as measured physical write amplification or a fixed storage cost per payment.

The exact paths and initial/peak/final sizes are recorded in the raw report; synthetic databases and logs remain under target/perf/events.

## Interpretation and next work

The current single-connection try-lock design rejects simultaneous storage access rather than queueing it. Storage-busy responses in the report directly confirm that contention. Retry-After waiting contributes about one or more seconds to affected client operations. Higher throughput at concurrency 16 therefore comes with much worse tail latency.

Successful POST timings alone hide this delay. At concurrency 16, successful-attempt p95 was 15.6 ms, while p95 including retries was 1,020 ms.

The short smoke run and longer baseline varied substantially, so do not infer a smooth scaling curve or advertise 102 events/s as a guaranteed limit. No stage-level evidence yet isolates disk sync, WAL checkpoint, Windows scheduling, JSON or logging as the dominant cost of an uncontended request.

Recommended next changes, to implement and measure separately:
1. Instrument storage lock/admission, transaction/commit, and serialization durations.
2. Evaluate a bounded single-writer queue so short bursts wait briefly rather than immediately triggering a one-second client retry. Keep explicit queue limits, cancellation/timeout semantics and overload responses.
3. Evaluate small bounded group commits only after defining acknowledgement semantics: acknowledge each event after its containing transaction commits, preserve idempotency/version checks, and measure added batching delay.
4. Repeat the same benchmark and add a fixed-arrival-rate workload plus longer soak tests before setting latency and capacity targets.

Do not improve benchmark numbers by silently lowering durability or dropping conflict checks.
