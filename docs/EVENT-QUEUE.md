# Bounded event storage queue and transaction timings

The HTTP event API now serializes access to its SQLite connection through a bounded FIFO gate. POST /v1/events and GET /v1/payments/{payment_id} share the gate, preventing status reads from racing a writer for the same connection.

There is one active storage operation and up to RECONCILE_EVENT_QUEUE_CAPACITY waiting operations (default 32, allowed 1–1024). Event requests first enter a bounded FIFO admission gate: at most concurrency + event queue capacity event requests are active or waiting (40 by default), with at most concurrency executing handlers (8 by default). Waiting for admission uses the same overall request deadline as handler execution. This prevents fast callers from continually reclaiming all execution slots while other clients exhaust their retry budgets. Snapshot requests retain immediate busy rejection. The storage gate then serializes SQLite access; worker limits and per-event commit semantics are unchanged.

A full queue returns HTTP 503 with error code event_queue_full and Retry-After. Existing busy responses still cover global request/worker limits. storage_busy can still occur if another process or direct library caller holds the database; the queue only serializes this server's event routes.

## Cancellation and durability

- Waiting requests remain subject to the HTTP deadline. Cancellation removes a waiting operation and releases its queue slot.
- A dispatched operation checks cancellation before starting storage work.
- After storage work starts, cancellation cannot safely interrupt an atomic transaction. The operation retains its queue, serial-access and worker permits until completion, even if its HTTP caller has timed out.
- Retry ambiguous writes using the identical event ID and content.
- Graceful shutdown waits for active storage work after HTTP shutdown.
- SQLite WAL and synchronous=FULL remain enabled. There is no batching or weakened acknowledgement rule: each new event is acknowledged only after its own transaction commits.
- The library's direct EventStore methods remain synchronous and can return Busy under concurrent use. The new queue belongs to the HTTP service.

Tests cover FIFO admission and storage order, bounded overflow, admission deadlines, cancellation while waiting, cancellation during active work, permit recovery after a worker panic, and concurrent exact retries.

## Timing measurements

Successful event and status responses include Server-Timing. Structured request logs include the same values under event_stages. No payment IDs or payloads are added to logs.

| Header metric | Meaning |
| --- | --- |
| event_body | Collecting the bounded event request body (zero for GET) |
| event_resume | Delay from blocking-operation completion until its async request resumes |
| event_queue | Waiting for serial access after queue admission |
| event_dispatch | Waiting for a blocking worker after serial access is acquired |
| event_service | Complete blocking operation, including JSON event decoding where applicable |
| event_prepare | Validation, canonical JSON preparation and obtaining the connection guard |
| event_begin | SQLite transaction begin, including any SQLite lock wait |
| event_work | SQL checks/updates, reading the current state and decoding stored values |
| event_commit | SQLite commit call, including synchronization/checkpoint work it performs |
| event_encode | Encoding the successful JSON response |

All values are milliseconds measured with a monotonic clock. event_service contains the four storage measurements; do not add it to those values. Queue and dispatch are separate from service. Body upload and completion-resume delay are measured separately from storage. Middleware, logging, network transfer and client retry waits are not included in the storage timings. Only successful operations carry these timing measurements.

Commit duration is not a direct measurement of physical disk latency: it includes everything performed inside SQLite's commit call. Likewise, unexplained end-to-end time must not automatically be attributed to the database.

## Repeating the comparison

The updated load client records stage statistics by outcome under event_stage_latency, including Applied, Stale, Duplicate and Read. The old binary has no stage headers, so its stage map is empty.

Normal run:

~~~powershell
.\scripts\load-test-events.ps1 -Seconds 15 -ReportName event-queue-after.json
~~~

The preserved old binary can be measured locally with the same client:

~~~powershell
.\scripts\load-test-events.ps1 -Seconds 15 -ReportName event-queue-before.json -ApiBinary target/perf-api/baselines/event-api-before-queue.exe -SkipBuild
~~~

The old binary is a local benchmark artifact, not included in source archives. Reports include binary hashes. Use the same workload, duration, rate limit and request concurrency for comparisons. Changes to the queue, instrumentation and their overhead are included in the new binary's results.

These remain short same-machine closed-loop tests. Repeat and add longer fixed-arrival-rate and soak testing before setting deployment service targets.
Measured results: [before/after comparison](performance/event-queue-comparison.md).
