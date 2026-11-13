# Persistence and backpressure decisions

This describes the existing implementation, not a production deployment. See [event contracts](EVENTS.md) and [queue behaviour](EVENT-QUEUE.md).

## Commit before acknowledgement

SQLite uses WAL and synchronous FULL. An event is acknowledged after its transaction commits. SQLite keeps local deployment small and inspectable, but remains a single-writer database. Multiple independent application hosts, high sustained write throughput or high availability call for a server database and revised coordination. WAL supplies neither unlimited writer concurrency nor a backup strategy.

## Bounded storage queue

The HTTP storage gate serialises one active operation and bounds waiting work (default 32). FIFO admission shares an overall deadline; full queues return 503 with retry guidance. This limits waiting tasks, not durable delivery. Clients need bounded backoff and stable event IDs. External SQLite writers can still cause storage_busy: an in-process gate cannot coordinate other processes. Slow storage creates head-of-line blocking.

## Timeout is not rollback

Waiting requests can leave the queue. Once synchronous storage starts, caller timeout cannot safely cancel the transaction. Permits remain held until work completes, preventing accidental concurrency above the limit. A client must resolve an uncertain result through idempotent retry, not assume timeout means nothing happened. Shutdown waits for active work; it cannot promise immediate termination of arbitrary I/O.

## Versions and idempotency solve different problems

An event ID identifies retries; monotonic payment/source versions prevent stale state replacing newer data. Neither authenticates upstream content. Origin verification and authorisation are separate. Snapshot reconciliation reports discrepancies; it does not initiate money movement.

## Measure the correct workload

Local batch benchmarks are not durable event throughput or a production SLA. Per-event commits, payload size, lock contention and queue delay alter performance. Batching can improve throughput but changes acknowledgement and failure boundaries. Measure those trade-offs before changing the persistence design.
