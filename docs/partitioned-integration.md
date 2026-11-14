# Checkout/provider integration with partitioned group commit

The API accepts continuous payment updates from two independently credentialed sources. Each payment's updates go to one fixed SQLite partition, where small groups share a durable commit. Disagreements produce history and signed webhook notifications; corrections preserve that history and resolve the disagreement.

## Run the demonstration when ready

From the repository root in Windows PowerShell:

```powershell
.\examples\run-partitioned-integration.ps1 -Partitions 4
```

The launcher builds the API and example, creates an isolated directory under `%LOCALAPPDATA%\Reconcile\integration-runs` (outside OneDrive), generates checkout/provider/auditor credentials, starts a loopback API and runs the simulation. It stops its own API process afterwards and restores the environment variables it changed. It does not change your existing tenant databases. `tenants.json` contains secrets: keep the run directory private.

The demonstration creates 32 payments and uses eight concurrent producers. It sends:

1. Checkout: Completed, GBP 10000 minor units.
2. After a delay, provider: Pending, GBP 9500. This produces AmountAndStatusMismatch.
3. Provider version 3: Completed, GBP 10000. This resolves the disagreement.
4. The original checkout events again, using identical IDs and bodies. Receipts identify duplicates.
5. Late provider version 2 events. These are stale and do not replace version 3.

Event IDs and payloads are saved to `event-plan.json` before transmission. The Rust client makes bounded retries with the same event ID/body and supplies the routing header. A timeout is an uncertain outcome: retry the same event instead of generating a replacement identity.

The example starts a loopback alert receiver and one existing `WebhookWorker` per partition. Those workers use a separate SQLite connection to each partition and deliver the durable notification jobs through HTTP. The receiver verifies HMAC signatures, deduplicates delivery IDs for the process lifetime and writes/syncs `alerts.jsonl` before replying. The example is intentionally a fresh run each time, not a persistent customer receiver service. A production receiver needs persistent deduplication and restart recovery.

Files retained:

- `event-plan.json`: original producer events, useful for retry inspection.
- `receipts.json`: API results, outcomes and duplicate flags.
- `alerts.jsonl`: signed notifications accepted by the simulated receiver.
- `sample-history.json`: one payment's evidence and discrepancy timeline.
- `partition-N-metrics.txt`: each partition's metrics snapshot.
- `partitions/partition-NN.sqlite3`: durable reconciliation state and history.
- API logs and the private tenant configuration.

Webhook delivery is at least once. Receivers must use the notification ID for deduplication and revision for ordering; delivery order is not guaranteed. The example drains notifications after each phase with a bounded timeout. It does not measure throughput or establish latency guarantees.

## How group commit works

Every event shares the existing bounded admission capacity with other storage operations. An elected asynchronous worker waits 2 ms, takes the partition's writer gate, drains up to 64 pending events and dispatches one blocking transaction. Actual group size is also limited by request concurrency and queue capacity. At light traffic a group may contain one event; 2 ms is deliberate accumulation delay, not an end-to-end latency bound.

Each event has a SQLite savepoint. Invalid inputs and identity/version conflicts roll back only that request; successful neighbours remain in the transaction. Database/commit failures abort the entire group. No successful receipt is sent before SQLite commits using FULL synchronization. Event evidence, source state, history and webhook job insertion stay in that transaction.

Disconnected requests are omitted when the queue is drained. Cancellation after a group starts does not cancel the durable write. Admission permits remain held until blocking work finishes. Graceful API shutdown waits for admitted storage work. The demonstration launcher force-stops only its isolated API after the demonstration; that is not a production shutdown procedure.

`Server-Timing` includes `event_group` (events sharing the commit), queue/service durations and shared transaction timings. Shared commit time is repeated on each receipt; do not sum it across receipts to calculate disk time.

## Tenant configuration and compatibility

Partitioning is opt-in:

```json
[
  {
    "id": "demo",
    "database": "C:\\reconcile\\demo\\events.sqlite3",
    "partitions": 4,
    "credentials": [
      { "id": "checkout", "role": "InternalWriter", "api_key": "replace-with-a-random-secret-at-least-32-characters" },
      { "id": "provider", "role": "ProviderWriter", "api_key": "replace-with-a-different-random-secret-at-least-32-characters" },
      { "id": "audit", "role": "Auditor", "api_key": "replace-with-another-random-secret-at-least-32-characters" }
    ]
  }
]
```

Create the dedicated parent directory first. In partition mode, `database` identifies that tenant directory; the single-file `events.sqlite3` must not already exist. Actual files live in its `partitions` subdirectory. Existing deployments that omit `partitions` keep single-file storage and their current event-ID format; they now use group commit for API ingestion. The stored layout refuses changes to tenant ID or partition count. Existing single-file databases require an explicit migration, which is not included here.

One partition router, write queue and deadline worker operate per database. Tenant-wide request admission and rate limits apply before dispatch; per-partition limits also remain active. This is parallel storage on one host, not replication, failover or horizontal clustering.

## Routing contract

- `GET /v1/partitions` with tenant credentials lists indices and routing requirements.
- In partition mode, `POST /v1/events` requires `X-Reconcile-Payment-Id`: the UTF-8 payment ID percent-encoded as a URI component. The server verifies it matches `payment.id`; callers cannot choose an arbitrary partition for a payment.
- Partition-mode event IDs are `lowercase_hex_sha256(payment.id):stable-unique-suffix`, at most 128 bytes total. Both sources must use distinct suffixes for distinct events. Binding identity to payment prevents an ID from being reused across unrelated partitions. The Rust client supplies the routing header but does not rewrite your event ID.
- Payment status/history/investigation/case requests route automatically from their URL payment ID.
- `/v1/cases`, `/v1/discrepancies`, `/v1/storage-health` and `/metrics` require `X-Reconcile-Partition`. Follow pagination independently for each partition. There is no global consistent snapshot or combined cursor.
- Responses routed to a partition include `X-Reconcile-Partition`.
- Snapshot `/v1/reconcile` keeps its existing contract and does not write event state.

The simulator constructs partition-bound IDs. Existing EventClient/outbox users adopting partition mode must construct and persist those IDs themselves before enqueueing. Do not rewrite IDs on retry.

## Validation for this change

Formatting, compilation and static lint checks were performed. Tests, benchmarks and the demonstration were not run, as requested. Regression test source was added for savepoint isolation and routing decoding. Existing performance results describe the earlier direct-storage implementation, not this integrated API.
