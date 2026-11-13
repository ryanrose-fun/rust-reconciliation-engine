# Continuous payment reconciliation

The existing POST /v1/reconcile compares complete snapshots. The new event API accepts one source update, stores it, and compares it with the latest known version from the other source. It does not move money or authenticate the origin of payment records.

## 1. Start the API

Stop the existing API with Ctrl+C before rebuilding on Windows. From the project folder:

~~~powershell
cargo fmt
cargo test --locked
cargo run --bin api
~~~

The default database is state/reconciliation.sqlite3, relative to the working directory. The API creates it automatically. Keep starting from the same directory, or set RECONCILE_DB_PATH to an absolute path.

## 2. Run the guided example

In another PowerShell window in the project folder:

~~~powershell
.\examples\test-events.ps1
~~~

The script creates unique identifiers so you can run it repeatedly, checks each response, and prints:

| Step | Outcome | Current payment status |
| --- | --- | --- |
| Internal says Completed | Applied | AwaitingProvider |
| Provider says Pending, version 1 | Applied | StatusMismatch |
| Provider says Completed, version 2 | Applied | Matched |
| Retry the original internal event | Applied, duplicate_event true | Matched |
| An older provider version arrives | Stale | Matched |

The retry returns the original event outcome and the **current** payment state. It does not replay an old status.

The example assumes the default 30-second waiting period. If you configured a very short period, its first waiting-state assertion may expire before the response is evaluated.

## 3. Understand an event

POST /v1/events accepts application/json:

~~~json
{
  "event_id": "checkout-delivery-001",
  "source": "Internal",
  "version": 1,
  "payment": {
    "id": "PAY-001",
    "amount_minor": 10000,
    "currency": "GBP",
    "status": "Completed"
  }
}
~~~

- payment.id identifies the payment shared by both systems.
- event_id identifies this specific update. Keep it identical when retrying delivery; use a new ID for a genuinely new update.
- source is exactly Internal or Provider.
- version is monotonically increasing **per payment and per source**. Internal version 8 and Provider version 2 are compared normally; their counters are independent. Version gaps are allowed.
- Version must be 1 through 9223372036854775807. Amounts support the full unsigned 64-bit range; clients must not convert large integers to floating point.
- Event IDs are nonblank, at most 128 UTF-8 bytes; payment IDs at most 256 bytes. Control characters are rejected. IDs are case-sensitive and are not trimmed.
- Currency/status validation is shared with snapshot reconciliation. Unknown JSON fields are rejected.
- Event bodies are limited to the smaller of 16 KiB and RECONCILE_MAX_BODY_BYTES.

Every accepted event returns HTTP 200 with event_id, duplicate_event, outcome and payment. Current state includes both source versions, their values, the comparison result, recommendation and receipt times.

## 4. Read the current status

~~~powershell
Invoke-RestMethod http://127.0.0.1:3000/v1/payments/PAY-001 |
    ConvertTo-Json -Depth 10
~~~

Use the unique payment ID printed by the example instead of PAY-001. URL-encode IDs containing reserved URL characters. If authentication is configured, supply the same Authorization bearer header used by the existing example scripts. The event script reads RECONCILE_API_KEY from its shell.

Unknown payments return HTTP 404. Responses use Cache-Control: no-store.

## 5. Waiting and late arrivals

The first source starts a fixed waiting deadline, by default 30 seconds:

- Only Internal is present: AwaitingProvider, then MissingFromProvider.
- Only Provider is present: AwaitingInternal, then MissingFromInternal.
- Both are present: compare their latest values immediately.
- A late counterpart replaces a missing result with the actual comparison.
- Retries and new versions do not extend the original deadline.
- A status is evaluated when you read it or submit another event. There is no background webhook, stream or push notification yet.

Configure the period before starting the server:

~~~powershell
$env:RECONCILE_EVENT_WAIT_MS = "30000"
$env:RECONCILE_DB_PATH = "C:\Users\local-user\rust\rust-reconciliation-engine\state\reconciliation.sqlite3"
cargo run --bin api
~~~

The API permits 1 through 86400000 milliseconds. Stored deadlines survive restart; changing configuration affects newly seen payments only. Timestamps are UTC Unix milliseconds from the server clock; keep the host clock synchronized.

## 6. Retries, ordering and errors

| Situation | Response / effect |
| --- | --- |
| Exact same event ID and typed content | 200, duplicate_event true; no second update |
| Same event ID with changed content | 409 event_id_conflict; no change |
| Lower source version, new event ID | 200 Stale; recorded, but current state unchanged |
| Equal source version and same payment values, new ID | 200 Unchanged |
| Equal source version with different values | 409 version_conflict; no change |
| Storage contention | 503 storage_busy; retry the same event with backoff |
| Bad event fields | 422 invalid_event |
| Malformed JSON / wrong content type / too large | 400 / 415 / 413 |

The shared API authentication, rate and concurrency limits still apply. A timeout or lost response is ambiguous: a blocking transaction may still commit. Retry the **same event ID and content**. This provides idempotent committed state changes, not an exactly-once network delivery guarantee.

JSON whitespace and object key order do not change event identity: comparison uses the parsed, canonical event.

## 7. Persistence and verification

SQLite stores the event receipt and latest source state in one transaction, and the server acknowledges after commit. It uses WAL mode and synchronous=FULL. SQLite documents this configuration's commit syncing in its [WAL](https://www.sqlite.org/wal.html) and [synchronous pragma](https://www.sqlite.org/pragma.html#pragma_synchronous) references. Storage durability still depends on the OS and hardware honoring sync operations.

Library tests cover rollback, retry conflicts, stale versions, deadlines, separate connections and reopening a database. Run the isolated process recovery test after building:

~~~powershell
cargo build --bin api
.\scripts\test-event-recovery.ps1
~~~

It starts its own server on a temporary port/database, submits an event, forcibly stops that process, restarts, and verifies the event, deadline, maximum integer amount, retry identity and late counterpart. It never stops your existing server.

The snapshot API and CLI remain available. Snapshot reports are not automatically imported into the event database.

## Operating limits

This is a durable local pilot, not a certified banking platform. A bounded FIFO queue serializes HTTP access to the SQLite connection; full queues and global limits return retryable overload responses; event throughput must be measured separately from snapshot benchmark results.

There is one shared namespace and service credential. A caller with that key can submit either source. There is no tenant isolation, scoped source authorization, signed provenance, replication, failover, tamper-evident history or retention policy. Accepted stale and unchanged events are retained, so database size grows.

Use a local disk and restrict database file access. Database files are excluded from source packaging/Git. For a simple consistent backup, stop **all** processes using the database, then back up its directory including any WAL files. Do not copy only the main SQLite file while writers are running. Define and test restore procedures before storing important data.

Containers mount a persistent named volume at /var/lib/reconcile. Removing that volume destroys state; container replacement alone should preserve it. Container deployment still needs testing on its target host.
See [EVENT-PERFORMANCE.md](EVENT-PERFORMANCE.md) for the isolated durable-event load test and metric definitions.

See [EVENT-QUEUE.md](EVENT-QUEUE.md) for queue limits, cancellation behaviour and transaction-stage measurements.
