# Durable delivery and operational workflow (0.4.0)

The engine now has two independent durable queues: the sender's outbox and each tenant's webhook queue. Automatic deadlines create missing-counterpart cases. Operators can assign those cases without editing source evidence. Archive and recovery commands preserve the records needed to explain and safely retry transactions.

## Start with the isolated walkthrough

From the repository root in Windows PowerShell:

```powershell
cargo fmt
cargo test --locked --all-features
.\examples\run-operations.ps1
```

The script creates synthetic data under a new ignored `target/operations-demo-*` directory. It saves a checkout event before starting the API, delivers it from the persisted outbox, waits for an automatic missing-provider case, assigns that case, stops the sender, queues provider completion, restarts the sender, and checks `Matched`. Re-enqueuing the same event adds nothing. Finally it runs a backup/restore drill and verifies an archive. It stops only processes it created and restores your environment. Its temporary administrator key is for this isolated example; use scoped credentials in a real integration.

The companion network tests exercise a receipt lost after commit and a webhook receiver that processes a notification but returns an error:

```powershell
cargo test --locked --features client --test durable_delivery
```

Both recover using their original IDs. Neither network delivery is exactly-once: receivers must deduplicate.

## 1. Sender outbox

Build the worker and CLI with the optional client feature:

```powershell
cargo build --locked --features client --bin outbox --bin webhook-worker
$env:RECONCILE_URL = 'http://127.0.0.1:3000'
$env:RECONCILE_API_KEY = '<the internal-writer credential>'
New-Item -ItemType Directory -Force state
cargo run --features client --bin outbox -- enqueue state/checkout-outbox.sqlite3 checkout-event.json
cargo run --features client --bin outbox -- status state/checkout-outbox.sqlite3
cargo run --features client --bin outbox -- run state/checkout-outbox.sqlite3
```

`checkout-event.json` is one normal event object, not an array. Keep its `event_id` stable across retries. Use a separate outbox for the provider's credential. `run` continues until Ctrl+C. After a crash, restart the same command with the same database and credentials.

Enqueue commits to SQLite WAL with `synchronous=FULL` before returning. Reusing an ID with the same payload is a no-op; different payloads conflict. The outbox is pinned to a hash of its endpoint and credential to prevent accidentally sending saved events to another tenant. Secrets themselves are not stored in this binding.

Workers claim jobs transactionally with a random lease token and a 130-second lease. Only the current token can finalize the job. A crashed worker's claim becomes eligible again after expiry. Each job permits eight persisted delivery cycles; each cycle uses the HTTP client's bounded retry policy (four attempts by default). Between cycles, exponential backoff with jitter grows to five minutes. Permanently rejected requests and exhausted retries stay visible, rather than being discarded. Cancellation or timeout may leave an already committed remote update; the original ID makes the retry safe.

Inspect and explicitly retry an exhausted job:

```powershell
cargo run --features client --bin outbox -- list state/checkout-outbox.sqlite3
cargo run --features client --bin outbox -- redrive state/checkout-outbox.sqlite3 '<event-id>'
```

`list` returns at most 100 records without payment payloads. Pass the last returned ID as a third argument to get the next page. `status` reports pending, delivered, exhausted and oldest pending time. Redrive resets the attempt budget but preserves identity and payload. The queue rejects new jobs once 10,000 pending/exhausted records exist; delivered identities remain retained.

For credential rotation, stop senders, set the intended new URL/key, and explicitly run `outbox rebind <database>`. Active leases block rebinding; wait for their expiry. Verify the new key belongs to the same tenant and source. This command is an administrator decision: it can redirect all queued data. Previously running workers with the old binding cannot claim further jobs.

### Atomicity with the checkout's business transaction

Saving to a separate outbox *after* committing an order leaves a crash gap. The Rust library exposes `Outbox::enqueue_in_transaction(&tx, &event)` so a SQLite application can commit business rows and the event together in an initialized outbox database:

```rust,ignore
let outbox = Outbox::open(path)?; // initializes the database once
let mut connection = rusqlite::Connection::open(path)?;
let tx = connection.transaction()?;
tx.execute("INSERT INTO orders(id) VALUES(?1)", [&order_id])?;
Outbox::enqueue_in_transaction(&tx, &event)?;
tx.commit()?;
```

Provision your business tables separately. Do not change the outbox database's application ID or schema version. For an existing PostgreSQL or other database application, implement its outbox in that same business transaction and use the HTTP client to deliver it; this library cannot make two independent databases atomic. The test suite proves rollback and commit of both business rows and an outbox event.

## 2. Automatic deadlines

API startup enables a background worker for each tenant. Every 500 ms it processes at most 100 due payments through the existing bounded storage queue. A dedicated indexed pending-deadline table avoids repeatedly scanning completed payments. Busy queues defer work to the next tick. Thus the expiry timestamp is exact, but notification/observation can be later under backlog or overload; there is no hard half-second latency promise.

Deadline state is persisted, so overdue payments are processed after restart. The worker records `Opened` with the original waiting deadline as effective time. It does not extend that deadline. A provider arriving later can resolve the case normally. Existing state/history reads still evaluate deadlines for immediate correctness. Library-only callers can run `EventStore::process_deadlines(100)` themselves; the automatic loop belongs to the API.

## 3. Signed, durable webhook notifications

Run one supervised worker for each tenant database. Configure it before accepting events if all future transitions must be notified:

```powershell
$env:RECONCILE_WEBHOOK_URL = 'https://your-receiver.example/reconciliation'
$env:RECONCILE_WEBHOOK_SECRET = '<independent random secret of at least 32 bytes>'
cargo run --features client --bin webhook-worker -- run 'C:\reconcile-state\tenant-a\events.sqlite3'
```

The worker enables notification enqueueing persistently in that database. Stopping it does not disable enqueueing. `Opened`, `Changed`, `Resolved`, and discrepant migration baselines create jobs in the same transaction as their history/evidence change. Historical transitions from before enablement are not replayed. Notes and assignments do not generate source-discrepancy notifications.

The worker sends JSON with `id`, `kind`, `revision`, effective/recorded times, and payment evidence. Headers:

- `X-Reconcile-Delivery`: stable notification ID.
- `X-Reconcile-Timestamp`: current UTC milliseconds for this attempt.
- `X-Reconcile-Signature`: lowercase hex HMAC-SHA256 over `timestamp + "." + delivery_id + "." + raw_body`.

Verify the signature over the exact bytes before parsing JSON, allow at most five minutes of clock skew, and deduplicate the delivery ID transactionally with your receiver's side effect. `reconcile_cli::webhooks::verify_signature` implements this verification. Retries retain the body and delivery ID but get a fresh timestamp/signature. This authenticates delivery from the engine, not the truth of the original payment evidence.

Delivery is at-least-once, with eight persisted attempts, a ten-second network deadline, lease recovery, and jittered exponential backoff. HTTP 2xx acknowledges receipt; 429/500/502/503/504 and transport errors retry. Other status codes exhaust the job. `Retry-After` seconds or HTTP dates are honored; delays beyond 24 hours require operator redrive rather than an early automatic retry. Redirects are disabled; HTTPS is required except loopback development. URLs are administrator configuration, never caller-supplied destinations. Response bodies are not retained or logged.

Inspect with `webhook-worker status <database>` or `list <database> [after-id]`, then `redrive <database> <delivery-id>` after fixing the receiver. Destination changes are rejected while pending/exhausted jobs exist; workers check the destination inside the claim transaction. Multiple workers may complete different deliveries out of order, so use the history revision to reject stale receiver state. Rotate signing secrets with an overlap period at the receiver.

The 10,000-job active backlog is a deliberate backpressure boundary: if enqueueing cannot succeed, the payment/history transaction also fails and the sender must retry its original event. Monitor pending/exhausted counts and oldest pending age. Storage remains synchronous FULL; no delivery worker holds a database transaction while making an HTTP request.

## 4. Investigation queue

```powershell
$headers = @{ Authorization = 'Bearer <auditor-or-investigator-key>' }
Invoke-RestMethod 'http://127.0.0.1:3000/v1/cases?owner=alice&workflow=InProgress&acknowledged=false&min_age_ms=60000&limit=50' -Headers $headers | ConvertTo-Json -Depth 10
```

Filters are optional. `after` is an exclusive payment-ID cursor; follow `next_after`. Pages contain at most 100 entries. Age means time since this discrepancy episode opened, not the payment's first receipt. Queue state reflects deadline work already processed; look up a particular payment if immediate evaluation is needed.

An investigator or administrator can POST `/v1/payments/{payment_id}/case`:

```json
{"id":"stable-assignment-id","expected_revision":2,"expected_case_version":0,"owner":"alice","workflow":"InProgress","note":"Checking provider settlement"}
```

Use the actual `revision` and `case_version` returned by the queue. Workflow values are `Open`, `InProgress`, and `Waiting`; `owner: null` clears assignment. The owner is an external team/user label, not a new authentication identity. Evidence revision and case version both prevent stale writes. Exact retries return the original receipt. A `WorkflowChanged` history entry records actor, note, assignment and workflow. Only matching source evidence resolves the discrepancy. Resolved cases leave the active queue; if they reopen, ownership/workflow are retained but acknowledgement is revision-specific. Auditor/source-writer credentials cannot change assignments.

## 5. Archival, retention and recovery

First stop an old API binary before upgrading its database. Version 0.4.0 migrates event schema 2 (or 1) transactionally to schema 3. It adds pending deadlines, case workflow and delivery tables. Back up before upgrade. Rollback requires restoring the pre-upgrade snapshot; do not lower `user_version` manually.

```powershell
cargo run --bin maintenance -- archive state/events.sqlite3 backups/archive-001 90
cargo run --bin maintenance -- verify-archive backups/archive-001
cargo run --bin maintenance -- drill state/events.sqlite3 backups/drill-001
```

Create the parent `backups` directory first; each destination must be new. `archive` takes a consistent full SQLite snapshot and exports history at least 90 days old to JSON Lines. Its manifest records hashes, record count, cutoff, and creation time. The completion manifest is written last, files are synced, and verification checks the snapshot plus export. Partial directories from failed runs must not be treated as completed archives. Copy verified archives to protected off-host storage and verify them again there. Checksums detect accidental corruption; they are not signatures against a malicious administrator.

**Retention policy in this implementation:** archive history older than the operator-selected age (90 days is an example, not a financial/legal standard); retain live evidence, event IDs, investigation IDs and delivered-job identities indefinitely. No automatic purge is performed. This keeps replay protection and the live audit trail intact, but database size will grow. Choose an organizational retention policy before production use; shortening live retention requires a separate design for durable dedupe tombstones and querying archived history. Archive copies are not a space-reclamation operation.

`drill` creates a verified backup and a second restored database, then compares logical contents of every application table. It writes `drill.json` only on success. It never swaps the running API's database. Run it regularly through your scheduler and rehearse switching the stopped service to a restored file. Existing CI also kills/restarts a real API and verifies committed events. The drill measures restoration and content equality, not hardware power-loss guarantees.

These commands operate on the engine database, not the sender outbox. Back up an outbox using a consistent SQLite backup, or stop all its users and preserve its database/WAL files together. Protect snapshots, exports and notes as payment data.

## 6. Performance evidence

For a local storage benchmark:

```powershell
cargo run --release --locked --example operations_benchmark -- target/storage-run-001 1000
```

It runs three rounds with fresh disk-backed WAL/FULL databases, with and without durable notification enqueueing. Each payment produces three committed events: internal completion, provider pending, provider completion. It verifies history/delivery row counts and a recovery drill. Reports include event rate; p50/p95/p99 latency; commit and transaction-work costs; database/WAL bytes; deadline batch and investigation-query latency; and recovery time. It does not measure network webhook delivery or HTTP throughput.

The sustained-load workflow separately tests the complete HTTP event API for two 60-second rounds at concurrency 8 and 16 on hosted Windows and Linux, with database audits and restart checks. Reports and interpretation are in [the 0.4 performance report](performance/durable-operations.md). Do not compare old matching-only figures to durable event processing, or infer production capacity from shared CI hardware. Monitor storage growth and tail latency, not just average events per second.
