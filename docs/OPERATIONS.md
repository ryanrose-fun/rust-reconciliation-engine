# Operating the reconciliation engine

This is a pilot service, not a certification of banking suitability. Authentication and data isolation do not replace infrastructure security, access review, backup retention, or workload-specific acceptance testing.

## Tenant isolation

Single-tenant mode still uses `RECONCILE_DB_PATH` and optionally `RECONCILE_API_KEY`. For multiple companies, configure `RECONCILE_TENANTS_FILE` instead of `RECONCILE_API_KEY`. The server fails startup on invalid configuration. There is no caller-supplied tenant ID: a credential selects a dedicated router, database, queue, rate limiter and metrics registry. IDs are scoped to that database. Snapshot reconciliation remains stateless.

Create a separate local directory for each tenant, outside OneDrive/network shares. Each database must be called `events.sqlite3`. Use absolute paths and distinct directories; database symlinks and reused keys are rejected. Provisioning paths is an administrator operation: do not use hard links or let tenants modify database/configuration files. All tenants share one process and host; this is data isolation, not process or resource isolation. Limits are per tenant, so size aggregate capacity before adding tenants (maximum 64).

Example `tenants.local.json` (generate independent random secrets; these placeholders are not deployment credentials):

```json
[
  {"id":"checkout","api_key":"REPLACE_WITH_RANDOM_SECRET_AT_LEAST_32_CHARS","database":"C:\\reconcile-state\\checkout\\events.sqlite3"},
  {"id":"bank","api_key":"REPLACE_WITH_A_DIFFERENT_RANDOM_SECRET_32_CHARS","database":"C:\\reconcile-state\\bank\\events.sqlite3"}
]
```

```powershell
$env:RECONCILE_TENANTS_FILE = (Resolve-Path .\tenants.local.json).Path
Remove-Item Env:RECONCILE_API_KEY -ErrorAction SilentlyContinue
cargo run --bin api
```

Keep the file readable only by the service administrator/account. Root `tenants*.json` files are gitignored. Store production credentials in your secret manager and materialize a protected configuration file. Rotate by changing the key and restarting; scoped mode supports 1–16 independently identified credentials per tenant. See [the role configuration](INTEGRATION.md); the legacy single key retains administrator access. TLS termination is required outside localhost. Tenant mode never permits unauthenticated access to data or metrics. `/health` and `/openapi.json` remain public. Unknown credentials are not forwarded to any database.

Schema-1 databases upgrade transactionally to schema 3 on API startup. Back up first and follow [migration notes](MIGRATION.md); old binaries cannot open schema 3. To move an existing single-tenant deployment, back it up, restore into that tenant's dedicated directory, stop the old instance, and configure the new tenant path. Never assign the same original data to two companies.

## Find unresolved discrepancies

```powershell
$headers = @{Authorization="Bearer $env:MY_TENANT_KEY"}
Invoke-RestMethod -Headers $headers 'http://127.0.0.1:3000/v1/discrepancies?limit=50&currency=GBP&source=Internal&min_age_ms=60000' | ConvertTo-Json -Depth 12
```

Optional filters: `status` (an exact discrepancy result, e.g. `StatusMismatch`), `currency` (GBP/USD/EUR/JPY), `source` (Internal/Provider), `min_age_ms` (age since first receipt, not since a mismatch began), `limit` (1–100; default 100), and `after` (exclusive payment ID). A source filter requires that source to exist. When combined with currency, it checks that source's currency; without a source, either source can match the currency. Matched payments and those still within their waiting period are excluded.

Each call scans at most 1,000 payment IDs in SQLite binary order, returning at most `limit` entries. Follow `next_after` **even when entries is empty**, URL-encoding it and retaining the same filters. A null cursor means that scan reached the end. Each page is transactionally consistent; the sequence of pages is a live view, not a frozen historical export. Updates to previously scanned IDs may require a fresh traversal. This deliberately bounds work rather than hiding a potentially unbounded full-database search behind one HTTP request.

## Monitor operation

`GET /metrics` exposes Prometheus text with the tenant's same bearer credential:

```powershell
(Invoke-WebRequest -UseBasicParsing -Headers $headers http://127.0.0.1:3000/metrics).Content
Invoke-RestMethod -Headers $headers http://127.0.0.1:3000/v1/storage-health
```

Metrics include request/error/rejection/timeout counts, a request-duration histogram in milliseconds, cumulative admission/queue/dispatch/service/commit/resume times, storage queue depth, active storage operations, and maximum observed async wakeup lag. Stage totals cover successful measured storage operations, not failed/cancelled operations. All metrics reset on restart. No payment IDs, credentials, or tenant names become labels. A scrape contributes to the next request count and shares tenant rate limits. The runtime wakeup probe detects scheduling delays; it does not prove their cause or prevent stalls.

Storage health reports total payments, those awaiting their counterpart, and overdue missing counterparts. It performs a full metadata scan and shares the bounded storage queue; poll sparingly on large databases (e.g. once a minute), not on every request. `/health` is liveness only. Treat successful storage-health responses as a storage-read readiness check; they do not prove disk write availability. Alert on sustained timeouts/storage errors, growing queues or missing-counterpart backlogs, and workload-specific latency objectives. Global process/OS telemetry still needs an external collector.

## Back up, restore, upgrade

```powershell
cargo run --bin maintenance -- backup state/reconciliation.sqlite3 backups/snapshot-001.sqlite3
cargo run --bin maintenance -- verify backups/snapshot-001.sqlite3
cargo run --bin maintenance -- restore backups/snapshot-001.sqlite3 state/restored.sqlite3
```

Create the backup directory first. Backup uses SQLite `VACUUM INTO` for a consistent standalone snapshot including committed WAL data. It can run online, but may contend with writers: schedule it away from peak traffic and retry a busy failure. It explicitly flushes the output file (and parent directory on Unix), then checks integrity, foreign keys, and schema. Destination files must be new. Treat an interrupted/failed backup as unusable; retain the error and choose a new destination on retry. Verification is structural, not proof of every business invariant, and is not encryption or tamper-proof attestation.

Restore refuses an existing destination or its WAL/SHM sidecars. It takes another SQLite snapshot instead of copying an arbitrary live main file. Stop the API before changing its database path; point it at the restored file, restart, retrieve known acknowledged payments, and retry a known event to verify idempotency. Keep the previous database until acceptance checks pass. Never restore over a running database or copy a live `.sqlite3` file without its committed WAL state.

Upgrade procedure: record the old binary/version, make and verify a backup, stop the service gracefully, run the new binary against a restored rehearsal database, run the acceptance checks, then switch production. Unknown schema versions fail closed. This change retains schema version 1. A future schema migration needs its own forward/rollback plan; do not assume an old binary can open a future database. Rehearse restores periodically and establish retention/RPO/RTO requirements with the deployment owner.

Ctrl+C (Windows/Linux) and SIGTERM (Linux/container orchestrators) stop acceptance and drain active requests/storage work. Forced termination cannot drain; commit-before-acknowledgement and retry-safe event IDs remain the recovery mechanism. A timed-out write may still commit: retry the identical event ID and payload, never invent a new ID solely because of a timeout. Keep an external shutdown deadline for a stalled process and audit after any forced stop.

## Sustained-load acceptance

```powershell
.\scripts\load-test-events.ps1 -Seconds 60 -CaseIndex 3,4 -ReportName operations-run1.json
.\scripts\load-test-events.ps1 -Seconds 60 -CaseIndex 3,4 -ReportName operations-run2.json -SkipBuild
```

The runner supports 2–600 seconds per case. It clears tenant configuration for its isolated synthetic database and restores the environment afterwards. Each case uses fresh storage, bounded retries/drain, binary hashes, latency/resource measurements, metrics capture, a full workload database audit after forced termination, and a restart lookup. Longer runs test sustained behavior; they are not the time taken by a single payment.

Acceptance gate: every lifecycle completes; zero transport errors or exhausted retry budgets; exact committed event counts and final payment states; no foreign workload; restart validation passes. A nonzero process exit or failed validation invalidates the throughput result. Run repeated tests on the intended OS/storage, outside development tool sandboxes, with representative monitoring and security software. Compare identical binaries/configuration/data/durability settings; do not claim a speed improvement from different machines or changed timeouts. Previous intermittent Windows stalls remain a known issue until reproduced, explained and independently verified as resolved. See the dated evidence in `docs/performance/`.

For an independent hosted-machine control, dispatch the GitHub Actions 'Sustained event acceptance' workflow. It tests Linux and Windows, retains report artifacts even on failure, and fails the job if any case fails validation. It is separate from fast CI and does not run automatically on each push. On a native Linux checkout, build the release API and perf/event-load binaries, then run: python3 scripts/load-test-events.py --seconds 60 --rounds 2 --report docs/performance/control.json. The portable runner stores evidence under target/soak-evidence and records Linux filesystem type. Do not use a tmpfs-backed checkout to measure persistent disk throughput. It records correctness and latency; only the PowerShell runner samples CPU/memory/storage usage.

## Durable operational workflow

Version 0.4 adds a sender outbox, automatic deadlines, persistent signed webhooks, case assignments, verified archival and recovery drills. See [DURABLE-OPERATIONS.md](DURABLE-OPERATIONS.md) for commands, retention policy, capacity boundaries and restart semantics.
