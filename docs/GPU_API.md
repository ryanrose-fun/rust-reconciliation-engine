# Run the Rust GPU Finance API

The opt-in `gpu` feature exposes a resident GPU reconciliation worker through the
existing authenticated HTTP service. A SQLite WAL/FULL journal makes acknowledged
updates recoverable after process restart. Computation is performed on a discrete
GPU; the service does not silently switch these endpoints to CPU reconciliation.

## Start on Windows

From the repository root, set a private random bearer token. Use the same token in
the client terminal. Do not put real tokens into source control or shell examples
you publish. One way to create a token locally is:

```powershell
$bytes = New-Object byte[] 32
$rng = [Security.Cryptography.RandomNumberGenerator]::Create()
$rng.GetBytes($bytes)
$rng.Dispose()
$env:RECONCILE_API_KEY = [Convert]::ToBase64String($bytes)
# Copy this token privately to your client terminal before starting the server.
$env:RECONCILE_API_KEY
.\scripts\run-gpu-api.ps1
```

The default data directory is `%LOCALAPPDATA%\RustGpuFinance`, outside OneDrive.
Keep the **same directory** when restarting. Startup rebuilds GPU state and checks
replayed results before opening the listening socket. The launcher displays the
journal path. It never deletes existing state.

In a second PowerShell terminal, from the same repository:

```powershell
$env:RECONCILE_API_KEY = Read-Host 'Paste your server token privately'
.\examples\test-gpu-api.ps1
```

The example submits two sources, checks an exact request retry, and prints a GET
command for checking that payment after restarting the server. Stop with Ctrl+C,
restart with `run-gpu-api.ps1`, and run the printed GET command.

Optional: `-BindAddress 127.0.0.1:3001 -Capacity 1000000 -DataDirectory C:\some\local\directory`.
Capacity is persisted in the journal metadata; changing it for an existing journal
requires an explicit migration. `RECONCILE_GPU_ADAPTER` selects a matching discrete
adapter name substring. Windows uses DX12; the Vulkan path on other operating systems
has not received the same hardware acceptance run.

## Endpoints

All GPU routes require the configured bearer token, including on loopback.

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/gpu/events` | Apply a batch and return a durably committed GPU result |
| GET | `/v1/gpu/payments/{payment_id}` | Read current source values and result from GPU memory |
| GET | `/v1/gpu/health` | GPU readiness, adapter, capacity and committed batch sequence |
| POST | `/v1/gpu/checkpoint` | Atomically persist the current committed GPU state; returns its sequence |
| GET | `/openapi.json` | Generated contract, including GPU routes in a GPU-enabled build |

```json
{
  "request_id": "checkout-batch-0001",
  "events": [
    {
      "event_id": "internal-PAY-001-v1",
      "source": "Internal",
      "version": 1,
      "payment": {
        "id": "PAY-001",
        "amount_minor": 10000,
        "currency": "GBP",
        "status": "Completed"
      }
    }
  ]
}
```

Successful responses contain `request_id`, `commit_sequence`, `duplicate`, and
`gpu.receipts` / `gpu.changes`. Receipts include a readable `status` and numeric
source outcomes. Source outcome codes are: 0 not submitted, 1 Applied, 2 exact
version/payload retry, 3 same-version conflict, 4 stale. A result code of 1 is Matched;
bits 2/4/8 indicate amount/currency/status disagreements; 16/32 mean missing
provider/internal. Changes contain payment ID and before/after result codes.

The GET response includes source version, exact amount, readable currency/status,
and the current reconciliation status. A version or evidence update that leaves
the result code unchanged still receives a receipt but emits no result-change row.

## Retry and durability contract

- Keep the **same request_id and exact event content/order** after a timeout or lost
  response. The original committed receipt is returned with `duplicate: true`.
- Reusing a request ID or event ID with different content returns HTTP 409. Exact
  event content under a new request ID is safe for state, but produces a new batch
  receipt based on its then-current source version.
- Versions are nonzero u64 values per payment/source. Equal version with different
  payload is a conflict outcome; older versions are stale. These business outcomes
  are recorded in successful batch receipts and do not overwrite evidence.
- Each source applies independently; a batch may contain Applied and Conflict/Stale
  outcomes together. Persistence commits the entire input/result record atomically.
- A successful POST is returned only after a SQLite WAL transaction commits with
  `synchronous=FULL`. A timed-out admitted request can still commit: retry its ID.
- If storage fails after GPU mutation, the GPU worker stops serving reads/writes.
  Restart replays only committed journal entries. Unacknowledged work must be retried.
- Recovery verifies SQLite integrity, journal sequence, input SHA-256, event identity
  records and shader/format/capacity metadata. With a checkpoint it verifies its payload,
  identity manifest and journal-prefix chain, restores GPU buffers with readback
  verification, then regenerates receipts/change rows for the remaining tail.
  A mismatch prevents startup. An advisory file lock prevents two cooperating API
  processes from owning the same journal.

## Limits and deployment scope

One GPU-backed tenant per process, one discrete GPU, 1..2,000,000 resident payment
IDs, 1..4,096 events per request, a 32-command actor queue and 32 admitted preparation
slots held until the command finishes (including after HTTP cancellation). There may be only
one event per payment/source in a request. Request/event IDs are at most 128 UTF-8
bytes; payment IDs at most 256. Existing API body, rate, concurrency and timeout
limits also apply. Capacity exhaustion returns 422; full/unavailable GPU queues
return 503. No CPU fallback is used.

GPU mode requires `RECONCILE_API_KEY` and rejects `RECONCILE_TENANTS_FILE`. Its token
currently has administrator access; use separate processes/journals for isolation.
Existing source-scoped multi-tenant and partitioned CPU endpoints retain their
existing behavior. GPU state uses its own namespace and **is not mirrored into
the legacy `/v1/events` store**. Existing waiting deadlines, investigation history,
webhooks and source-role credentials are not yet integrated into GPU endpoints.

Automatic checkpoints default to every 256 newly committed batches. Set
`RECONCILE_GPU_CHECKPOINT_BATCHES` to 1..1,000,000 to change the interval. Publication
uses one bounded background publisher. Only immutable GPU snapshot capture pauses
the actor; encoding, checksums, identity scanning and disk publication run outside it.
Disk/CPU contention can still affect latency. An authenticated empty
POST to `/v1/gpu/checkpoint` requests a checkpoint at the latest committed sequence;
its response waits until at least that boundary is durably published. Concurrent
requests can coalesce onto a later boundary. Repeating it without intervening writes
is a no-op. Empty state returns 422; at most 32 manual waiters are retained.
Health reports `checkpoint_sequence` and nullable `checkpoint_in_flight` separately.

The checkpoint is a compact little-endian copy of the GPU's slots, source values,
versions and results. It is stored in `gpu-checkpoints.sqlite3`, beside the event
journal `gpu.sqlite3`, with its own WAL/FULL transaction. This avoids holding the
event journal's writer lock while publishing a large blob. The captured boundary
is already durably committed in the journal; recovery verifies that binding before
readiness, so no cross-database atomic write is required. Payload, boundary and
identity manifest publish atomically together. A failed publication leaves the
previous checkpoint intact and stops the worker when the failure is observed.
Explicit checkpoint requests do not report success before publication.

Only one immutable snapshot may be in flight. Pending requests are coalesced rather
than queuing more full snapshots. Shutdown waits for the publisher before releasing
the journal ownership lock. Corruption fails startup rather than silently choosing
an older checkpoint. Existing embedded checkpoints from the preceding release are
still loaded when no separate checkpoint exists; later snapshots publish to the new
file. Journals without checkpoints still replay normally. Nothing is deleted.

The complete journal and identity index remain retained. Startup still scans their
integrity; only GPU/JSON replay is limited to the tail. This is not constant-time
recovery or a retention policy. Do not externally edit, delete, renumber or vacuum
identity rows: checkpoint manifests bind their row order. Checksums detect accidental
corruption, not an attacker able to rewrite the database and all its checksums.

Disk loss is not process restart: maintain backups. Never delete
SQLite WAL/SHM files while the service is running. For a simple offline copy, stop
the service cleanly and retain the complete data directory, including both databases
and any WAL/SHM files. The GPU journal records
payment evidence and needs filesystem access controls appropriate to that data.

## Reproducible hardware acceptance

```powershell
cargo test --locked --features gpu,client --test gpu_api_recovery -- --ignored --nocapture
```

This intentionally requires a real discrete GPU. Ordinary CI compiles GPU support
and runs portable checks; the hardware test is explicitly invoked, not silently
emulated. It covers authenticated HTTP, forced process kills, committed replay,
idempotency conflicts, stale/version-conflict outcomes, exclusive journal ownership,
storage failure after GPU mutation, uncertain-outcome retry, and corrupt-journal
startup rejection. Each run prints its isolated evidence directory.

The earlier millisecond compute benchmarks exclude HTTP and durable commits. They
are not latency claims for this durable endpoint.
