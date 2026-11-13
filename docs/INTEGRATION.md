# Scoped integration and investigation workflow

The engine compares source evidence; it does not transfer money. These additions support integration and investigation, not a certification of banking compliance.

## Run the complete example

From the repository root in Windows PowerShell:

```powershell
cargo fmt
cargo test --locked --all-features
.\examples\run-lifecycle.ps1
```

The script builds the API and Rust example, starts a private loopback API on an available port, and creates four random credentials in an ignored `target/lifecycle-*` directory. It stops only its own server and restores the caller's environment. It retains the SQLite database and event plan. Treat `tenants.json` as secret; do not commit it or share the directory. To rerun the same persisted events, pass `-RunDirectory <printed-directory>`.

The checkout sends an internal completion. A local proxy forwards it to the real API, waits for the committed receipt, then delays that first response past the client's timeout. The client retries the identical event. Next, the provider sends a delayed pending event, an investigator acknowledges the disagreement, and provider version 3 completes it. Version 2 arrives late, followed by a duplicate checkout event.

Expected first-run sequence:

```text
Checkout: 2 attempts, duplicate receipt: true, state: "AwaitingProvider"
Delayed provider: "StatusMismatch"
Investigator acknowledged this revision; payment evidence unchanged
Provider completes: "Matched"
Late provider v2: "Stale"
Retry checkout: duplicate=true
Audit timeline: Baseline -> Opened -> Acknowledged -> Resolved
```

Retries of the entire example preserve event IDs; already resolved payments remain resolved. The demo's proxy is fault injection, not a production component. The script stops its disposable API process after completion; committed SQLite records survive that stop.

## Credentials and permissions

Set `RECONCILE_TENANTS_FILE` to a JSON array with dedicated existing database directories. Use `credentials` instead of the legacy `api_key` field:

```json
[
  {
    "id": "checkout-company",
    "database": "C:\\reconcile-state\\checkout-company\\events.sqlite3",
    "credentials": [
      {"id":"checkout","role":"InternalWriter","api_key":"REPLACE_WITH_UNIQUE_RANDOM_SECRET_1"},
      {"id":"provider","role":"ProviderWriter","api_key":"REPLACE_WITH_UNIQUE_RANDOM_SECRET_2"},
      {"id":"audit-team","role":"Auditor","api_key":"REPLACE_WITH_UNIQUE_RANDOM_SECRET_3"},
      {"id":"operations","role":"Investigator","api_key":"REPLACE_WITH_UNIQUE_RANDOM_SECRET_4"}
    ]
  }
]
```

Generate independent random secrets of at least 32 printable ASCII characters; the strings above are placeholders. Keys must be globally unique, credential IDs unique within a tenant, and each tenant may have 1–16 credentials. Configuration is validated at startup. Restart to apply changes or revocation. Do not also set `RECONCILE_API_KEY` in tenant mode.

| Role | Read tenant endpoints | POST internal event | POST provider event | Notes/acknowledgement | Batch reconcile |
|---|---|---|---|---|---|
| InternalWriter | Yes | Yes | No | No | No |
| ProviderWriter | Yes | No | Yes | No | No |
| Auditor | Yes | No | No | No | No |
| Investigator | Yes | No | No | Yes | No |
| Administrator | Yes | Yes | Yes | Yes | Yes |

Writer roles can read both sources' reconciliation evidence. This is source-scoped write authority, not source-confidential read isolation. All credentials for one tenant share its database, queue, and admission limits. Another tenant's credentials select a different store. Wrong-source writes return HTTP 403 before applying an event. Auditor credentials cannot submit changes. The legacy single `api_key` and unauthenticated loopback development mode retain administrator behaviour for compatibility; use scoped tenant mode for these controls.

## Rust delivery client

Enable the optional `client` Cargo feature in a path or Git dependency on `reconcile-cli`. The module is `reconcile_cli::client`:

```rust,ignore
use reconcile_cli::client::{EventClient, RetryPolicy};
let client = EventClient::new(&api_url, internal_key, RetryPolicy::default())?;
// Persist this PaymentEvent before calling deliver. Never regenerate its ID on retry.
let delivery = client.deliver(&persisted_event).await?;
println!("{} attempts: {:?}", delivery.attempts, delivery.receipt);
```

The defaults permit four attempts, five seconds per attempt, and twenty seconds total including admission and backoff. Exponential backoff uses jitter; HTTP 429/500/502/503/504 and transport failures may retry. `Retry-After` seconds and HTTP dates are respected within the total budget; a longer wait returns an error instead of retrying early. Other HTTP errors, including 401/403/409/422, are not retried. Redirects are disabled. HTTPS is required except for loopback development. Event bodies are limited to 16 KiB and responses to 1 MiB. Clones share an eight-request concurrency bound.

All attempts use the same serialized event ID and payload. This gives idempotent application through the server's durable event log, not exactly-once network delivery. On a timeout, an update may already have committed. `DeliveryError` returns `event_id`, `attempts`, and `outcome_unknown`; retain the original event for later retry. Do not generate a replacement ID or assume a timeout means failure. Cancelling the client future can also leave an uncertain outcome.

This small client is not a durable outbox or background worker. A production sender must persist pending events with its business transaction, retry them after restart, and mark completion only after a receipt. The demo persists its synthetic plan before sending to illustrate stable identities. Server receipts for duplicates contain the current payment state, which may have advanced since the original event.

## History, notes, and acknowledgement

`GET /v1/payments/{payment_id}/history?after=0&limit=100` returns ordered entries, a `next_after` sequence cursor, current revision/status, and whether that revision is acknowledged. Limits are 1–100. Follow `next_after` until null; concurrent updates may advance current state between pages.

Each history entry includes a source-evidence snapshot, recorded and effective times, and a kind:

- `Baseline`: first observation, including existing records upgraded from schema 1. Past history is not fabricated.
- `Opened`: a disagreement first appears after a non-discrepant state.
- `Changed`: discrepancy status or semantic source evidence changes.
- `Resolved`: a disagreement becomes matched.
- `Observed`: another semantic evidence change outside an active discrepancy.
- `Note` / `Acknowledged`: investigator actions, with server-derived credential identity.

Events and their history commit in the same transaction. Exact retries and stale updates do not create duplicate transitions. Revisions reflect semantic evidence changes, not merely a higher source version containing identical values. Event identity/version evidence remains in the event log.

Deadline transitions are observed when a payment is accessed by status/history, investigated, or receives another event. They record the waiting deadline as `effective_at_ms` and observation time as `recorded_at_ms`. There is no background scheduler; a general discrepancy scan does not materialize every history transition. Reads may append these automatic observations even for an auditor, but auditors cannot add notes, acknowledgements, or source evidence.

An investigator posts to `/v1/payments/{payment_id}/investigation`:

```json
{
  "id": "stable-investigation-request-id",
  "action": "Acknowledge",
  "note": "Provider is pending; awaiting its completion event.",
  "expected_revision": 2
}
```

Read `current_revision` first; do not hard-code 2. `Note` is also supported. Acknowledgement requires an active discrepancy. Notes must contain non-whitespace text and at most 2,000 UTF-8 bytes. The actor comes from the authenticated credential, never the request body. Reusing the same investigation ID and request returns a duplicate receipt; changing its payload conflicts. A stale revision returns 409: reload the evidence before deciding again. An exact retry of an already committed action remains valid even after the case advances.

Acknowledgement does not resolve a discrepancy or edit payment evidence. A subsequent semantic change clears acknowledgement for the new revision. Notes and history have no edit/delete API; they are not cryptographically tamper-proof against database administrators. Protect and retain the database appropriately, and keep personal data and secrets out of notes. History grows with changes; retention/archival is not automated.

## Upgrade and verification

See [migration notes](MIGRATION.md) before opening an existing database: this release introduces schema 2. Back up first. The maintenance backup command preserves schema-1 files without migrating them. API startup performs the transactional migration; older binaries reject schema 2.

Automated tests cover cross-source rejection, auditor/investigator permissions, actor spoofing, immutable evidence, revision conflicts, pagination, migration, transactional rollback on history failure, and timeout-after-commit retries. CI runs optional client tests on Windows and Linux. The live demo is additional integration evidence, not a load test. Previous throughput results predate history writes and must be remeasured before making capacity claims.
