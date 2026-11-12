# API contract for version 0.2.0

Base URL defaults to `http://127.0.0.1:3000`. Route version remains `/v1` during this unreleased pilot. Version 0.2.0 deliberately replaces the old GBP-only amount format; see MIGRATION.md.

## Routes

| Method | Route | Result |
| --- | --- | --- |
| GET | `/health` | 200 with `{"status":"ok"}`; process liveness only |
| GET | `/openapi.json` | Generated OpenAPI document |
| POST | `/v1/reconcile` | 200 with a report, including when discrepancies exist |

Set `Content-Type: application/json`. When `RECONCILE_API_KEY` is configured, reconciliation requires `Authorization: Bearer <token>`. Health and OpenAPI remain public. Loopback-only development may omit a token. The generated contract describes the authenticated service.

## Event API

POST /v1/events accepts one versioned payment update. GET /v1/payments/{payment_id} reads its current comparison. Both require the configured bearer key. See [EVENTS.md](EVENTS.md) for request examples, idempotency, deadlines and error codes; both routes are included in openapi.json.

## Snapshot request

```json
{
  "internal_payments": [
    {"id":"PAY-001","currency":"GBP","amount_minor":5000,"status":"Completed"}
  ],
  "provider_payments": [
    {"id":"PAY-001","currency":"USD","amount_minor":4500,"status":"Pending"}
  ],
  "source_labels": {"internal":"checkout-export-001","provider":"processor-export-001"}
}
```

Both payment arrays are required; empty arrays are accepted. `source_labels` is optional; if present, both labels must be nonblank and at most 128 Unicode characters. Labels are references, not verified origins. Unknown properties are rejected.

| Payment field | Rules |
| --- | --- |
| `id` | Nonblank string; exact, case-sensitive comparison; not trimmed; unique within each source |
| `currency` | Required enum: GBP, USD, EUR, JPY |
| `amount_minor` | Required unsigned 64-bit JSON integer, 0 through 18446744073709551615 |
| `status` | Required enum: Pending, Completed, Failed |

GBP, USD, EUR use 100 minor units per major unit; JPY uses 1. No conversion or rounding occurs. The engine independently compares raw minor-unit integers, statuses, and currencies. An amount difference across currencies is a raw-field difference, not an FX valuation. Unsupported currencies are rejected rather than guessed. Clients must preserve integers exactly; JavaScript Number is not exact above 9007199254740991.

## Response

The response has three top-level objects: `metadata`, `summary`, and `entries` (an array).

`summary` contains `total_entries`, `matched`, and `discrepancies`. Each distinct payment ID appears once. A payment with three differences still counts as one discrepant entry. Total entries equals matched plus discrepancies.

Each entry includes:

- `payment_id`, `result`, `recommended_action`.
- `internal_amount_minor`, `provider_amount_minor`.
- `internal_currency`, `provider_currency`.
- `internal_status`, `provider_status`.

The three evidence fields for an absent source are null. An actual zero is 0. Entries follow internal input order, then provider-only input order. Matching two Failed or Pending statuses means agreement, not successful payment.

| Result | Recommendation |
| --- | --- |
| Matched | ReviewPaymentStatus |
| AmountMismatch | InvestigateAmount |
| StatusMismatch | RecheckSource |
| AmountAndStatusMismatch | InvestigateAmountAndStatus |
| CurrencyMismatch | InvestigateCurrencyAndValues |
| AmountAndCurrencyMismatch | InvestigateCurrencyAndValues |
| StatusAndCurrencyMismatch | InvestigateCurrencyAndValues |
| AmountStatusAndCurrencyMismatch | InvestigateCurrencyAndValues |
| MissingFromProvider | RecheckSource |
| MissingFromInternal | RecheckSource |

The internal single-pair comparator also defines DifferentPaymentIds / InvestigateIdentity. The HTTP endpoint matches by ID, so unrelated IDs produce missing-source entries instead. Recommendations are informational; none executes recovery or payment operations.

`metadata` contains report_id (UUID), generated_at (UTC RFC3339), engine_version, rules_version, fingerprint_format, internal_source, and provider_source. Each source has a nullable label, record_count, and sha256. See TRACEABILITY.md for exact hashing semantics.

## Limits and errors

Defaults: 1 MiB JSON body, 5,000 payments per source, 8 active requests/workers, 10-second deadline, global 100 requests/second fixed-window limit. Deployment environment variables can change these within hard bounds. The OpenAPI types describe the data model; cross-record duplicate rules, runtime limits, and source-label blank checks are additionally enforced by the server.

Errors use `{"error":{"code":"...","message":"..."}}`. Clients should branch on status and code, not message text. Transaction values are not echoed in parser or validation error messages. The endpoint returns one error, not a partial report or complete list of validation failures.

| Status | Codes |
| --- | --- |
| 400 | invalid_json |
| 401 | unauthorized |
| 404 | not_found |
| 405 | method_not_allowed |
| 413 | request_too_large, too_many_payments |
| 415 | unsupported_media_type |
| 422 | invalid_payment_data, invalid_payment_id, invalid_source_labels, duplicate_internal_ids, duplicate_provider_ids |
| 429 | rate_limited |
| 500 | internal_error |
| 503 | busy |
| 504 | timeout |

Blank IDs, invalid currencies, amounts and statuses are normally rejected while decoding and return invalid_payment_data. Internal duplicate IDs take priority over provider duplicate IDs if both snapshots have duplicates.

Responses passing through the reconciliation guard have an `x-request-id` header. Logs contain that ID, status and elapsed time, not payment bodies or tokens. A timeout stops waiting for the response; a running computation may finish later, while retaining its worker slot until completion.

## PowerShell

Start the server in one terminal; use another in the project folder:

```powershell
Invoke-RestMethod http://127.0.0.1:3000/health
.\examples\test-api.ps1
.\examples\test-both-mismatches.ps1
```

Set the same `RECONCILE_API_KEY` variable in both terminals when testing authentication. The scripts support Windows PowerShell 5.1 and preserve JSON arrays without introducing wrapper properties.

To inspect a validation error:

```powershell
$headers = @{}
if ($env:RECONCILE_API_KEY) { $headers.Authorization = 'Bearer ' + $env:RECONCILE_API_KEY }
try {
  Invoke-RestMethod -Headers $headers -Uri http://127.0.0.1:3000/v1/reconcile -Method Post -ContentType application/json -Body '{}'
} catch {
  [int]$_.Exception.Response.StatusCode
  $_.ErrorDetails.Message
}
```

Expected: 422 invalid_payment_data when authenticated correctly. Missing/invalid tokens return 401 first.
## Overload response delivery and processing timings

429 and 503 responses include Retry-After: 1 and x-request-id. Before returning an early rejection, the API consumes and discards the upload without parsing it. This keeps valid HTTP/1 uploads from being reset while the client is still sending.

This work is bounded: at most the configured body limit, at most the smaller of the request timeout and 2 seconds, and at most twice the configured concurrency in simultaneous rejected-body drains. If a sender exceeds those limits or all drain slots are occupied, HTTP/1 connections are closed. A readable JSON error is not guaranteed beyond these resource boundaries. Configure equivalent connection/upload limits at the deployment proxy.

Successful responses include Server-Timing durations in milliseconds:
- body: receiving and buffering the bounded body.
- queue: scheduling delay before the blocking worker starts.
- json_decode: decoding and schema validation.
- reconcile: snapshot validation, matching, report assembly and fingerprints.
- json_encode: encoding the complete report.

The same values are recorded as stages in structured request logs. End-to-end client latency also includes transport and response delivery. Do not add server and client timings together. Parsing, reconciliation and encoding run in the bounded worker pool; worker permits remain held until that work ends even if the HTTP deadline expires. Processing-stage timers do not separately account for every cleanup/allocation operation.

The API enables TCP_NODELAY on accepted sockets, using Axum's supported listener hook, so small writes are not intentionally delayed by Nagle's algorithm. The request and report JSON shapes are unchanged.