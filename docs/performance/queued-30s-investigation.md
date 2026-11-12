# Thirty-second load failure investigation

Status: unresolved. Do not use these failed runs as validated sustained-capacity results.

Evidence retained:
- queued-30s.json: original user run. Eight transport failures at concurrency 8; transport failures and exhausted retry budgets at concurrency 16. Both SQLite integrity checks were OK.
- queued-30s-diagnostic.json: detailed error chains identify request timeouts.
- queued-30s-background-logs.json: timeouts remained after removing synchronous request-log writes from async workers. This disproves logging as a complete explanation.
- queued-30s-clock-probe.json: the reported client completed 2,824 lifecycles in 30.11 seconds with no request errors and no independently sampled scheduling gaps above 250 ms. The database nevertheless contained 5,459 payments from two distinct workload IDs:
  - LOAD-398673a081ee483d8d0838b7e0715d02: 2,635 payments
  - LOAD-3e3096514ba34e12b51eab70c2ff3e65: 2,824 payments

Two event-load processes were observed during that diagnostic invocation. Their origin has not been established. Installed security software is a hypothesis, not a confirmed cause; no security settings were changed.

An HTTP response can be lost after an event commits. Interrupted lifecycles can therefore leave valid partial state or more committed events than acknowledged responses. Integrity OK does not prove all intended work succeeded, and a complete-lifecycle audit failure does not by itself prove corruption or lost acknowledged events.

Changes made:
- Detailed timeout/connect/transport error chains instead of generic errors.
- A bounded background operational-log writer, with overflow notices and a slow-sink regression test. This does not change durable event logging in SQLite.
- An independent timing-gap probe in the load client.
- Explicit workload IDs in new reports and a foreign_workload_payments check in the database audit. Other workloads are rejected, never filtered out to make a sample pass.
- Snapshot log analysis distinguishes request records from operational-log overflow notices.

Validation: API tests (33), benchmark tests (7), strict Clippy checks, and release builds passed. These code checks do not resolve the long-run environment/transport failures.

Next evidence needed: check security history for api.exe and event-load.exe around the failed runs. Do not disable protection or add exclusions based on this unconfirmed hypothesis.
