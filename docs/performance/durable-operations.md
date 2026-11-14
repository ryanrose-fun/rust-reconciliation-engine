# Durable operations: measured 0.4.0 results

Measured on 2026-11-13 at commit `baedaa195702057cb6418779ea12e83d3b6f8019`, Rust 1.99.1, optimized release builds. [Hosted measurement run](https://github.com/ryanrose-fun/rust-gpu-finance/actions/runs/43409871712). [Source CI](https://github.com/ryanrose-fun/rust-gpu-finance/actions/runs/43409871075) and [pull-request CI](https://github.com/ryanrose-fun/rust-gpu-finance/actions/runs/43409883902) passed on Windows and Linux, including tests, Clippy, the live PowerShell walkthroughs, and forced-restart recovery. The dependency audit and container build also passed.

Local automated coverage: **119 main-project tests and 7 benchmark-harness tests**. The new failure-path tests cover lease expiry/fencing, exhausted retries, restart delivery, atomic business-row/outbox commit, long Retry-After persistence, webhook signature tampering, notification enqueue rollback, automatic deadlines, stale assignment updates, archival corruption and restore equality.

## Full HTTP event API

Two 60-second rounds at concurrency 8 and 16 on each platform: **eight valid cases**, no workload errors, all complete-lifecycle database audits and restart lookups passed. Each case uses a fresh disk-backed SQLite WAL/FULL database. Linux reported ext4; the portable runner did not identify the Windows volume's filesystem explicitly. The client and API share the runner and communicate over loopback. This is a closed-loop capacity test; it does not reproduce an external production arrival distribution.

Ranges below are the two rounds, not confidence intervals. Throughput is unique committed event IDs per second, including retained stale events; it is not completed payments per second. Successful POST latency includes duplicate/stale receipts as well as applied updates. Complete raw results also retain logical retry latency and status-read latency.

| Platform | Concurrency | Committed events/s | Successful POST p95 | Successful POST p99 |
|---|---:|---:|---:|---:|
| Hosted Linux | 8 | 1,382–1,392 | 7.85–7.87 ms | 9.21–9.25 ms |
| Hosted Linux | 16 | 1,285–1,317 | 15.07–15.65 ms | 16.67–17.78 ms |
| Hosted Windows | 8 | 1,022–1,067 | 17.32–17.90 ms | 19.60–20.45 ms |
| Hosted Windows | 16 | 977–980 | 29.51–29.55 ms | 31.78–32.34 ms |

Raw evidence: [Linux HTTP](durable-hosted-linux-http.json), [Windows HTTP](durable-hosted-windows-http.json). These cases use the normal event API with history and automatic deadlines enabled; external webhook delivery is not enabled in this HTTP workload.

Increasing concurrency from 8 to 16 reduced throughput and increased latency on both runners. More simultaneous callers did not increase the capacity of the single SQLite writer. Start with bounded concurrency near 8 for a similar workload, then measure on the actual deployment hardware and arrival pattern.

## Storage and durable notification enqueue cost

Three rounds per configuration, 1,000 payments and 3,000 committed events per fresh database: internal completion, provider pending, provider completion. Every case verified 3,000 history entries; notification-enabled cases additionally verified 2,000 durable jobs. These measurements include event construction and synchronous storage, but exclude HTTP and network webhook delivery.

| Platform | Notification enqueue | Events/s range | Event p95 range | Event p99 range |
|---|---|---:|---:|---:|
| Linux | Disabled | 1,843–2,291 | 0.68–0.81 ms | 1.85–1.94 ms |
| Linux | Enabled | 1,591–1,847 | 0.80–0.87 ms | 2.50–2.61 ms |
| Windows | Disabled | 1,731–1,809 | 0.68–0.73 ms | 3.00–3.24 ms |
| Windows | Enabled | 1,522–1,558 | 0.76–0.78 ms | 4.51–4.67 ms |

On Linux without notifications, median transaction work was 0.075–0.079 ms while median commit was 0.319–0.364 ms. Enqueueing notifications raised median transaction work to 0.105–0.107 ms and median commit to 0.402–0.414 ms. Durable commit cost is a larger optimization target than the comparison itself in this workload. Shared-runner storage noise remains visible: individual storage events exceeded 100 ms even though p99 was much lower. These short storage runs are diagnostic, not sustained-load capacity claims.

For this fixture, the database plus WAL/sidecar footprint was approximately 7.32 MiB without queued notifications and 9.10–9.13 MiB with them. This includes allocated WAL space and is one workload size, not a long-term growth curve or a universal bytes-per-payment estimate. Retained history and delivery identities require an explicit capacity/retention plan.

Raw evidence: [Linux storage](durable-hosted-linux-storage.json), [Windows storage](durable-hosted-windows-storage.json).

## Deadline, queue-query and recovery checks

| Operation | Linux | Windows |
|---|---:|---:|
| Process 100 due deadlines, single measured batch | 3.66 ms | 5.83 ms |
| Read first 50 active cases, single measured query | 0.16 ms | 0.27 ms |
| Backup, restore and compare logical contents, six small fixtures | 201–314 ms | 282–403 ms |

Every recovery drill passed. These fixtures are small; the restore figures are not a recovery-time objective for a large production database. The API's deadline loop runs at 500 ms intervals with a 100-record batch limit, so the measured batch time is not the maximum wall-clock delay before an alert. Under backlog, observations can be later while preserving the original effective expiry time.

## What to improve next, based on the evidence

1. Measure storage commit latency on dedicated deployment hardware before changing durability settings. Keep WAL/FULL; do not trade away commit guarantees to improve a headline number.
2. Investigate bounded group commit with explicit receipt-after-commit semantics if greater write throughput is required. It needs separate crash, ordering and idempotency tests before adoption.
3. Benchmark webhook receiver throughput and large active-case/history volumes separately. This run measures durable enqueue costs and functional delivery correctness, not external receiver capacity.
4. Extend tests to hours and larger databases, recording disk growth and memory. These results validate the tested windows and workloads; they do not establish banking certification, multi-region failover or billion-event capacity.

No speedup over an older release is claimed: new durable history, deadlines and notification jobs change the work, and hosted runners are not a controlled before/after hardware comparison.
