# Bounded queue: measured before/after results

Local run: 10 November 2026. Each case admitted work for 15 seconds and included completion/drain time in throughput. Same optimized load-client binary, same workload, same authentication and limits, fresh SQLite WAL/FULL database per case. Server and client shared this Windows machine.

Raw evidence:
- [Before: concurrency 1, 4, 8](event-queue-before.json)
- [Before: concurrency 16 and default rate limit](event-queue-before-high.json)
- [After: all five cases](event-queue-after.json)
- [Comparison with binary hashes](event-queue-comparison.json)
- [Earlier failed before run at concurrency 16](event-queue-before-failed-16.json)

## Results

p99 includes rejected attempts and retry waiting for each logical POST. Unique committed events include retained stale updates but exclude duplicate retries.

| Profile | Clients | Before events/s | After events/s | Ratio | Before p99 | After p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Capacity | 1 | 52.7 | 61.9 | 1.17x | 18.8 ms | 19.1 ms |
| Capacity | 4 | 58.6 | 185.1 | 3.16x | 1,033 ms | 84.8 ms |
| Capacity | 8 | 88.5 | 470.9 | 5.32x | 1,031 ms | 32.6 ms |
| Capacity | 16 | 82.6 | 597.1 | 7.23x | 2,042 ms | 67.1 ms |
| Default rate limit | 8 | 54.1 | 59.7 | 1.10x | 2,039 ms | 1,043 ms |

The queue removed storage_busy rejections in every after case. The first three after cases had no rejections. At 16 clients, 85 global request-capacity busy responses remained because request concurrency stays at 8. At the default 100 requests/second limit, 112 rate_limited responses remained. These responses were readable and retried successfully.

The 16-client p99 does not capture every retry delay: the remaining busy responses were uncommon enough to fall outside that percentile. Use the raw maximum and rejection counts alongside percentiles.

All five after cases passed:
- 5,307 complete payment lifecycles.
- 21,228 unique acknowledged event-log records.
- Expected duplicate and stale-event behaviour.
- Exact latest source values and versions.
- Database integrity and count checks after forced shutdown.
- Status lookup after restarting each isolated API.

An additional forced-restart test passed, preserving the waiting deadline, duplicate identity and maximum u64 amount. Rust checks covered FIFO order, queue overflow, cancellation before dispatch, cancellation during active work, worker-panic cleanup, concurrent retries and readable queue-full HTTP errors.

## Where transaction time goes

Applied updates at concurrency 8, measured inside the new API:

| Stage | Mean | p95 |
| --- | ---: | ---: |
| Queue waiting | 0.84 ms | 2.76 ms |
| Blocking-worker dispatch | 0.01 ms | 0.03 ms |
| SQLite transaction begin | 0.03 ms | 0.05 ms |
| SQL work and state reconstruction | 0.10 ms | 0.17 ms |
| SQLite commit | 0.79 ms | 0.96 ms |
| Complete blocking service | 0.93 ms | 1.17 ms |

Preparation and response encoding each averaged less than 0.01 ms. Service contains transaction stages and decoding; it must not be added to them as if independent.

For those same applied updates, full successful HTTP attempts averaged 11.29 ms. Queue plus storage timing does not explain the full response delay. The remaining time can include async scheduling/wakeups, middleware/logging, HTTP processing and transport. These measurements do not isolate those components, so neither SQLite nor JSON should be declared the sole bottleneck.

Commit is the largest measured storage component, but reducing commit durability is not justified by this result. WAL and synchronous=FULL were unchanged; every event still commits individually.

## Limits of the comparison

These are short local closed-loop samples, not a guaranteed production capacity or latency target. Single-client p99 did not improve. Default-rate performance remains constrained by the intentional rate limit.

One initial before sample contained two distinct workload IDs in the database while the client reported one run; its audit rejected it and it is excluded. Another before run at concurrency 16 had two transport failures and eleven exhausted retry budgets; that failed sample is retained above. The separate repeated high-load run passed and supplies the table's before values. These failures and variation are reasons to run repeated trials and longer soak/fixed-arrival tests, not to advertise the best ratio as universal.

The load runner now retains failed cases in its JSON report, continues other cases, and exits unsuccessfully if any case fails validation. An unsuccessful case is not evidence of valid throughput simply because it contains a rate measurement.

## Use the change

Stop your existing API with Ctrl+C, then:

~~~powershell
cargo run --bin api
~~~

To repeat the benchmark:

~~~powershell
.\scripts\load-test-events.ps1 -Seconds 15 -ReportName my-queued-event-test.json
~~~

See [queue semantics and timing definitions](../EVENT-QUEUE.md) for configuration and cancellation behaviour.
