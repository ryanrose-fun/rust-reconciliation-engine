# Historical performance baseline: 10 November 2026

See [IMPROVEMENTS.md](IMPROVEMENTS.md) for the subsequent fixes and final measurements.

These are measurements from this Windows workstation (Intel Core i7-14650HX, 24 logical processors, Rust 1.99.1), not an industry ranking or a production capacity guarantee. No production reconciliation algorithm was changed.

## Comparable engine timings

100,000 payments per source, all matched, shuffled provider order. Mean of 15 timed samples after two warmups, with rotating execution order:

| Implementation | Typed inputs to full report and destruction | JSON input to JSON output and destruction |
|---|---:|---:|
| Current engine | 69.00 ms | 132.19 ms |
| Rust sorted merge baseline | 56.89 ms | 117.40 ms |
| SQLite load/index/join plus Rust report assembly | 256.31 ms | 320.35 ms |

All implementations produced equivalent outputs, including evidence, source fingerprints and ordering. Random report ID and timestamp were excluded from equality checks. SQLite timings include fresh in-memory database creation and ID loading; they are not measurements of a persistent or preloaded SQL system.

The sorted-merge baseline took about 18% less time than the engine at the typed boundary. That is evidence to investigate our repeated hashing, index lookups and allocations; it does not prove sorted merge wins on every workload. Internal inputs are already ordered in this fixture; provider inputs are shuffled. Test both sides shuffled and real customer ID distributions before choosing an algorithm.

Mixed discrepancies increased the engine's typed mean to 80.01 ms. Long IDs raised it to 234.31 ms. The JSON boundary at 100,000 matched payments was about 63 ms more expensive than the typed boundary. That difference includes parsing, serialization and extra allocation/destruction, rather than identifying any one function as the cause.

The previous 57 ms headline used a different fixture and timing boundary; these results do not demonstrate a regression.

## Full HTTP results

Authenticated HTTP/1.1 over loopback, with logging enabled, mixed findings and full validation of every successful response. Each batch contains two source arrays of the stated size. Capacity profile raises the rate cap to 10,000 requests/second while retaining server concurrency 8, body limit 1 MiB and input limit 5,000/source.

Useful clean runs at eight concurrent clients:

| Payments per source | Successful batches/s | Input records/s, both sources | p50 full-response latency | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| 100 | 284.7 | 56,949 | 26.6 ms | 37.9 ms | 57.3 ms |
| 1,000 | 262.7 | 525,401 | 27.0 ms | 36.5 ms | 93.6 ms |
| 5,000 | 127.2 | 1,272,465 | 47.9 ms | 81.0 ms | 154.7 ms |

These three runs had no rejected requests, transport errors or incorrect successful reports. They are reconciliation input-record rates, not financial transfer execution rates.

For 5,000/source at concurrency 8, sampled server working set peaked around 36.2 MiB and client working set around 43.8 MiB. Response traffic alone was about 182 MiB/second. Full client response validation added roughly 9.7 ms on average after HTTP receipt. Server middleware reported a mean around 14.7 ms, while the client measured about 52.9 ms to receive the complete response. Middleware timing excludes full response transmission and has integer-millisecond precision; this gap warrants HTTP/scheduling/transfer profiling, not a claim that all remaining time is network latency.

Increasing concurrency from 8 to 16 at 5,000/source only increased successful throughput from 127.2 to 142.0 batches/second, while p95 latency rose from 81.0 to 153.4 ms and eight client transport errors appeared. The matching server log recorded eight 503 responses. This is an unattractive tradeoff.

## Rate limits and overload findings

At 200 scheduled requests/second with the default 100 requests/second cap:
- 2,000 scheduled arrivals; 173 dropped locally by the generator's concurrency bound.
- 1,827 submitted; 1,000 successful, 522 readable HTTP 429 responses and 305 transport failures.
- Achieved success rate: 99.94 batches/second.
- Server logs show rate-limit rejections, but not all reached the client as readable HTTP responses.

A 15-second recheck at the default cap reproduced the issue: 1,500 successes, 476 readable 429 responses and 368 connection resets. The server recorded 844 rate-limit rejections, exactly the sum of readable rejections and resets. The client's underlying error was Windows socket error 10054: the remote host forcibly closed the connection.

This strongly suggests early rejection while an HTTP request body is still being uploaded. It is a diagnosis to verify in a focused regression test, not yet a production fix. Clients need predictable, readable 429/503 responses and retry guidance.

At 250 scheduled requests/second with the capacity profile, 2,318 requests succeeded, but 182 of 2,500 arrivals were dropped by the generator's concurrency limit. This achieved about 231 batches/second; it did not sustain the complete 250-request/second offered workload.

## Measurement anomalies and limits

The first small-batch run had more server log events than client submissions plus warmups; its resource observation also lasted longer than expected. The first case after rebuilding the diagnostic tool showed a similar excess. The cause is not established. These cases are retained and flagged rather than treated as clean capacity evidence. The small-batch recheck had matching counters but achieved only 61.9 batches/second at concurrency 1, versus the initial 93.3; workstation variability is material. The headline HTTP table uses the clean concurrency-8 cases.

Raw results include client_count_consistent. A lower server count can occur when a transport failure prevents a request from reaching the handler; an unexplained higher count needs investigation. Successful response correctness and request-status totals are checked independently.

Runs were 10 seconds, with 15-second diagnostic rechecks. p99 estimates remain exploratory. Client and server share the machine, and client JSON validation consumes CPU. CPU/memory observations include warmup and cooldown; the throughput denominator does not. This is snapshot reconciliation, not a streaming-event latency test. TLS, durable storage, remote networks and multiple tenants are outside this baseline.

## Recommended next work

1. Reproduce and fix readable overload responses for large POST bodies. Add a live HTTP regression test for 429 and 503 delivery; current in-process route tests cannot expose TCP resets.
2. Profile full HTTP request handling and output transfer. At 1,000/source, middleware is around 2.1 ms on average while full response receipt is around 28.9 ms. Measure before attributing the gap to matching.
3. Reduce repeated ID hashing/lookups and intermediate report allocations. Use the independent sort baseline as a comparison, and preserve evidence, fingerprinting and output semantics.
4. Measure compact/summary-only responses as an explicit optional API feature. The detailed report is valuable, but transferring it is expensive; never silently remove evidence.
5. Repeat longer runs on a stable machine, then use a separate load-generator host. Establish an acceptable p95/error budget before claiming capacity.

## Reproduce and inspect

See ../../perf/README.md for commands, configuration and exact boundaries.

- comparison-before.json: 72 comparison cases with raw timings and hardware metadata.
- http-load.json: all 14 original HTTP cases, including errors, resource samples and server observations.
- overload-recheck.json: 15-second diagnostic reruns with detailed connection causes.
- small-batch-recheck.json: small-batch counter/variability check.

Four benchmark correctness tests and strict Clippy checks passed. Release comparison and load tools built successfully. CI now includes those checks; hosted CI has not been run in this local task.
## All original HTTP cases

Transport errors include failed receipt of some early rejections. Server logs include warmups.

| Case | Profile | Payments/source | Clients | Mode | Success/s | p95 ms | Client statuses | Local drops | Server/client counts match |
|---:|---|---:|---:|---|---:|---:|---|---:|---|
| 1 | capacity | 100 | 1 | closed_loop | 93.3 | 17.8 | 200:934 | 0 | False |
| 2 | capacity | 100 | 4 | closed_loop | 211.6 | 35.6 | 200:2119 | 0 | True |
| 3 | capacity | 100 | 8 | closed_loop | 284.7 | 37.9 | 200:2854 | 0 | True |
| 4 | capacity | 100 | 16 | closed_loop | 283.9 | 68.8 | 200:2863 | 0 | True |
| 5 | capacity | 1000 | 1 | closed_loop | 35.8 | 41.1 | 200:359 | 0 | True |
| 6 | capacity | 1000 | 4 | closed_loop | 162.6 | 35.6 | 200:1628 | 0 | True |
| 7 | capacity | 1000 | 8 | closed_loop | 262.7 | 36.5 | 200:2633 | 0 | True |
| 8 | capacity | 1000 | 16 | closed_loop | 232.4 | 102.7 | 0:1, 200:2373 | 0 | True |
| 9 | capacity | 5000 | 1 | closed_loop | 18.8 | 70.6 | 200:188 | 0 | True |
| 10 | capacity | 5000 | 4 | closed_loop | 71.0 | 65.2 | 200:712 | 0 | True |
| 11 | capacity | 5000 | 8 | closed_loop | 127.2 | 81.0 | 200:1279 | 0 | True |
| 12 | capacity | 5000 | 16 | closed_loop | 142.0 | 153.4 | 0:8, 200:1431 | 0 | True |
| 13 | defaults | 1000 | 8 | scheduled_arrivals | 99.9 | 55.6 | 0:305, 200:1000, 429:522 | 173 | False |
| 14 | capacity | 1000 | 16 | scheduled_arrivals | 231.1 | 85.1 | 200:2318 | 182 | True |