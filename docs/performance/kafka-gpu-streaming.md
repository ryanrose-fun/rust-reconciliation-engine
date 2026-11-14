# Kafka to GPU: sustained flow, burst and restart

Measured on 14 November 2026 with an RTX 5070 Laptop GPU, i7-14650HX,
Windows Rust processes and Apache Kafka 4.0.1 in WSL. One local broker,
replication factor 1, two topics with one partition each. These are local
integration measurements, not bank deployment capacity or a maximum-throughput test.

## Workload and results

Both runs used the same deterministic 1,100,000 payments: 3,300,000 unique source
events and 3,410,000 delivered events including deliberate duplicates, carried in
6,820 Kafka records. Two synchronized generator tasks used the production Rust
generator code. Each source generated 5,000 payments/s for 180 seconds, with a
10-second burst at 25,000 payments/s starting at second 60. Provider payments
produce pending and final updates. Amount/status disagreements are deliberate.

At approximately second 71, the consumer was killed after a durable API ACK but
before committing its cursor. The API was also killed. Producers continued during
the outage; after a three-second pause the API and consumer were restarted. A
second API restart after draining checked final recovery.

| Measurement | Initial polling | Revised polling |
|---|---:|---:|
| Time from generation start to all cursors durable | 306.49 s | 188.10 s |
| Drain after producers finished | 126.58 s | 8.19 s |
| Unique source events / total elapsed second | 10,767 | 17,544 |
| Whole-run record latency p50 | 45.80 s | 15.34 s |
| Whole-run record latency p95 | 122.96 s | 51.84 s |
| Whole-run record latency p99 | 126.13 s | 55.09 s |
| Mid-stream API restart to readiness | 4.06 s | 4.11 s |
| Validated | yes | yes |

Elapsed time fell 38.6%; effective event throughput rose 62.9%. These are single
runs, not statistical confidence bounds. The fixed arrival schedule limits the
meaning of the throughput figure. Payment throughput is about 5,848 completed
payment lifecycles per elapsed second in the revised run; events are not separate
financial transfers.

The initial consumer waited up to 100 ms on an empty source before reading the
other source. It now polls both without that wait and sleeps 10 ms only when
neither makes progress. GPU reconciliation and FULL journal durability were
unchanged. URL validation and Kafka connection retry deadlines were also hardened;
the final snapshot sample was diversified outside the ingestion timing window.

## Latency and bottlenecks

In the revised run, records created before the burst had p50 **110 ms** and p95
**1,474 ms** latency. Burst-created records had p95 **53,772 ms**; records created
after the burst had p95 **52,053 ms**. The burst and forced outage overlap in their
effects, so these figures do not isolate either cause.

Record latency runs from envelope creation to durable API acknowledgement and
includes Kafka waiting, retries and API work. It ends before the local cursor
commit. It is per Kafka record, not a per-payment latency histogram. HTTP time
alone had p50 **13.47 ms**, p95 **33.26 ms**. Much of the tail is queueing upstream.

The consumer deliberately has one outstanding GPU request and commits its durable
cursor after each record. Bounded concurrency across independently ordered sources,
and carefully measured batching of cursor commits, are the next experiments.
Neither may acknowledge work before the GPU journal commits or lose the exact
pending request on restart. Generator serialization, Kafka transport, journal I/O,
GPU dispatch and checkpointing all remain part of the end-to-end budget.

The final GPU-worker startup took **6.79 s**, including initialization and recovery;
the GPU replay compute portion was **82.09 ms** for 164 batches after the checkpoint.
This is not the complete external process-to-readiness interval. Improving shader
speed alone will not remove storage and state reconstruction costs.

## Correctness and recovery evidence

- Every Kafka record reached a durable cursor; no pending record was skipped.
- The forced ACK/cursor ambiguity produced a cached duplicate acknowledgement.
- Durable GPU event identity counts equalled 3,300,000 unique events.
- Request counts matched actual Kafka high-watermarks, including any broker retries.
- 2,001 final payment snapshots were checked after restart against expected amounts,
  statuses and source versions. The revised run samples mixed outcomes.
- SQLite integrity checks passed. Both full runs passed their assertions.

These tests cover process crashes, not power failure, disk loss, broker failover or
multi-node ownership. Kafka RF1 is not replicated durability. Full local database
evidence remains outside Git; no payment database or credentials are published.

## Reproduce

Follow the [streaming setup](../KAFKA_STREAMING.md), then:

```powershell
$env:KAFKA_BROKER = '<broker-host>:19092'
$env:STREAM_TEST_SECONDS = '180'
$env:STREAM_TEST_RATE = '5000'
cargo test --release --locked --features gpu,streaming --test kafka_gpu_stream -- --ignored --nocapture
```

Allow time for process startup, backlog draining and recovery validation in addition
to the 180-second generation window. The test prints its retained evidence directory.

Raw reports: [initial](kafka-gpu-streaming-baseline.json),
[revised](kafka-gpu-streaming.json). Source/build hashes and environment:
[initial provenance](kafka-gpu-streaming-baseline.provenance.json),
[revised provenance](kafka-gpu-streaming.provenance.json).
