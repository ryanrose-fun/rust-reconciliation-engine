# Kafka to GPU streaming

This local integration sends independently produced bank/internal and provider
records through Apache Kafka to the existing durable GPU API. Reconciliation stays
on the GPU. Kafka transport, JSON handling and durability use host resources.

```text
Internal generator -> <run>-internal, partition 0 --+
                                                   +-> Rust consumer -> durable GPU API
Provider generator -> <run>-provider, partition 0 --+                  -> findings
```

## Broker

Use Apache Kafka 4.0.1 with Java 17+ or the official Docker image. The measured run
used Java 21 and Kafka in WSL, with the GPU API and Rust tools on Windows.
From the repository root, `wsl -d Ubuntu -- bash scripts/start-kafka-wsl.sh` starts an
isolated broker after verifying the pinned Apache archive checksum. Java must already
be installed in WSL. Keep that terminal open and use the broker address it prints.
The script stores data under the Linux user profile and does not delete existing logs.
See the [Apache quickstart](https://kafka.apache.org/quickstart/) for other platforms.

For Docker users, `docker compose -f deploy/kafka-compose.yaml up -d` provides a
local single-node broker on `127.0.0.1:19092`. The acceptance evidence was collected
with WSL, not Docker. If WSL localhost forwarding is unavailable, advertise its private
IP address and use that same address as the broker argument; do not change host
firewall settings or expose this plaintext lab publicly.

## Build and run

```powershell
cargo build --release --locked --features gpu,streaming --bin api --bin payment-stream
$broker = '127.0.0.1:19092' # or the broker's private WSL address
$run = 'demo-' + [guid]::NewGuid().ToString('N')
.\target\release\payment-stream.exe init $broker $run
```

Start the GPU API using [its guide](GPU_API.md). Set capacity to at least the expected
number of distinct payments and retain its data directory. Set the same
`RECONCILE_API_KEY` in the consumer terminal. Keep the API URL stable across restart.

In another terminal, substituting the same broker/run values:

```powershell
.\target\release\payment-stream.exe consume $broker $run http://127.0.0.1:3000 C:\path\to\cursor.sqlite3
```

The cursor directory must already exist; use local storage outside OneDrive. In two
producer terminals, run these commands with the same run ID:

```powershell
.\target\release\payment-stream.exe generate $broker $run Internal 90 2000 30 10 10000
.\target\release\payment-stream.exe generate $broker $run Provider 90 2000 30 10 10000
```

Arguments after the source mean: 90 seconds, 2,000 payments/source/second, a burst
starting at 30 seconds lasting 10 seconds at 10,000 payments/source/second. Generation
uses scheduled 100 ms ticks and records achieved timing/schedule lag. If publishing
falls behind, it catches up; configured rate is not automatically achieved rate.
Each Kafka record holds up to 512 same-source events. The consumer fetches bounded
chunks and alternates between topics without blocking on an empty source; it sleeps briefly only when both are idle. HTTP sends one Kafka record per request.

Internal emits Completed version 1. Provider emits Pending version 1 then final
version 2, with an old duplicate after selected completions. Every tenth payment has
an amount mismatch; every twentieth also has a status mismatch. These deterministic
synthetic rules are expected-result fixtures, not a model of real bank error rates.

## Inspect the live findings

The API remains the query interface; this version does not publish an output Kafka
topic. During or after generation, inspect a payment with the same run ID:

```powershell
$headers = @{ Authorization = "Bearer $env:RECONCILE_API_KEY" }
Invoke-RestMethod "http://127.0.0.1:3000/v1/gpu/payments/$run-0" -Headers $headers |
    ConvertTo-Json -Depth 10
```

After both sources finish, payment `0` has amount/status disagreements, payment `1`
matches, and payment `10` has an amount disagreement. Intermediate states can show
missing counterparts or pending/completed disagreement until the later evidence
arrives. Consumer latency metrics are per Kafka record; they are not counts of
unique final discrepancies. Original event evidence and GPU receipts remain in the
API journal.

## Delivery and recovery

The consumer stores exact request bytes and source offset in SQLite WAL/FULL before
HTTP delivery. Its request ID derives from topic/partition/offset. After a valid
GPU durable acknowledgement, cursor advancement and pending removal commit together.
A crash between those steps resends the exact request and receives the cached GPU
receipt. This is at-least-once delivery with idempotent application, not a distributed
exactly-once transaction between Kafka and SQLite.

One process owns the cursor database via an exclusive file lock. The database is
bound to broker, run and API URL. Do not reuse/delete topics within a run, reset a
cursor independently of its GPU state, or point the consumer at a fresh API journal
while retaining old cursors. A new dataset needs a new run ID. Retention gaps,
truncation, invalid input and permanent HTTP errors stop ingestion without skipping
records. Repair the cause and restart with the same cursor database.

Transient HTTP failures use bounded exponential backoff with jitter; after the retry
budget is exhausted, the pending record stays on disk and the process exits. Kafka
transport failures also stop the process without advancing its cursor. An operator
or supervisor must restart it. No records are silently sent to a dead-letter queue.
Consumer completion metrics commit atomically with the cursor and are also printed as JSONL. They contain record latency, attempts, duplicate acknowledgements and the
remaining record count observed at fetch time (not a global live lag measurement). Completion metrics are retained without pruning in this lab. Record latency ends at the API acknowledgement, before the local cursor commit; it includes Kafka waiting and API time. Generator publishing is at-least-once and can append duplicate Kafka records after an ambiguous broker response; stable event IDs prevent repeated state application.

Kafka uses two fixed partitions, no consumer group rebalance and no automatic worker
failover. This lab uses plaintext and replication factor 1. Production replication,
TLS/SASL, partition ownership/fencing and retention policy require further work. Kafka
broker acknowledgement is not a physical disk-loss guarantee. GPU and cursor FULL
commits do not make a single broker replicated storage.

## Automated acceptance

```powershell
$env:KAFKA_BROKER = $broker
$env:STREAM_TEST_SECONDS = '90'
$env:STREAM_TEST_RATE = '2000'
cargo test --release --locked --features gpu,streaming --test kafka_gpu_stream -- --ignored --nocapture
```

The test creates unique topics and temporary state, starts two synchronized generator tasks using the same library code as the CLI,
forces a consumer stop after an API acknowledgement but before cursor persistence,
kills/restarts the API while producers continue, drains the backlog and validates
final evidence after another restart. It retains JSONL measurements and a summary.
`PAYMENT_STREAM_TEST_PAUSE_AFTER_ACK_MS` plus `PAYMENT_STREAM_TEST_PAUSE_FILE` enable a bounded acceptance fault-injection hook;
leave it unset in normal use. Hardware tests are explicit; ordinary CI runs portable
checks without requiring a broker or GPU. See [results](performance/kafka-gpu-streaming.md).

The test stops its child API/consumer processes and completes its generator tasks. Kafka is operator-owned
and must be stopped separately. Evidence and topics are retained for inspection.
