# Operational readiness verification — 13 November 2026

## Delivered capabilities

Optional credential-selected tenant databases and independent admission limits; bounded discrepancy pagination with status/currency/source/age filters; tenant Prometheus metrics and storage backlog counts; consistent SQLite backups and non-overwriting restores; Linux SIGTERM handling; and portable sustained-load acceptance with forced-restart audits. See [operations](../OPERATIONS.md) for commands and limitations.

The database remains schema version 1. Durability is still WAL/FULL with acknowledgement after commit. No durability settings, HTTP deadlines or retry budgets were relaxed to obtain passing results.

## A confirmed drain race fixed

Earlier GitHub CI for commit d7d67f8 failed `cancellation_before_blocking_dispatch_skips_storage_work`: after `wait_idle`, one queue slot had not yet been released. The writer semaphore could become available before the operation dropped its other permits. `wait_idle` now acquires all admission slots, so it waits for every admitted operation rather than just writer ownership. A deterministic regression test holds an admission slot while the writer is free and verifies that shutdown still waits. This is distinct from the Windows runtime stalls.

## Windows: sustained reliability still fails

`operations-60s-round1.json` contains 60-second concurrency-8 and concurrency-16 runs. Both failed workload validation. The runtime wakeup metric recorded about 9.98 and 9.90 seconds of excess lag, while storage-busy errors were zero. Case 3 also contained 200 payments from an unexpected workload ID; it was rejected by the audit, not filtered out to obtain a passing result. The cause of that extra workload is unconfirmed. Both database integrity checks were OK, but incomplete lifecycles failed the expected-state/count audit.

Do not use these failed runs as capacity or improvement claims. An overloaded/concurrent Windows regression run also reproduced a connection reset during an in-limit rejected upload. The subsequent standalone regression suite passed. These observations do not establish a specific OS, antivirus, or engine root cause; a loaded security module is not proof of causation.

## Linux controls

`operations-linux-60s.json` contains two rounds at concurrency 8 and 16 on WSL2 with memory-backed `/tmp` (tmpfs). These are runtime controls, not persistent-disk durability or throughput measurements. All four 60-second cases passed: 943,028 unique committed events across 235,757 complete payment lifecycles, zero workload errors, full database audits, restart lookups and clean SIGTERM exits. The first controls preceded the final drain-race fix and tenant unauthorized-body reuse; those changes do not justify treating these measurements as a benchmark of the final binary. Post-drain-fix evidence is recorded separately in `operations-linux-final-60s.json`: two more 60-second cases on persistent ext4 storage passed with 93,356 committed events and 23,339 complete lifecycles, zero workload errors, passing audits/restarts and clean SIGTERM exits. An additional explicit flush in the maintenance backup command was tested separately after these API workloads.

These are same-host synthetic, closed-loop workloads, not independent client/server hardware, external banking traffic or a certification. Windows, Linux ext4 and Linux tmpfs throughput must not be compared as a before/after optimization. The runner now defaults evidence storage to the checkout target directory and records Linux filesystem type to make this distinction explicit. Report binary hashes identify the actual measured executables. SIGTERM checks occur after restart validation without an active workload; in-flight permit retention and cancellation are covered by queue tests.

## Regression coverage

Final integrated Windows suite: 38 library unit tests, 41 API tests, 10 library integration tests, one maintenance CLI integration test, four concurrently added ledger integration tests and seven benchmark tests (101 total). Strict Clippy and formatting checks pass. The maintenance CLI test verifies online backup of acknowledged WAL data, restored exact u64 amounts, idempotent retries, refusal to overwrite, and rejection of corrupt input. Tenant tests cover identical IDs with different amounts in separate databases, unauthorized access, restart recovery, and invalid filters. Bounded pagination tests cover empty pages with continuation, matching/waiting exclusion and source/currency/age filtering.

The independent hosted workflow `Sustained event acceptance` runs the same portable workload on Windows and Linux. Passing controls narrow the investigation; they do not erase reproducible failures on another host. The remaining Windows stall is an explicit release limitation, not marked fixed.

## Independent hosted verification

Published code commit: 7dc4f28f0a0f0417dd100775d39b9faa8655d09a. [Standard CI](https://github.com/ryanrose-fun/rust-reconciliation-engine/actions/runs/43399307547) passed Windows/Linux tests, strict lints, Docker build and dependency audit. The concurrent engineering-decisions documentation and ledger feature were preserved and tested before publication.

[Hosted soak workflow](https://github.com/ryanrose-fun/rust-reconciliation-engine/actions/runs/43399308964) passed two rounds each at concurrency 8 and 16 on both Windows and Linux: eight 60-second cases total. Linux (ext4) committed 844,028 unique events across 211,007 complete lifecycles. Hosted Windows committed 323,660 unique events across 80,915 complete lifecycles. All cases had zero workload errors, matching full database audits and successful restart lookups. Linux SIGTERM exit codes were zero; Windows forced termination is not treated as a graceful shutdown test.

Raw reports: operations-hosted-linux-60s.json and operations-hosted-windows-60s.json. They retain binary hashes and operating-system information. Windows filesystem type was not independently verified by the portable runner. These are synthetic same-host measurements, not a comparative platform benchmark or a production SLA.

The passing hosted Windows controls narrow the remaining local investigation but do not establish the cause of the local 10-second stalls or unexpected workload. No security settings were changed. Local Windows sustained reliability remains unresolved; do not infer that it was fixed simply because another machine passed.
