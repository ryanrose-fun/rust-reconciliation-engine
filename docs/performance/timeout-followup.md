# Timeout investigation — 10 November 2026

## Status

A reproducible admission-starvation defect has been fixed. The intermittent Windows transport stalls are still unresolved. Do not treat passing unit tests or an occasional passing load case as evidence that Windows sustained-load reliability has been established.

## Confirmed defect and change

The server previously rejected every event request that arrived while all eight handler slots were occupied. Busy clients could immediately take the next free slot, while rejected clients slept for Retry-After and lost the race again. The Linux 16-client control reproduced seven exhausted retry budgets without transport errors.

Event routes now enter bounded FIFO admission before obtaining handler capacity. The bound is concurrency + event queue capacity (40 by default), and only eight handlers execute at once by default. Admission and processing share the existing request deadline. Overflow still returns structured 503 with Retry-After. Snapshot routes retain immediate busy rejection. Cancellation releases waiting capacity; active SQLite work retains its permits until it finishes.

New regression tests cover FIFO ordering, overflow, cancellation, and a readable admission deadline response. SQLite WAL/FULL, commit-before-acknowledgement, idempotency, and validation remain unchanged.

## Windows stall evidence

The completed-operation resume probe captured a 0.1347 ms storage read followed by a 10,020.2347 ms delay before its async request resumed. The independent load-client thread reported no corresponding whole-process scheduling gap. A diagnostic 100 ms async interval also recorded a 10,017 ms wakeup gap.

Controlled experiments that did NOT reliably fix the stalls:
- Four runtime workers instead of the default count.
- A dedicated storage thread instead of per-operation spawn_blocking.
- A single-thread I/O runtime.
- A 100 ms periodic runtime wakeup.
- Tokio 1.53.3 instead of 1.54.1.

Those behavioral experiments and the dependency downgrade were reverted. The original runtime and lockfiles are retained. Body-reading and completion-resume timings remain as useful diagnostics.

Norton's aswhook.dll was observed loaded in both benchmark executables. This does not establish causation. No security settings were changed by this investigation. The final active-stall native capture was incomplete: SuspendThread failed for one thread with Windows error 5 (access denied). It did not establish the remaining blocking call. This is not proof that the inaccessible thread or Norton caused the stall. Diagnostic captures are not throughput measurements.

## Evidence

- timeout-resume-60s.json: reproduced delayed completion wakeups on Windows.
- timeout-four-workers-60s.json: failed thread-count experiment.
- dedicated-worker-60s.json: failed storage-thread experiment.
- single-io-loop-60s.json: one passing case and one failure.
- bounded-poll-60s.json: periodic wakeup did not remove stalls.
- tokio-1533-control.json: earlier runtime release did not reliably remove stalls.
- linux-timeout-control.json: Linux 8-client case passed; 16-client case exhausted retries, with no transport errors.
- fair-admission-windows-60s.json: final admission fix; Windows transport failures remain, without admission rejections.
- fair-admission-linux-60s.json: both final 60-second cases pass. Eight clients: 8,748 complete payments / 34,992 committed events. Sixteen clients: 11,952 complete payments / 47,808 committed events. Both have zero request errors, passing complete-lifecycle database audits, and successful restart lookups.

The PowerShell load runner now has an independent process wall-clock deadline (admission duration + 120 seconds) and a 10-second restart lookup timeout. Raw evidence is retained on failure. HTTP and retry deadlines were not increased.

The Linux control uses the same source and lockfiles, but a Linux filesystem and runtime. Its throughput is not a directly comparable Windows before/after result.

## Remaining work

Capture the specific blocked Windows I/O call in a verified active stall, then compare on an independent clean Windows environment. A loaded security DLL alone is insufficient evidence to blame security software. Windows reliability remains unverified until repeated sustained runs and restart audits pass without transport errors.

## Final checks

- 34 library unit tests, 37 API tests, 10 library integration tests, and 7 benchmark tests passed (88 total).
- Root and benchmark formatting checks and strict Clippy checks passed.
- Windows forced-restart recovery passed: acknowledged event, waiting deadline, exact amount, duplicate protection, and late counterpart reconciliation.
- Original dependency lockfiles were restored byte-for-byte.
- Final Windows sustained-load cases still failed with transport timeouts. No Windows reliability or throughput improvement is claimed.