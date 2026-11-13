# Reliability and performance engineering scope

This iteration implements the three requested improvements: readable overload responses for bounded uploads, HTTP/JSON stage profiling, and fewer matching/hash/report allocations. It does not certify this software for banking use.

Verified locally:
- Existing reconciliation and API contract tests remain applicable.
- Real TCP tests exercise 900 KB rejected bodies with Content-Length and chunked framing.
- Busy and rate-limit responses contain complete JSON, Retry-After and a request ID.
- Rejection byte, time and simultaneous-drain bounds are tested.
- Independent Rust and SQLite implementations compare full output, including source fingerprints, against the optimized engine.
- Seeded tests vary ordering, duplicate/error combinations, long IDs and empty inputs.
- Auth, size/count limits, malformed input, exact u64 values, currency distinctions and combined findings remain covered.
- Strict Clippy checks and release builds are part of verification.
- Before/after binaries and raw benchmark observations are retained locally.

Important limits:
- Reconciliation compares submitted snapshots or stored source events; it neither authenticates their origin nor executes settlement.
- Fingerprints are evidence of input content, not a durable or tamper-proof audit store.
- Optional tenant mode isolates database namespaces, credentials and rate limits; source-scoped permissions and user management remain unimplemented.
- Snapshot work after an HTTP timeout may be discarded. Event writes are atomic and durable with event-ID retry protection and status lookup; a timeout may still commit, so retry the same event. No replicated job service or high-availability storage exists.
- The deployment still needs a defined threat model, independent security assessment, credential lifecycle, TLS/proxy configuration, tenant/access controls, audit retention, recovery objectives, monitored service-level targets, long-duration capacity/failure tests and a release/rollback process.
- Hosted CI, Linux deployment, multi-host performance and disaster-recovery drills are not established by local Windows tests.
- Reports are operational reconciliation findings, not guarantees of payment correctness or regulatory compliance.

These are release-readiness requirements to define with a prospective customer and operator. Do not label this pilot "bank-grade", certified, or production-approved based only on unit tests and short load runs.
Continuous event verification covers version conflicts, exact retries, stale updates, deadline boundaries, database rollback/reopening, authenticated HTTP routes, and forced process restart. This does not establish power-loss resilience on customer hardware or sustained event throughput.
