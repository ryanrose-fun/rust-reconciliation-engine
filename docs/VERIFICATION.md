# Pilot candidate verification

Version 0.2.0 was verified locally on Windows with Rust 1.99.1.

Completed checks:

- 50 passing tests: 24 core, 16 API/config/contract, 10 library integration.
- cargo fmt --check and strict clippy across all targets.
- Optimized release builds for API, CLI, and benchmark.
- Real HTTP tests on a temporary loopback port: rejected missing bearer token, accepted valid token, combined amount/status/currency finding, source labels, stable input hashes, distinct report IDs, health, and OpenAPI.
- Windows PowerShell 5.1 example script against the authenticated server.
- Request-log inspection confirmed test payment IDs, source labels, and token were absent.
- File-based CLI report generation with migrated examples.
- Generated OpenAPI compared to its checked-in snapshot and actual response schema validation.
- Synthetic performance baseline with latency, throughput and process memory measurements; see benchmark-results.json.
- cargo-audit 0.22.2 against the fetched RustSec database: zero known vulnerabilities, no warnings. Raw results are in dependency-audit.json; this is a point-in-time check, not a security certification.
- Windows executable and full-source archives with verified SHA-256 archive checksums.

External checks still required:

- GitHub account authentication, repository publication, and execution of hosted Windows/Linux CI.
- Docker image build/run verification; Docker was not installed on the development machine.
- HTTPS/proxy, secret management, retention, load testing and operational acceptance on the intended deployment environment.

The service is ready for local evaluation of a pilot candidate, not declared production-certified. No GitHub remote release, crates.io publication, hosted deployment, or real bank integration has been performed.