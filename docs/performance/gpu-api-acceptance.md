# GPU API acceptance record — 14 November 2026

Hardware: NVIDIA GeForce RTX 5070 Laptop GPU, Windows, DirectX 12; Intel i7-14650HX
host; Rust 1.99.1. This is a correctness/recovery acceptance record, not a durable
API throughput benchmark.

The live test ran successfully twice during integration. The final run completed
in **32.14 seconds**, using real discrete-GPU execution and isolated SQLite files.
It exercised one seed payment plus 1,000 additional payment IDs, several source
versions and multiple abrupt server restarts. The separate resident compute
benchmark's one-million-payment result is not presented as a million-payment
durable-recovery test.

Verified cases:

- Bearer authentication and GPU readiness; missing-payment 404.
- Refusal of a second process owning the same GPU journal.
- Acknowledged u64::MAX payment amounts preserved across forced restart.
- Exact request retry returns original sequence/receipt; changed request/event
  identity returns 409.
- GPU amount/status disagreement, stale update, and conflicting-version outcomes.
- Duplicate event/source pairs rejected before mutation.
- Injected SQLite insert failure **after GPU computation** returns 503 and stops
  subsequent GPU reads. Restart restores committed state; retry then succeeds.
- Server killed during an HTTP request with an uncertain outcome. Restart and
  stable-ID retry result in exactly one committed journal entry.
- Resident-capacity overflow returns 422 without registering partial IDs or
  changing the committed batch count.
- SQLite integrity check passes before deliberate corruption; tampered input
  checksum then prevents startup.

Portable checks also passed: 127 existing all-feature regression tests, two new
GPU queue/validation tests, strict all-feature/all-target Clippy, GPU-crate Clippy
and formatting. The GPU hardware test is ignored in ordinary portable runs and
was invoked explicitly; it does not silently fall back to CPU emulation.

Reproduce:

```powershell
cargo test --locked --all-features
cargo test --locked --features gpu,client --test gpu_api_recovery -- --ignored --nocapture
cargo clippy --locked --all-features --all-targets -- -D warnings
```

The test prints the evidence directory. Its final journal is intentionally corrupted
to exercise startup rejection, so it is not suitable as a demo database. Source
tests and this record are published; temporary databases and machine-local logs are
not committed.
