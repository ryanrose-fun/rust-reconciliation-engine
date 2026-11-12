# Release preparation

The repository is MIT licensed. Cargo.lock pins dependencies. Current tested compiler: Rust 1.99.1. The crate name remains hello_cargo; choose a permanent product/package name before publishing to crates.io.

## Local checks

Stop the running API on Windows, then run:

```powershell
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
.\scripts\benchmark.ps1
.\scripts\package.ps1
```

The packaging script builds Windows x64 binaries in an isolated target directory, bundles docs, examples, data and licence, and creates a zip plus SHA-256 checksum in dist. The Windows archive includes SHA256SUMS for its files. A separate source archive includes the complete project and CI configuration, excluding Git internals, build outputs, and secrets. Hashes verify file integrity against a trusted checksum; they are not release signatures.

Regenerate the OpenAPI document when public types or routes change:

```powershell
cargo run --bin api -- --openapi
```

The output is pure JSON. Store it as UTF-8 `docs/openapi.json`; the API serves the same generated document. Contract tests validate actual responses against generated schemas.

## GitHub handoff

The local machine was not signed into GitHub when this candidate was prepared. No repository or remote release has been created or published. After the account is ready, sign in using `gh auth login`, create the intended repository, and push the reviewed source (excluding target, dist, secrets, and live transaction data).

CI files perform tests and linting on Windows/Linux, build the container, and run cargo audit. The manually dispatched release workflow produces a Windows archive artifact. It does not automatically publish to crates.io or a production environment. Review successful hosted checks before tagging and publishing a release.

The container build and GitHub workflows could not be executed locally without Docker and GitHub authentication. They must be verified after the environment is available. No production deployment or provider integration has been performed.