# Migration notes

## Version 0.3.0: source roles, delivery client, and schema 2

Stop the old service and create a verified backup before upgrading each event database. The updated maintenance backup command reads schema-1 databases without migrating them. API startup upgrades schema 1 to 2 transactionally, adding case state and append-only history tables while preserving event IDs and payment evidence. Existing records get a labelled baseline on first observation; no historical transitions are invented.

Older binaries reject schema 2. To roll back, stop the new service and restore the pre-upgrade backup to a fresh destination; changes accepted since that backup must be reconciled separately. Do not manually lower user_version. The maintenance verifier accepts both schemas. Ledger databases are separate and unaffected.

Legacy tenant api_key credentials remain administrators. Opt into separate source writer, auditor, and investigator credentials using the new credentials list, never both fields. Existing event and batch payloads remain compatible. The optional client feature adds the Rust HTTP client. See [integration guide](INTEGRATION.md) for roles, history routes, revision-based acknowledgement and the executable example.

## Operational endpoints and optional tenant mode

At that earlier milestone the schema remained version 1; version 0.3.0 now migrates it as described above. Existing single-tenant files, API payloads and routes remain compatible. New endpoints: GET /v1/discrepancies, GET /v1/storage-health, and GET /metrics. Tenant mode is opt-in via RECONCILE_TENANTS_FILE and requires dedicated database directories. See [operations](OPERATIONS.md) before moving existing data. The maintenance binary provides consistent backup, verification and non-overwriting restore.

## Package and executable rename

The Cargo package and default CLI executable are now named reconcile-cli (reconcile-cli.exe on Windows). Replace old hello_cargo executable invocations and Cargo package selections with reconcile-cli. Rust imports change from hello_cargo to reconcile_cli; path dependencies should use reconcile-cli as their dependency key.

The container CLI is now /usr/local/bin/reconcile-cli. The API binary remains api (reconcile-api in the container), and cargo run -- <internal.json> <provider.json> <report.json> still works. Payment formats, HTTP routes, and database paths are unchanged. Historical benchmark reports retain the paths recorded when those measurements were made.

## Migrating from the GBP-only prototype

Version 0.2.0 changes payment JSON and Rust types. Before upgrading, stop the old API. Replace each `amount_pence` input field with `amount_minor` and add `"currency":"GBP"` for the old GBP data. Evidence fields similarly change from `internal_amount_pence` / `provider_amount_pence` to `internal_amount_minor` / `provider_amount_minor`.

Every payment now requires currency. GBP, USD, EUR and JPY are supported; unsupported codes are rejected. Missing currency is rejected, never silently assumed. JPY uses whole yen; GBP/USD/EUR use hundredths. Do not merely relabel amounts from another currency.

Reports add internal_currency/provider_currency and metadata. Currency differences have four new result names: CurrencyMismatch, AmountAndCurrencyMismatch, StatusAndCurrencyMismatch, AmountStatusAndCurrencyMismatch. All recommend InvestigateCurrencyAndValues and count as one discrepancy per payment.

The bundled examples and tests are already migrated. Start the updated API after rebuilding. The old process cannot understand the new JSON. On Windows, stop it with Ctrl+C before running cargo test or cargo run --bin api.

The route remains `/v1/reconcile` during this pre-publication pilot. No external compatibility promise existed for the old prototype. Future released breaking changes should use a new API major version.