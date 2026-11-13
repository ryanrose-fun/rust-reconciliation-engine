# Migration notes

## Package and executable rename

The Cargo package and default CLI executable are now named reconcile-cli (reconcile-cli.exe on Windows). Replace old hello_cargo executable invocations and Cargo package selections with reconcile-cli. Rust imports change from hello_cargo to reconcile_cli; path dependencies should use reconcile-cli as their dependency key.

The container CLI is now /usr/local/bin/reconcile-cli. The API binary remains api (reconcile-api in the container), and cargo run -- <internal.json> <provider.json> <report.json> still works. Payment formats, HTTP routes, and database paths are unchanged. Historical benchmark reports retain the paths recorded when those measurements were made.

## Migrating from the GBP-only prototype

Version 0.2.0 changes payment JSON and Rust types. Before upgrading, stop the old API. Replace each `amount_pence` input field with `amount_minor` and add `"currency":"GBP"` for the old GBP data. Evidence fields similarly change from `internal_amount_pence` / `provider_amount_pence` to `internal_amount_minor` / `provider_amount_minor`.

Every payment now requires currency. GBP, USD, EUR and JPY are supported; unsupported codes are rejected. Missing currency is rejected, never silently assumed. JPY uses whole yen; GBP/USD/EUR use hundredths. Do not merely relabel amounts from another currency.

Reports add internal_currency/provider_currency and metadata. Currency differences have four new result names: CurrencyMismatch, AmountAndCurrencyMismatch, StatusAndCurrencyMismatch, AmountStatusAndCurrencyMismatch. All recommend InvestigateCurrencyAndValues and count as one discrepancy per payment.

The bundled examples and tests are already migrated. Start the updated API after rebuilding. The old process cannot understand the new JSON. On Windows, stop it with Ctrl+C before running cargo test or cargo run --bin api.

The route remains `/v1/reconcile` during this pre-publication pilot. No external compatibility promise existed for the old prototype. Future released breaking changes should use a new API major version.