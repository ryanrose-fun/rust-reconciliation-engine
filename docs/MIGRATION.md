# Migrating from the GBP-only prototype

Version 0.2.0 changes payment JSON and Rust types. Before upgrading, stop the old API. Replace each `amount_pence` input field with `amount_minor` and add `"currency":"GBP"` for the old GBP data. Evidence fields similarly change from `internal_amount_pence` / `provider_amount_pence` to `internal_amount_minor` / `provider_amount_minor`.

Every payment now requires currency. GBP, USD, EUR and JPY are supported; unsupported codes are rejected. Missing currency is rejected, never silently assumed. JPY uses whole yen; GBP/USD/EUR use hundredths. Do not merely relabel amounts from another currency.

Reports add internal_currency/provider_currency and metadata. Currency differences have four new result names: CurrencyMismatch, AmountAndCurrencyMismatch, StatusAndCurrencyMismatch, AmountStatusAndCurrencyMismatch. All recommend InvestigateCurrencyAndValues and count as one discrepancy per payment.

The bundled examples and tests are already migrated. Start the updated API after rebuilding. The old process cannot understand the new JSON. On Windows, stop it with Ctrl+C before running cargo test or cargo run --bin api.

The route remains `/v1/reconcile` during this pre-publication pilot. No external compatibility promise existed for the old prototype. Future released breaking changes should use a new API major version.