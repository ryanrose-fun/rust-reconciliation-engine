# Rust financial ledger: settlement journal

A runnable engineering example of transactional double-entry posting. This complements reconciliation: reconciliation identifies differences between sources; a journal records the accounting movements themselves. Neither module automatically posts the other's findings.

## Run it

```sh
cargo run --example ledger -- ledger-demo.db
cargo run --example ledger -- ledger-demo.db
cargo test --test ledger
```

The first run reports `Recorded`; the second reports `AlreadyRecorded`. Both show the same balances. The example represents settlement of a GBP 100 receivable, receiving GBP 99.75 cash and recognising a GBP 0.25 processing fee:

| Account | Debit (minor units) | Credit (minor units) |
| --- | ---: | ---: |
| cash | 9975 | |
| processing-fees | 25 | |
| merchant-receivable | | 10000 |

Positive values mean debit; negative values mean credit. This is a journal convention, not a statement that positive balances always represent available funds. Amounts use signed integer minor units, never floating-point arithmetic. Supported currencies are explicitly GBP, USD and EUR, each with two decimal places. Cross-currency transfers must balance independently in each currency; FX conversion and rounding policy are outside this example.

## Posting path

1. Validate bounded reference, description and posting counts; reject missing accounts, zero amounts and unsupported currencies.
2. Require debits and credits to sum to zero **per currency**, using i128 totals to avoid i64 summation overflow.
3. Start SQLite `BEGIN IMMEDIATE`, serialising writers across independent connections.
4. Look up the unique reference. An identical serialised payload is a safe replay; a changed payload is a conflict. Posting order is significant.
5. Insert the entire journal as one row, then commit before returning success. WAL and synchronous FULL are enabled. Storage errors propagate to the caller.

After a lost response, retry the identical reference and payload. A timeout does not justify generating a fresh reference. There is no external payment execution, so this does not claim exactly-once processing across a bank or provider boundary.

## Audit and correction

The database retains a sequence, UTC recording timestamp, business reference and full posting payload. UPDATE/DELETE triggers prevent ordinary mutations. Corrections are additional reversing journals, leaving the original visible. The regression test demonstrates a reversal and preserved original history.

These controls are **not tamper-proof**: a database administrator can drop triggers or replace the file. Reversal references are descriptive only; there is no enforced reversal linkage or single-reversal constraint. Production audit requirements need authorised actors, reason codes, access separation, retention controls and external audit evidence.

## What this demonstrates—and where it stops

- Atomic posting, persistent idempotency and deterministic balance reconstruction.
- Independent-connection concurrency tests and reopen verification; no power-loss simulation or distributed failover testing.
- A five-second SQLite busy timeout, not an unbounded retry loop.
- Balances are derived by reading journals. This is clear and auditable for a sample, but grows linearly in memory and time. A larger service needs indexed posting rows, transactional balance projections, reconciliation of projections, pagination and migrations.
- No account registry, tenant boundaries, authorisation, overdraft control, reservations, settlement calendar, interest, FX, or regulatory certification. Negative balances are allowed.
- No HTTP endpoint is exposed for this module. Integrating it with a financial service requires authenticated commands and account-level permissions; do not expose database access directly.
- SQLite is a deliberate single-host demonstration. Multi-region operation and high write throughput need a different storage and operational design.

## Tests

`tests/ledger.rs` covers replay versus changed-reference conflicts, invalid journal rejection without partial persistence, independent currency balancing, reversal, concurrent duplicate posting through separate connections, reopen persistence and direct UPDATE/DELETE rejection. The demo uses synthetic data only.
