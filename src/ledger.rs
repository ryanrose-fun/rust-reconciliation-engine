//! Transactional double-entry journal. Positive amounts are debits, negative amounts credits.
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Posting {
    pub account: String,
    pub currency: String,
    /// Integer minor units; the caller must use a consistent currency exponent.
    pub minor_units: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Journal {
    pub reference: String,
    pub description: String,
    pub postings: Vec<Posting>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Recorded,
    AlreadyRecorded,
}

#[derive(Debug)]
pub enum Error {
    Invalid(&'static str),
    ReferenceConflict,
    Storage(rusqlite::Error),
    Encoding(serde_json::Error),
}
impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        Self::Storage(value)
    }
}
impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Encoding(value)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "Invalid journal: {reason}"),
            Self::ReferenceConflict => {
                write!(f, "Reference already used for different journal content")
            }
            Self::Storage(error) => write!(f, "Ledger storage: {error}"),
            Self::Encoding(error) => write!(f, "Ledger encoding: {error}"),
        }
    }
}
impl std::error::Error for Error {}

pub struct Ledger {
    connection: Connection,
}

impl Ledger {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS financial_journal (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                reference TEXT NOT NULL UNIQUE,
                recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                payload TEXT NOT NULL);
            CREATE TRIGGER IF NOT EXISTS journal_no_update BEFORE UPDATE ON financial_journal
                BEGIN SELECT RAISE(ABORT, 'Journal is append-only'); END;
            CREATE TRIGGER IF NOT EXISTS journal_no_delete BEFORE DELETE ON financial_journal
                BEGIN SELECT RAISE(ABORT, 'Journal is append-only'); END;",
        )?;
        Ok(Self { connection })
    }

    /// Commit a balanced journal atomically, or return a replay/conflict result.
    /// Posting order is part of the idempotency payload. Retry the identical request.
    pub fn post(&mut self, journal: &Journal) -> Result<Outcome, Error> {
        validate(journal)?;
        let payload = serde_json::to_string(journal)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<String> = transaction
            .query_row(
                "SELECT payload FROM financial_journal WHERE reference=?1",
                [&journal.reference],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            return if existing == payload {
                Ok(Outcome::AlreadyRecorded)
            } else {
                Err(Error::ReferenceConflict)
            };
        }
        transaction.execute(
            "INSERT INTO financial_journal(reference,payload) VALUES (?1,?2)",
            params![journal.reference, payload],
        )?;
        transaction.commit()?;
        Ok(Outcome::Recorded)
    }

    /// Ordered source records, rather than a mutable balance cache.
    pub fn journals(&self) -> Result<Vec<Journal>, Error> {
        let mut statement = self
            .connection
            .prepare("SELECT payload FROM financial_journal ORDER BY sequence")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn balances(&self) -> Result<BTreeMap<(String, String), i128>, Error> {
        let mut balances = BTreeMap::new();
        for journal in self.journals()? {
            for posting in journal.postings {
                let balance = balances
                    .entry((posting.account, posting.currency))
                    .or_insert(0_i128);
                *balance = balance
                    .checked_add(i128::from(posting.minor_units))
                    .ok_or(Error::Invalid("balance exceeds supported range"))?;
            }
        }
        Ok(balances)
    }
}

fn validate(journal: &Journal) -> Result<(), Error> {
    if journal.reference.trim().is_empty()
        || journal.reference.len() > 128
        || journal.description.len() > 2000
    {
        return Err(Error::Invalid("reference or description length"));
    }
    if !(2..=1000).contains(&journal.postings.len()) {
        return Err(Error::Invalid("requires 2..1000 postings"));
    }
    let mut totals = BTreeMap::new();
    for posting in &journal.postings {
        if posting.account.trim().is_empty()
            || posting.account.len() > 128
            || posting.minor_units == 0
        {
            return Err(Error::Invalid("account or zero amount"));
        }
        // Explicit supported currency set: all three use two decimal minor units.
        if !matches!(posting.currency.as_str(), "GBP" | "USD" | "EUR") {
            return Err(Error::Invalid("supported currencies are GBP, USD and EUR"));
        }
        *totals.entry(&posting.currency).or_insert(0_i128) += i128::from(posting.minor_units);
    }
    if totals.values().any(|total| *total != 0) {
        return Err(Error::Invalid(
            "debits and credits must balance per currency",
        ));
    }
    Ok(())
}
