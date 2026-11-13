use reconcile_cli::ledger::{Error, Journal, Ledger, Outcome, Posting};

fn payment() -> Journal {
    Journal {
        reference: "settlement-001".into(),
        description: "Merchant settlement less fee".into(),
        postings: vec![
            Posting {
                account: "cash".into(),
                currency: "GBP".into(),
                minor_units: 9975,
            },
            Posting {
                account: "fees".into(),
                currency: "GBP".into(),
                minor_units: 25,
            },
            Posting {
                account: "receivable".into(),
                currency: "GBP".into(),
                minor_units: -10000,
            },
        ],
    }
}

#[test]
fn balanced_settlement_replay_conflict_and_reversal() {
    let mut ledger = Ledger::open(":memory:").unwrap();
    let journal = payment();
    assert_eq!(ledger.post(&journal).unwrap(), Outcome::Recorded);
    assert_eq!(ledger.post(&journal).unwrap(), Outcome::AlreadyRecorded);
    let mut changed = journal.clone();
    changed.description = "Different intent".into();
    assert!(matches!(
        ledger.post(&changed),
        Err(Error::ReferenceConflict)
    ));
    assert_eq!(ledger.journals().unwrap().len(), 1);
    assert_eq!(
        ledger.balances().unwrap()[&("cash".into(), "GBP".into())],
        9975
    );
    let mut reversal = journal.clone();
    reversal.reference = "reversal-of-settlement-001".into();
    for posting in &mut reversal.postings {
        posting.minor_units = -posting.minor_units;
    }
    ledger.post(&reversal).unwrap();
    assert!(
        ledger
            .balances()
            .unwrap()
            .values()
            .all(|balance| *balance == 0)
    );
    assert_eq!(ledger.journals().unwrap().len(), 2);
}

#[test]
fn invalid_journals_leave_no_records() {
    let mut ledger = Ledger::open(":memory:").unwrap();
    let mut journal = payment();
    journal.postings[0].minor_units += 1;
    assert!(ledger.post(&journal).is_err());
    journal = payment();
    journal.postings[0].currency = "USD".into();
    assert!(ledger.post(&journal).is_err());
    journal = payment();
    journal.postings[0].currency = "BTC".into();
    assert!(ledger.post(&journal).is_err());
    assert!(ledger.journals().unwrap().is_empty());
}

#[test]
fn independent_connections_serialize_duplicate_posting_and_survive_reopen() {
    let path = std::env::temp_dir().join(format!("ledger-{}.db", uuid::Uuid::new_v4()));
    let first = Ledger::open(&path).unwrap();
    let second = Ledger::open(&path).unwrap();
    let handles: Vec<_> = [first, second]
        .into_iter()
        .map(|mut ledger| std::thread::spawn(move || ledger.post(&payment()).unwrap()))
        .collect();
    let outcomes: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| **result == Outcome::Recorded)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| **result == Outcome::AlreadyRecorded)
            .count(),
        1
    );
    let ledger = Ledger::open(&path).unwrap();
    assert_eq!(ledger.journals().unwrap(), vec![payment()]);
    drop(ledger);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn database_rejects_direct_mutation() {
    let path = std::env::temp_dir().join(format!("ledger-{}.db", uuid::Uuid::new_v4()));
    let mut ledger = Ledger::open(&path).unwrap();
    ledger.post(&payment()).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    assert!(
        connection
            .execute("DELETE FROM financial_journal", [])
            .is_err()
    );
    assert!(
        connection
            .execute("UPDATE financial_journal SET payload='{}'", [])
            .is_err()
    );
    drop(connection);
    drop(ledger);
    std::fs::remove_file(path).unwrap();
}
