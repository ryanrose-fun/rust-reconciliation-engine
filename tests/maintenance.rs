use reconcile_cli::{
    Currency, Payment, PaymentStatus,
    events::{EventSource, EventStore, PaymentEvent},
};
use std::{process::Command, time::Duration};
#[test]
fn backup_restore_cli_preserves_acknowledged_events_and_refuses_overwrite() {
    let dir = std::env::temp_dir().join(format!("reconcile-maintenance-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let db = dir.join("original.sqlite3");
    let backup = dir.join("backup.sqlite3");
    let restored = dir.join("restored.sqlite3");
    let store = EventStore::open(&db, Duration::from_secs(30)).unwrap();
    let event = PaymentEvent {
        event_id: "one".into(),
        source: EventSource::Internal,
        version: 1,
        payment: Payment {
            id: "one".into(),
            amount_minor: u64::MAX,
            currency: Currency::GBP,
            status: PaymentStatus::Completed,
        },
    };
    store.apply(event.clone()).unwrap();
    let run = |args: Vec<&std::ffi::OsStr>| {
        Command::new(env!("CARGO_BIN_EXE_maintenance"))
            .args(args)
            .output()
            .unwrap()
    };
    assert!(
        run(vec!["backup".as_ref(), db.as_os_str(), backup.as_os_str()])
            .status
            .success()
    );
    assert!(
        !run(vec!["backup".as_ref(), db.as_os_str(), backup.as_os_str()])
            .status
            .success()
    );
    assert!(
        run(vec![
            "restore".as_ref(),
            backup.as_os_str(),
            restored.as_os_str()
        ])
        .status
        .success()
    );
    assert!(
        !run(vec![
            "restore".as_ref(),
            backup.as_os_str(),
            restored.as_os_str()
        ])
        .status
        .success()
    );
    let recovered = EventStore::open(&restored, Duration::from_secs(30)).unwrap();
    assert!(recovered.apply(event).unwrap().duplicate_event);
    assert_eq!(
        recovered
            .status("one")
            .unwrap()
            .internal
            .unwrap()
            .payment
            .amount_minor,
        u64::MAX
    );
    let invalid = dir.join("invalid.sqlite3");
    std::fs::write(&invalid, b"not a database").unwrap();
    assert!(
        !run(vec!["verify".as_ref(), invalid.as_os_str()])
            .status
            .success()
    );
    drop(recovered);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}
