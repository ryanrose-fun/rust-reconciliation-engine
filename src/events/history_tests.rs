use super::tests::{event, memory};
use super::*;
use crate::PaymentStatus;
#[test]
fn history_tracks_changes_resolution_and_revision_scoped_acknowledgements() {
    let s = memory();
    let first = event("i", EventSource::Internal, 1, PaymentStatus::Completed);
    s.apply(first.clone()).unwrap();
    s.apply(event("p", EventSource::Provider, 1, PaymentStatus::Pending))
        .unwrap();
    let h = s.history("PAY-001", 0, 100).unwrap();
    assert_eq!(
        h.entries
            .iter()
            .map(|e| e.kind.as_str())
            .collect::<Vec<_>>(),
        vec!["Baseline", "Opened"]
    );
    let ack = Investigation {
        id: "ack".into(),
        action: InvestigationAction::Acknowledge,
        note: "Awaiting provider update".into(),
        expected_revision: h.current_revision,
    };
    let r = s.investigate("PAY-001", "reviewer", ack.clone()).unwrap();
    assert!(!r.duplicate);
    assert!(
        s.investigate("PAY-001", "reviewer", ack.clone())
            .unwrap()
            .duplicate
    );
    assert!(s.history("PAY-001", 0, 100).unwrap().acknowledged);
    let mut change = event("p2", EventSource::Provider, 2, PaymentStatus::Pending);
    change.payment.amount_minor = 10;
    s.apply(change).unwrap();
    let h = s.history("PAY-001", 0, 100).unwrap();
    assert!(!h.acknowledged);
    assert_eq!(h.entries.last().unwrap().kind, "Changed");
    let stale = Investigation {
        id: "new-ack".into(),
        ..ack.clone()
    };
    assert!(matches!(
        s.investigate("PAY-001", "reviewer", stale),
        Err(EventError::RevisionConflict)
    ));
    assert!(s.investigate("PAY-001", "reviewer", ack).unwrap().duplicate);
    s.apply(event(
        "p3",
        EventSource::Provider,
        3,
        PaymentStatus::Completed,
    ))
    .unwrap();
    let h = s.history("PAY-001", 0, 100).unwrap();
    assert_eq!(h.current_status, "Matched");
    assert_eq!(h.entries.last().unwrap().kind, "Resolved");
    let len = h.entries.len();
    s.apply(first).unwrap();
    s.apply(event(
        "late",
        EventSource::Provider,
        2,
        PaymentStatus::Pending,
    ))
    .unwrap();
    assert_eq!(s.history("PAY-001", 0, 100).unwrap().entries.len(), len);
    let first_page = s.history("PAY-001", 0, 2).unwrap();
    let second = s
        .history("PAY-001", first_page.next_after.unwrap(), 100)
        .unwrap();
    assert_eq!(first_page.entries.len() + second.entries.len(), len);
    assert_eq!(
        s.status("PAY-001")
            .unwrap()
            .internal
            .unwrap()
            .payment
            .amount_minor,
        u64::MAX
    );
}
#[test]
fn expired_wait_records_effective_deadline_before_late_resolution() {
    let s = memory();
    s.apply_at(
        event("i", EventSource::Internal, 1, PaymentStatus::Completed),
        100,
    )
    .unwrap();
    s.apply_at(
        event("p", EventSource::Provider, 1, PaymentStatus::Completed),
        2000,
    )
    .unwrap();
    let h = s.history("PAY-001", 0, 100).unwrap();
    let opened = h.entries.iter().find(|e| e.kind == "Opened").unwrap();
    assert_eq!(opened.effective_at_ms, 1100);
    assert_eq!(opened.recorded_at_ms, 2000);
    assert_eq!(h.entries.last().unwrap().kind, "Resolved");
}
#[test]
fn history_failure_rolls_back_payment_evidence_and_event_identity() {
    let s = memory();
    s.lock().unwrap().execute_batch("CREATE TRIGGER fail_history BEFORE INSERT ON payment_history BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(
        s.apply(event(
            "i",
            EventSource::Internal,
            1,
            PaymentStatus::Completed
        ))
        .is_err()
    );
    assert_eq!(
        s.lock()
            .unwrap()
            .query_row("SELECT count(*) FROM event_log", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert!(matches!(s.status("PAY-001"), Err(EventError::NotFound)));
}
#[test]
fn schema_one_migration_preserves_evidence_and_marks_unknown_past_as_baseline() {
    let path =
        std::env::temp_dir().join(format!("history-upgrade-{}.sqlite3", uuid::Uuid::new_v4()));
    let e = event("i", EventSource::Internal, 1, PaymentStatus::Completed);
    {
        let s = EventStore::open(&path, Duration::from_secs(30)).unwrap();
        s.apply(e.clone()).unwrap();
    }
    {
        let c = Connection::open(&path).unwrap();
        c.execute_batch(
            "DROP TABLE pending_deadlines; DROP TABLE case_workflow; DROP TABLE notification_settings; DROP TABLE delivery_jobs; DROP INDEX due_payments; DROP TABLE case_state; DROP TABLE payment_history; PRAGMA user_version=1;",
        )
        .unwrap();
    }
    let backup = path.with_extension("backup.sqlite3");
    super::operations::backup_database(&path, &backup).unwrap();
    for file in [&path, &backup] {
        let c = Connection::open(file).unwrap();
        assert_eq!(
            c.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    std::fs::remove_file(backup).unwrap();
    {
        let s = EventStore::open(&path, Duration::from_secs(30)).unwrap();
        let h = s.history("PAY-001", 0, 100).unwrap();
        assert_eq!(h.entries.len(), 1);
        assert_eq!(h.entries[0].kind, "Baseline");
        assert!(s.apply(e).unwrap().duplicate_event);
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn notes_and_acknowledgement_survive_backup_without_modifying_evidence() {
    let path = std::env::temp_dir().join(format!("history-copy-{}.sqlite3", uuid::Uuid::new_v4()));
    let backup = path.with_extension("backup.sqlite3");
    let acknowledgement;
    {
        let s = EventStore::open(&path, Duration::from_secs(30)).unwrap();
        s.apply(event(
            "i",
            EventSource::Internal,
            1,
            PaymentStatus::Completed,
        ))
        .unwrap();
        s.apply(event("p", EventSource::Provider, 1, PaymentStatus::Pending))
            .unwrap();
        let revision = s.history("PAY-001", 0, 100).unwrap().current_revision;
        let note = Investigation {
            id: "note-1".into(),
            action: InvestigationAction::Note,
            note: "Checked the provider receipt".into(),
            expected_revision: revision,
        };
        s.investigate("PAY-001", "audits", note.clone()).unwrap();
        assert!(!s.history("PAY-001", 0, 100).unwrap().acknowledged);
        assert!(matches!(
            s.investigate("PAY-001", "different-actor", note),
            Err(EventError::RevisionConflict)
        ));
        acknowledgement = Investigation {
            id: "ack-1".into(),
            action: InvestigationAction::Acknowledge,
            note: "Waiting on provider".into(),
            expected_revision: revision,
        };
        s.investigate("PAY-001", "audits", acknowledgement.clone())
            .unwrap();
        s.backup(&backup).unwrap();
    }
    for file in [&path, &backup] {
        let s = EventStore::open(file, Duration::from_secs(30)).unwrap();
        let h = s.history("PAY-001", 0, 100).unwrap();
        assert!(h.acknowledged);
        assert_eq!(h.entries.len(), 4);
        assert_eq!(h.entries[2].kind, "Note");
        assert_eq!(
            h.entries[2].note.as_deref(),
            Some("Checked the provider receipt")
        );
        assert!(
            s.investigate("PAY-001", "audits", acknowledgement.clone())
                .unwrap()
                .duplicate
        );
        let payment = s.status("PAY-001").unwrap();
        assert_eq!(payment.internal.unwrap().version, 1);
        assert_eq!(
            payment.provider.unwrap().payment.status,
            PaymentStatus::Pending
        );
    }
    std::fs::remove_file(path).unwrap();
    std::fs::remove_file(backup).unwrap();
}
