use super::{
    tests::{event, memory},
    *,
};
use crate::PaymentStatus;
#[test]
fn deadlines_are_bounded_repeatable_and_open_cases_without_reads() {
    let s = memory();
    for i in 0..3 {
        let mut e = event(
            &format!("i{i}"),
            EventSource::Internal,
            1,
            PaymentStatus::Completed,
        );
        e.payment.id = format!("p{i}");
        s.apply_at(e, 100).unwrap();
    }
    assert_eq!(s.process_deadlines_at(1099, 2).unwrap(), 0);
    assert_eq!(s.process_deadlines_at(1100, 2).unwrap(), 2);
    assert_eq!(s.process_deadlines_at(1100, 2).unwrap(), 1);
    assert_eq!(s.process_deadlines_at(1100, 2).unwrap(), 0);
    let page = s
        .cases(CaseQuery {
            limit: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(page.entries.len(), 2);
    assert_eq!(page.entries[0].opened_at_ms, 1100);
    assert_eq!(
        s.cases(CaseQuery {
            after: page.next_after,
            ..Default::default()
        })
        .unwrap()
        .entries
        .len(),
        1
    );
}
#[test]
fn workflow_changes_are_audited_and_do_not_resolve_source_disagreement() {
    let s = memory();
    s.apply(event(
        "i",
        EventSource::Internal,
        1,
        PaymentStatus::Completed,
    ))
    .unwrap();
    s.apply(event("p", EventSource::Provider, 1, PaymentStatus::Pending))
        .unwrap();
    let before = s.cases(CaseQuery::default()).unwrap().entries.remove(0);
    let update = CaseUpdate {
        id: "assignment".into(),
        expected_revision: before.revision,
        expected_case_version: 0,
        owner: Some("alice".into()),
        workflow: WorkflowStatus::InProgress,
        note: "Investigating delayed settlement".into(),
    };
    s.update_case("PAY-001", "operator", update.clone())
        .unwrap();
    assert!(
        s.update_case("PAY-001", "operator", update.clone())
            .unwrap()
            .duplicate
    );
    assert!(matches!(
        s.update_case(
            "PAY-001",
            "operator",
            CaseUpdate {
                id: "stale".into(),
                ..update
            }
        ),
        Err(EventError::RevisionConflict)
    ));
    let rows = s
        .cases(CaseQuery {
            owner: Some("alice".into()),
            acknowledged: Some(false),
            workflow: Some(WorkflowStatus::InProgress),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(rows.entries.len(), 1);
    assert_eq!(rows.entries[0].case_version, 1);
    assert_eq!(rows.entries[0].status, "StatusMismatch");
    assert_eq!(
        s.history("PAY-001", 0, 100)
            .unwrap()
            .entries
            .last()
            .unwrap()
            .kind,
        "WorkflowChanged"
    );
    s.apply(event(
        "p2",
        EventSource::Provider,
        2,
        PaymentStatus::Completed,
    ))
    .unwrap();
    assert!(s.cases(CaseQuery::default()).unwrap().entries.is_empty());
    s.apply(event(
        "p3",
        EventSource::Provider,
        3,
        PaymentStatus::Pending,
    ))
    .unwrap();
    let reopened = s.cases(CaseQuery::default()).unwrap().entries.remove(0);
    assert_eq!(reopened.owner.as_deref(), Some("alice"));
    assert!(reopened.revision > before.revision);
}
#[test]
fn archive_and_recovery_preserve_all_evidence_and_detect_corruption() {
    let root = std::env::temp_dir().join(format!("ops-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let db = root.join("events.sqlite3");
    let s = EventStore::open(&db, Duration::from_secs(30)).unwrap();
    s.apply(event(
        "i",
        EventSource::Internal,
        1,
        PaymentStatus::Completed,
    ))
    .unwrap();
    let archive = root.join("archive");
    let manifest = archive_database(&db, &archive, chrono::Utc::now().timestamp_millis()).unwrap();
    assert_eq!(manifest.history_records, 1);
    verify_archive(&archive).unwrap();
    assert!(recovery_drill(&db, &root.join("drill")).unwrap().passed);
    assert!(
        s.apply(event(
            "i",
            EventSource::Internal,
            1,
            PaymentStatus::Completed
        ))
        .unwrap()
        .duplicate_event
    );
    std::fs::write(archive.join("history.jsonl"), "tampered").unwrap();
    assert!(verify_archive(&archive).is_err());
    drop(s);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn webhook_enqueue_failure_rolls_back_transition_and_payment() {
    let s = memory();
    s.lock()
        .unwrap()
        .execute(
            "INSERT INTO notification_settings VALUES(1,'https://example.test/hook')",
            [],
        )
        .unwrap();
    s.apply(event(
        "i",
        EventSource::Internal,
        1,
        PaymentStatus::Completed,
    ))
    .unwrap();
    s.lock().unwrap().execute_batch("CREATE TRIGGER reject_job BEFORE INSERT ON delivery_jobs BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(
        s.apply(event("p", EventSource::Provider, 1, PaymentStatus::Pending))
            .is_err()
    );
    assert!(s.status("PAY-001").unwrap().provider.is_none());
    let count: i64 = s
        .lock()
        .unwrap()
        .query_row("SELECT count(*) FROM event_log", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}
