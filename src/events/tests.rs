use super::*;
use crate::{Currency, PaymentStatus};
pub(super) fn event(
    id: &str,
    source: EventSource,
    version: u64,
    status: PaymentStatus,
) -> PaymentEvent {
    PaymentEvent {
        event_id: id.into(),
        source,
        version,
        payment: Payment {
            id: "PAY-001".into(),
            currency: Currency::GBP,
            amount_minor: u64::MAX,
            status,
        },
    }
}
fn status(value: &LivePayment) -> String {
    serde_json::to_value(value).unwrap()["status"]
        .as_str()
        .unwrap()
        .into()
}
pub(super) fn memory() -> EventStore {
    EventStore::in_memory(Duration::from_millis(1000)).unwrap()
}
#[test]
fn waiting_mismatch_then_match() {
    let store = memory();
    let a = store
        .apply_at(
            event("i1", EventSource::Internal, 1, PaymentStatus::Completed),
            100,
        )
        .unwrap();
    assert_eq!(status(&a.payment), "AwaitingProvider");
    assert!(a.payment.recommended_action.is_none());
    let b = store
        .apply_at(
            event("p1", EventSource::Provider, 1, PaymentStatus::Pending),
            200,
        )
        .unwrap();
    assert_eq!(status(&b.payment), "StatusMismatch");
    let c = store
        .apply_at(
            event("p2", EventSource::Provider, 2, PaymentStatus::Completed),
            300,
        )
        .unwrap();
    assert_eq!(status(&c.payment), "Matched");
    assert_eq!(c.payment.provider.unwrap().payment.amount_minor, u64::MAX);
}
#[test]
fn timeout_boundary_and_late_arrival() {
    let store = memory();
    store
        .apply_at(
            event("p1", EventSource::Provider, 1, PaymentStatus::Completed),
            100,
        )
        .unwrap();
    assert_eq!(
        status(&store.status_at("PAY-001", 1099).unwrap()),
        "AwaitingInternal"
    );
    assert_eq!(
        status(&store.status_at("PAY-001", 1100).unwrap()),
        "MissingFromInternal"
    );
    let late = store
        .apply_at(
            event("i1", EventSource::Internal, 1, PaymentStatus::Completed),
            2000,
        )
        .unwrap();
    assert_eq!(status(&late.payment), "Matched");
}
#[test]
fn retries_are_idempotent_but_changed_event_ids_conflict() {
    let store = memory();
    let e = event("i1", EventSource::Internal, 1, PaymentStatus::Completed);
    store.apply_at(e.clone(), 100).unwrap();
    let retry = store.apply_at(e.clone(), 200).unwrap();
    assert!(retry.duplicate_event);
    assert_eq!(retry.outcome, EventOutcome::Applied);
    assert_eq!(retry.payment.first_seen_at_ms, 100);
    let mut changed = e;
    changed.payment.amount_minor = 1;
    assert!(matches!(
        store.apply_at(changed, 300),
        Err(EventError::EventIdConflict)
    ));
    assert_eq!(
        store
            .status_at("PAY-001", 400)
            .unwrap()
            .internal
            .unwrap()
            .payment
            .amount_minor,
        u64::MAX
    );
}
#[test]
fn old_versions_never_overwrite_and_equal_versions_must_agree() {
    let store = memory();
    store
        .apply_at(
            event("i2", EventSource::Internal, 2, PaymentStatus::Completed),
            100,
        )
        .unwrap();
    let old = store
        .apply_at(
            event("i1", EventSource::Internal, 1, PaymentStatus::Pending),
            200,
        )
        .unwrap();
    assert_eq!(old.outcome, EventOutcome::Stale);
    assert_eq!(old.payment.internal.unwrap().version, 2);
    let equal = store
        .apply_at(
            event(
                "i2-alias",
                EventSource::Internal,
                2,
                PaymentStatus::Completed,
            ),
            300,
        )
        .unwrap();
    assert_eq!(equal.outcome, EventOutcome::Unchanged);
    assert!(matches!(
        store.apply_at(
            event("conflict", EventSource::Internal, 2, PaymentStatus::Failed),
            400
        ),
        Err(EventError::VersionConflict)
    ));
    // Failed transactions do not reserve event IDs.
    assert!(
        store
            .apply_at(
                event("conflict", EventSource::Internal, 3, PaymentStatus::Failed),
                500
            )
            .is_ok()
    );
    assert!(
        store
            .apply_at(
                event("i1", EventSource::Internal, 1, PaymentStatus::Pending),
                600
            )
            .unwrap()
            .duplicate_event
    );
}
#[test]
fn higher_versions_and_retries_do_not_extend_waiting_period() {
    let store = memory();
    let first = event("i1", EventSource::Internal, 1, PaymentStatus::Pending);
    store.apply_at(first.clone(), 100).unwrap();
    store
        .apply_at(
            event("i2", EventSource::Internal, 2, PaymentStatus::Completed),
            900,
        )
        .unwrap();
    let retry = store.apply_at(first, 1100).unwrap();
    assert_eq!(retry.payment.waiting_until_ms, 1100);
    assert_eq!(status(&retry.payment), "MissingFromProvider");
    assert_eq!(retry.payment.internal.unwrap().version, 2);
}
struct TestDb(std::path::PathBuf);
impl TestDb {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "reconcile-event-test-{}.sqlite3",
            uuid::Uuid::new_v4()
        )))
    }
}
impl Drop for TestDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}
#[test]
fn disk_restart_preserves_events_deadlines_and_exact_values() {
    let db = TestDb::new();
    {
        let store = EventStore::open(&db.0, Duration::from_millis(1000)).unwrap();
        store
            .apply_at(
                event("i1", EventSource::Internal, 1, PaymentStatus::Completed),
                100,
            )
            .unwrap();
    }
    // New waiting policy affects only new payment IDs, not existing deadlines.
    let store = EventStore::open(&db.0, Duration::from_secs(90)).unwrap();
    assert_eq!(
        status(&store.status_at("PAY-001", 1100).unwrap()),
        "MissingFromProvider"
    );
    let retry = store
        .apply_at(
            event("i1", EventSource::Internal, 1, PaymentStatus::Completed),
            1200,
        )
        .unwrap();
    assert!(retry.duplicate_event);
    assert_eq!(
        retry.payment.internal.unwrap().payment.amount_minor,
        u64::MAX
    );
    assert_eq!(
        status(
            &store
                .apply_at(
                    event("p1", EventSource::Provider, 1, PaymentStatus::Completed),
                    1300
                )
                .unwrap()
                .payment
        ),
        "Matched"
    );
}
#[test]
fn transactions_roll_back_event_log_and_state_together() {
    let store = memory();
    store.connection.lock().unwrap().execute_batch(
        "CREATE TRIGGER fail_state BEFORE INSERT ON source_state BEGIN SELECT RAISE(ABORT,'injected failure'); END;"
    ).unwrap();
    let e = event("i1", EventSource::Internal, 1, PaymentStatus::Completed);
    assert!(store.apply_at(e.clone(), 100).is_err());
    assert!(matches!(
        store.status_at("PAY-001", 100),
        Err(EventError::NotFound)
    ));
    store
        .connection
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER fail_state")
        .unwrap();
    let accepted = store.apply_at(e, 200).unwrap();
    assert!(!accepted.duplicate_event);
    assert_eq!(accepted.payment.first_seen_at_ms, 200);
}
#[test]
fn independent_connections_serialize_and_busy_errors_are_retryable() {
    let db = TestDb::new();
    let a = EventStore::open(&db.0, Duration::from_secs(1)).unwrap();
    let b = EventStore::open(&db.0, Duration::from_secs(1)).unwrap();
    a.apply_at(
        event("i9", EventSource::Internal, 9, PaymentStatus::Completed),
        100,
    )
    .unwrap();
    assert_eq!(
        b.apply_at(
            event("i1", EventSource::Internal, 1, PaymentStatus::Pending),
            200
        )
        .unwrap()
        .outcome,
        EventOutcome::Stale
    );
    let mut connection = a.connection.lock().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    assert!(matches!(
        b.apply_at(
            event("p1", EventSource::Provider, 1, PaymentStatus::Completed),
            300
        ),
        Err(EventError::Busy)
    ));
    tx.rollback().unwrap();
    assert!(
        b.apply_at(
            event("p1", EventSource::Provider, 1, PaymentStatus::Completed),
            400
        )
        .is_ok()
    );
}
#[test]
fn validation_is_bounded_and_unknown_payments_are_not_found() {
    let store = memory();
    for version in [0, u64::MAX] {
        assert!(matches!(
            store.apply_at(
                event(
                    "e",
                    EventSource::Internal,
                    version,
                    PaymentStatus::Completed
                ),
                1
            ),
            Err(EventError::InvalidInput(_))
        ));
    }
    for id in [" ".into(), "x".repeat(129), "a\nb".into()] {
        assert!(matches!(
            store.apply_at(
                event(&id, EventSource::Internal, 1, PaymentStatus::Completed),
                1
            ),
            Err(EventError::InvalidInput(_))
        ));
    }
    assert!(matches!(store.status("unknown"), Err(EventError::NotFound)));
    let _guard = store.connection.lock().unwrap();
    assert!(matches!(store.status("unknown"), Err(EventError::Busy)));
}
#[test]
fn unsupported_database_version_is_not_silently_reset() {
    let db = TestDb::new();
    let connection = Connection::open(&db.0).unwrap();
    connection.execute_batch("PRAGMA user_version=99").unwrap();
    drop(connection);
    assert!(matches!(
        EventStore::open(&db.0, Duration::from_secs(1)),
        Err(EventError::UnsupportedSchema)
    ));
    let version: i64 = Connection::open(&db.0)
        .unwrap()
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 99);
}
