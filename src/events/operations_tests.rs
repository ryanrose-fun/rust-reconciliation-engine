use super::tests::{event, memory};
use super::*;
use crate::PaymentStatus;
#[test]
fn discrepancy_pages_filter_and_advance_without_duplicates() {
    let store = memory();
    for (id, currency) in [
        ("A", crate::Currency::GBP),
        ("B", crate::Currency::USD),
        ("C", crate::Currency::GBP),
    ] {
        let mut e = event(id, EventSource::Internal, 1, PaymentStatus::Completed);
        e.payment.id = id.into();
        e.payment.currency = currency;
        store.apply_at(e, 0).unwrap();
    }
    let page = store
        .discrepancies(DiscrepancyQuery {
            limit: Some(1),
            currency: Some(crate::Currency::GBP),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(page.entries[0].payment_id, "A");
    assert_eq!(page.next_after.as_deref(), Some("A"));
    let page = store
        .discrepancies(DiscrepancyQuery {
            after: page.next_after,
            currency: Some(crate::Currency::GBP),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].payment_id, "C");
    assert!(page.next_after.is_none());
    assert!(
        store
            .discrepancies(DiscrepancyQuery {
                source: Some(EventSource::Provider),
                ..Default::default()
            })
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(
        store
            .discrepancies(DiscrepancyQuery {
                limit: Some(101),
                ..Default::default()
            })
            .is_err()
    );
    assert!(
        store
            .discrepancies(DiscrepancyQuery {
                status: Some("Wrong".into()),
                ..Default::default()
            })
            .is_err()
    );
    assert_eq!(store.health().unwrap().overdue_counterpart, 3);
}
#[test]
fn waiting_and_matched_are_not_discrepancies_and_age_is_respected() {
    let store = memory();
    store
        .apply(event(
            "i",
            EventSource::Internal,
            1,
            PaymentStatus::Completed,
        ))
        .unwrap();
    assert_eq!(store.health().unwrap().awaiting_counterpart, 1);
    assert!(
        store
            .discrepancies(Default::default())
            .unwrap()
            .entries
            .is_empty()
    );
    store
        .apply(event(
            "p",
            EventSource::Provider,
            1,
            PaymentStatus::Completed,
        ))
        .unwrap();
    assert!(
        store
            .discrepancies(Default::default())
            .unwrap()
            .entries
            .is_empty()
    );
    store
        .apply(event(
            "p2",
            EventSource::Provider,
            2,
            PaymentStatus::Pending,
        ))
        .unwrap();
    assert_eq!(
        store
            .discrepancies(Default::default())
            .unwrap()
            .entries
            .len(),
        1
    );
    assert!(
        store
            .discrepancies(DiscrepancyQuery {
                min_age_ms: Some(60000),
                ..Default::default()
            })
            .unwrap()
            .entries
            .is_empty()
    );
}
#[test]
fn backup_preserves_wal_events_and_retry_identity_without_overwriting() {
    let root = std::env::temp_dir().join(format!("reconcile-backup-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let store = EventStore::open(root.join("live.sqlite3"), Duration::from_secs(30)).unwrap();
    let e = event("i", EventSource::Internal, 1, PaymentStatus::Completed);
    store.apply(e.clone()).unwrap();
    let backup = root.join("backup.sqlite3");
    store.backup(&backup).unwrap();
    assert!(store.backup(&backup).is_err());
    verify_backup(&backup).unwrap();
    let recovered = EventStore::open(&backup, Duration::from_secs(30)).unwrap();
    assert_eq!(
        recovered
            .status("PAY-001")
            .unwrap()
            .internal
            .unwrap()
            .payment
            .amount_minor,
        u64::MAX
    );
    assert!(recovered.apply(e).unwrap().duplicate_event);
    drop(recovered);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn scan_bound_returns_cursor_even_when_no_matches_in_first_page() {
    let store = memory();
    for n in 0..1001 {
        let mut e = event(
            &format!("i{n}"),
            EventSource::Internal,
            1,
            PaymentStatus::Completed,
        );
        e.payment.id = format!("{n:04}");
        store.apply_at(e, 0).unwrap();
    }
    let query = DiscrepancyQuery {
        source: Some(EventSource::Provider),
        ..Default::default()
    };
    let page = store.discrepancies(query).unwrap();
    assert_eq!(page.scanned, 1000);
    assert!(page.entries.is_empty());
    assert_eq!(page.next_after.as_deref(), Some("0999"));
}
