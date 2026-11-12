use hello_cargo::{Payment, PaymentSource, PaymentStatus, ReconciliationError, reconcile_report};

fn payment(id: &str, amount: u64) -> Payment {
    Payment {
        id: id.into(),
        currency: hello_cargo::Currency::GBP,
        amount_minor: amount,
        status: PaymentStatus::Completed,
    }
}

#[test]
fn library_builds_a_report_without_files() {
    let report = reconcile_report(
        &[payment("PAY-001", 100), payment("PAY-002", 200)],
        &[payment("PAY-001", 100), payment("PAY-002", 300)],
    )
    .expect("Valid snapshots should produce a report");
    let json = serde_json::to_value(report).unwrap();
    assert_eq!(
        json["summary"],
        serde_json::json!({
            "total_entries": 2, "matched": 1, "discrepancies": 1
        })
    );
    assert_eq!(json["entries"][1]["result"], "AmountMismatch");
    assert_eq!(
        json["entries"][1]["recommended_action"],
        "InvestigateAmount"
    );
}

#[test]
fn library_rejects_blank_ids_in_either_snapshot() {
    for id in ["", " \t\n", "\u{2003}"] {
        assert_eq!(
            reconcile_report(&[payment("valid", 100), payment(id, 100)], &[]).unwrap_err(),
            ReconciliationError::InvalidPaymentId {
                source: PaymentSource::Internal,
                index: 1
            }
        );
        assert_eq!(
            reconcile_report(&[], &[payment(id, 100)]).unwrap_err(),
            ReconciliationError::InvalidPaymentId {
                source: PaymentSource::Provider,
                index: 0
            }
        );
    }
}

#[test]
fn library_returns_structured_duplicate_errors() {
    assert_eq!(
        reconcile_report(&[payment("duplicate", 100), payment("duplicate", 100)], &[]).unwrap_err(),
        ReconciliationError::DuplicateInternalIds(vec!["duplicate".into()])
    );
    assert_eq!(
        reconcile_report(&[], &[payment("duplicate", 100), payment("duplicate", 100)]).unwrap_err(),
        ReconciliationError::DuplicateProviderIds(vec!["duplicate".into()])
    );
}

#[test]
fn library_accepts_empty_snapshots() {
    let report = reconcile_report(&[], &[]).unwrap();
    assert_eq!(report.summary.total_entries, 0);
    assert_eq!(report.summary.matched, 0);
    assert_eq!(report.summary.discrepancies, 0);
    assert!(report.entries.is_empty());
}

#[test]
fn report_preserves_values_for_both_kinds_of_mismatch() {
    let internal = vec![payment("amount", 100), payment("status", 200)];
    let provider = vec![
        Payment {
            id: "status".into(),
            currency: hello_cargo::Currency::GBP,
            amount_minor: 200,
            status: PaymentStatus::Pending,
        },
        payment("amount", 90),
    ];
    let json = serde_json::to_value(reconcile_report(&internal, &provider).unwrap()).unwrap();
    assert_eq!(json["entries"][0]["internal_amount_minor"], 100);
    assert_eq!(json["entries"][0]["provider_amount_minor"], 90);
    assert_eq!(json["entries"][1]["internal_status"], "Completed");
    assert_eq!(json["entries"][1]["provider_status"], "Pending");
    assert_eq!(json["summary"]["discrepancies"], 2);
}

#[test]
fn absent_sources_are_null_and_zero_amounts_are_preserved() {
    let report = reconcile_report(
        &[payment("internal-only", 0)],
        &[payment("provider-only", 0)],
    )
    .unwrap();
    let json = serde_json::to_value(report).unwrap();
    assert_eq!(json["entries"][0]["internal_amount_minor"], 0);
    assert_eq!(json["entries"][0]["internal_status"], "Completed");
    assert!(json["entries"][0]["provider_amount_minor"].is_null());
    assert!(json["entries"][0]["provider_status"].is_null());
    assert!(json["entries"][1]["internal_amount_minor"].is_null());
    assert!(json["entries"][1]["internal_status"].is_null());
    assert_eq!(json["entries"][1]["provider_amount_minor"], 0);
    assert_eq!(json["entries"][1]["provider_status"], "Completed");
}

#[test]
fn combined_mismatch_counts_as_one_discrepant_payment() {
    let internal = [payment("matched", 100), payment("both", 5000)];
    let provider = [
        Payment {
            id: "both".into(),
            currency: hello_cargo::Currency::GBP,
            amount_minor: 4500,
            status: PaymentStatus::Pending,
        },
        payment("matched", 100),
    ];
    let json = serde_json::to_value(reconcile_report(&internal, &provider).unwrap()).unwrap();
    assert_eq!(
        json["summary"],
        serde_json::json!({"total_entries":2,"matched":1,"discrepancies":1})
    );
    assert_eq!(json["entries"].as_array().unwrap().len(), 2);
    assert_eq!(
        json["entries"][1],
        serde_json::json!({
            "payment_id":"both", "result":"AmountAndStatusMismatch",
            "recommended_action":"InvestigateAmountAndStatus",
            "internal_amount_minor":5000, "provider_amount_minor":4500,
            "internal_currency":"GBP", "provider_currency":"GBP", "internal_status":"Completed", "provider_status":"Pending"
        })
    );
}

#[test]
fn currency_combinations_never_match_and_count_once() {
    use hello_cargo::{Currency, ReconciliationResult};
    for (amount, status, expected) in [
        (
            100,
            PaymentStatus::Completed,
            ReconciliationResult::CurrencyMismatch,
        ),
        (
            90,
            PaymentStatus::Completed,
            ReconciliationResult::AmountAndCurrencyMismatch,
        ),
        (
            100,
            PaymentStatus::Pending,
            ReconciliationResult::StatusAndCurrencyMismatch,
        ),
        (
            90,
            PaymentStatus::Pending,
            ReconciliationResult::AmountStatusAndCurrencyMismatch,
        ),
    ] {
        let internal = [payment("P1", 100)];
        let provider = [Payment {
            id: "P1".into(),
            currency: Currency::USD,
            amount_minor: amount,
            status,
        }];
        let report = reconcile_report(&internal, &provider).unwrap();
        assert_eq!(report.entries[0].result, expected);
        assert_eq!(report.summary.discrepancies, 1);
        assert_eq!(report.entries[0].internal_currency, Some(Currency::GBP));
        assert_eq!(report.entries[0].provider_currency, Some(Currency::USD));
    }
    assert_eq!(Currency::JPY.minor_unit_exponent(), 0);
    assert_eq!(Currency::GBP.minor_unit_exponent(), 2);
}

#[test]
fn json_requires_a_supported_currency_and_new_amount_field() {
    for text in [
        r#"{"id":"P1","amount_minor":100,"status":"Completed"}"#,
        r#"{"id":"P1","currency":"XYZ","amount_minor":100,"status":"Completed"}"#,
        r#"{"id":"P1","currency":"GBP","amount_pence":100,"status":"Completed"}"#,
    ] {
        assert!(serde_json::from_str::<Payment>(text).is_err());
    }
}

#[test]
fn provenance_is_repeatable_for_inputs_and_changes_with_values() {
    let a = [payment("P1", 100)];
    let b = [payment("P1", 90)];
    let first = reconcile_report(&a, &b).unwrap();
    let second = reconcile_report(&a, &b).unwrap();
    assert_ne!(first.metadata.report_id, second.metadata.report_id);
    assert_eq!(
        first.metadata.internal_source.sha256,
        second.metadata.internal_source.sha256
    );
    assert_ne!(
        first.metadata.internal_source.sha256,
        first.metadata.provider_source.sha256
    );
    assert_eq!(first.metadata.internal_source.record_count, 1);
    assert_eq!(first.metadata.engine_version, env!("CARGO_PKG_VERSION"));
    assert!(chrono::DateTime::parse_from_rfc3339(&first.metadata.generated_at).is_ok());
    assert!(uuid::Uuid::parse_str(&first.metadata.report_id).is_ok());
    let changed = [Payment {
        currency: hello_cargo::Currency::USD,
        ..payment("P1", 100)
    }];
    assert_ne!(
        first.metadata.internal_source.sha256,
        reconcile_report(&changed, &b)
            .unwrap()
            .metadata
            .internal_source
            .sha256
    );
}
