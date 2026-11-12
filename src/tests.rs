use crate::payment::{Payment, PaymentStatus};
use crate::reconciliation::{
    ReconciliationEntry, ReconciliationError, ReconciliationResult, find_duplicate_ids, reconcile,
    reconcile_payments,
};
use crate::recovery::{NextAction, recommend_action};

#[test]
fn detects_amount_mismatch() {
    let internal = Payment {
        id: String::from("PAY-001"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 10_000,
        status: PaymentStatus::Completed,
    };

    let provider = Payment {
        id: String::from("PAY-001"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 9_000,
        status: PaymentStatus::Completed,
    };

    let result = reconcile(&internal, &provider);

    assert_eq!(result, ReconciliationResult::AmountMismatch);
}
#[test]
fn matches_identical_payments() {
    let internal = Payment {
        id: String::from("PAY-001"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 10_000,
        status: PaymentStatus::Completed,
    };

    let provider = Payment {
        id: String::from("PAY-001"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 10_000,
        status: PaymentStatus::Completed,
    };

    let result = reconcile(&internal, &provider);

    assert_eq!(result, ReconciliationResult::Matched);
}
#[test]
fn detects_completed_versus_pending() {
    let internal = Payment {
        id: String::from("PAY-001"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 10_000,
        status: PaymentStatus::Completed,
    };

    let provider = Payment {
        id: String::from("PAY-001"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 10_000,
        status: PaymentStatus::Pending,
    };

    let result = reconcile(&internal, &provider);

    assert_eq!(result, ReconciliationResult::StatusMismatch);
}
#[test]
fn matches_when_both_systems_report_failure() {
    let internal = Payment {
        id: String::from("PAY-001"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 10_000,
        status: PaymentStatus::Failed,
    };

    let provider = Payment {
        id: String::from("PAY-001"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 10_000,
        status: PaymentStatus::Failed,
    };

    let result = reconcile(&internal, &provider);

    assert_eq!(result, ReconciliationResult::Matched);
}
#[test]
fn rejects_different_payment_ids() {
    let internal = Payment {
        id: String::from("PAY-001"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 10_000,
        status: PaymentStatus::Completed,
    };

    let provider = Payment {
        id: String::from("PAY-002"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 10_000,
        status: PaymentStatus::Completed,
    };

    let result = reconcile(&internal, &provider);

    assert_eq!(result, ReconciliationResult::DifferentPaymentIds);
}
#[test]
fn detects_payment_missing_from_provider() {
    let internal_payments = vec![Payment {
        id: String::from("PAY-002"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 5_000,
        status: PaymentStatus::Completed,
    }];

    let provider_payments = vec![];

    let entries = reconcile_payments(&internal_payments, &provider_payments)
        .expect("These inputs have unique payment IDs");

    assert_eq!(
        entries,
        vec![ReconciliationEntry {
            payment_id: String::from("PAY-002"),
            result: ReconciliationResult::MissingFromProvider,
            internal_amount_minor: Some(5000),
            provider_amount_minor: None,
            internal_currency: Some(crate::payment::Currency::GBP),
            internal_status: Some(PaymentStatus::Completed),
            provider_currency: None,
            provider_status: None,
        },]
    );
}
#[test]
fn detects_payment_missing_from_internal() {
    let internal_payments = vec![];

    let provider_payments = vec![Payment {
        id: String::from("PAY-003"),
        currency: crate::payment::Currency::GBP,
        amount_minor: 7_500,
        status: PaymentStatus::Completed,
    }];

    let entries = reconcile_payments(&internal_payments, &provider_payments)
        .expect("These inputs have unique payment IDs");

    assert_eq!(
        entries,
        vec![ReconciliationEntry {
            payment_id: String::from("PAY-003"),
            result: ReconciliationResult::MissingFromInternal,
            internal_amount_minor: None,
            provider_amount_minor: Some(7500),
            internal_currency: None,
            internal_status: None,
            provider_currency: Some(crate::payment::Currency::GBP),
            provider_status: Some(PaymentStatus::Completed),
        },]
    );
}
#[test]
fn matches_payments_in_different_orders() {
    let internal_payments = vec![
        Payment {
            id: String::from("PAY-001"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 10_000,
            status: PaymentStatus::Completed,
        },
        Payment {
            id: String::from("PAY-002"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 5_000,
            status: PaymentStatus::Pending,
        },
    ];

    // Same payments, but in reverse order.
    let provider_payments = vec![
        Payment {
            id: String::from("PAY-002"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 5_000,
            status: PaymentStatus::Pending,
        },
        Payment {
            id: String::from("PAY-001"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 10_000,
            status: PaymentStatus::Completed,
        },
    ];

    let entries = reconcile_payments(&internal_payments, &provider_payments)
        .expect("These inputs have unique payment IDs");

    assert_eq!(
        entries,
        vec![
            ReconciliationEntry {
                payment_id: String::from("PAY-001"),
                result: ReconciliationResult::Matched,
                internal_amount_minor: Some(10000),
                provider_amount_minor: Some(10000),
                internal_currency: Some(crate::payment::Currency::GBP),
                internal_status: Some(PaymentStatus::Completed),
                provider_currency: Some(crate::payment::Currency::GBP),
                provider_status: Some(PaymentStatus::Completed),
            },
            ReconciliationEntry {
                payment_id: String::from("PAY-002"),
                result: ReconciliationResult::Matched,
                internal_amount_minor: Some(5000),
                provider_amount_minor: Some(5000),
                internal_currency: Some(crate::payment::Currency::GBP),
                internal_status: Some(PaymentStatus::Pending),
                provider_currency: Some(crate::payment::Currency::GBP),
                provider_status: Some(PaymentStatus::Pending),
            },
        ]
    );
}
#[test]
fn reports_a_duplicate_id_only_once() {
    let payments = vec![
        Payment {
            id: String::from("PAY-001"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 10_000,
            status: PaymentStatus::Completed,
        },
        Payment {
            id: String::from("PAY-001"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 10_000,
            status: PaymentStatus::Completed,
        },
        Payment {
            id: String::from("PAY-001"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 9_000,
            status: PaymentStatus::Pending,
        },
    ];

    assert_eq!(find_duplicate_ids(&payments), vec![String::from("PAY-001")]);
}
#[test]
fn rejects_duplicate_provider_ids() {
    let internal_payments = vec![];

    let provider_payments = vec![
        Payment {
            id: String::from("PAY-001"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 10_000,
            status: PaymentStatus::Completed,
        },
        Payment {
            id: String::from("PAY-001"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 9_000,
            status: PaymentStatus::Pending,
        },
    ];

    let result = reconcile_payments(&internal_payments, &provider_payments);

    assert_eq!(
        result,
        Err(ReconciliationError::DuplicateProviderIds(vec![
            String::from("PAY-001")
        ]))
    );
}
#[test]
fn rejects_duplicate_internal_ids() {
    let internal_payments = vec![
        Payment {
            id: String::from("PAY-001"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 10_000,
            status: PaymentStatus::Completed,
        },
        Payment {
            id: String::from("PAY-001"),
            currency: crate::payment::Currency::GBP,
            amount_minor: 10_000,
            status: PaymentStatus::Completed,
        },
    ];

    let provider_payments = vec![];

    let result = reconcile_payments(&internal_payments, &provider_payments);

    assert_eq!(
        result,
        Err(ReconciliationError::DuplicateInternalIds(vec![
            String::from("PAY-001")
        ]))
    );
}

#[test]
fn recommends_recheck_for_status_mismatch() {
    let result = ReconciliationResult::StatusMismatch;

    let action = recommend_action(&result);

    assert_eq!(action, NextAction::RecheckSource);
}

#[test]
fn recommends_investigation_for_amount_mismatch() {
    let result = ReconciliationResult::AmountMismatch;

    let action = recommend_action(&result);

    assert_eq!(action, NextAction::InvestigateAmount);
}

#[test]
fn parses_valid_payment_json() {
    let json = r#"[
        {
            "id": "PAY-001",
            "currency":"GBP","amount_minor": 10000,
            "status": "Completed"
        }
    ]"#;

    let payments: Vec<Payment> = serde_json::from_str(json).expect("Valid JSON should parse");

    assert_eq!(payments.len(), 1);
    assert_eq!(payments[0].id, "PAY-001");
    assert_eq!(payments[0].amount_minor, 10000);
    assert_eq!(payments[0].status, PaymentStatus::Completed);
}
#[test]
fn rejects_unknown_payment_status_in_json() {
    let json = r#"[
        {
            "id": "PAY-001",
            "currency":"GBP","amount_minor": 10000,
            "status": "Banana"
        }
    ]"#;

    let result = serde_json::from_str::<Vec<Payment>>(json);

    assert!(result.is_err());
}
#[test]
fn rejects_negative_payment_amount_in_json() {
    let json = r#"[
        {
            "id": "PAY-001",
            "currency":"GBP","amount_minor": -100,
            "status": "Completed"
        }
    ]"#;

    let result = serde_json::from_str::<Vec<Payment>>(json);

    assert!(result.is_err());
}
#[test]
fn generates_json_report_with_recommended_action() {
    let entries = vec![ReconciliationEntry {
        payment_id: String::from("PAY-001"),
        result: ReconciliationResult::StatusMismatch,
        internal_amount_minor: Some(10000),
        provider_amount_minor: Some(10000),
        internal_currency: Some(crate::payment::Currency::GBP),
        internal_status: Some(PaymentStatus::Completed),
        provider_currency: Some(crate::payment::Currency::GBP),
        provider_status: Some(PaymentStatus::Pending),
    }];

    let report = crate::report::build_report(entries);

    let json = serde_json::to_value(&report).expect("Report should serialize to JSON");

    let expected = serde_json::json!({
        "summary": { "total_entries": 1, "matched": 0, "discrepancies": 1 },
        "entries": [
            {
                "payment_id": "PAY-001",
                "result": "StatusMismatch",
                "recommended_action": "RecheckSource",
                "internal_amount_minor": 10000,
                "provider_amount_minor": 10000,
                "internal_currency":"GBP", "provider_currency":"GBP", "internal_status": "Completed",
                "provider_status": "Pending"
            }
        ]
    });

    let mut json = json;
    json.as_object_mut().unwrap().remove("metadata");
    assert_eq!(json, expected);
}

#[test]
fn summarizes_matches_and_discrepancies() {
    let entries = vec![
        ReconciliationEntry {
            payment_id: String::from("PAY-001"),
            result: ReconciliationResult::Matched,
            internal_amount_minor: Some(10000),
            provider_amount_minor: Some(10000),
            internal_currency: Some(crate::payment::Currency::GBP),
            internal_status: Some(PaymentStatus::Completed),
            provider_currency: Some(crate::payment::Currency::GBP),
            provider_status: Some(PaymentStatus::Completed),
        },
        ReconciliationEntry {
            payment_id: String::from("PAY-002"),
            result: ReconciliationResult::AmountMismatch,
            internal_amount_minor: Some(5000),
            provider_amount_minor: Some(4999),
            internal_currency: Some(crate::payment::Currency::GBP),
            internal_status: Some(PaymentStatus::Completed),
            provider_currency: Some(crate::payment::Currency::GBP),
            provider_status: Some(PaymentStatus::Completed),
        },
        ReconciliationEntry {
            payment_id: String::from("PAY-003"),
            result: ReconciliationResult::MissingFromProvider,
            internal_amount_minor: Some(7500),
            provider_amount_minor: None,
            internal_currency: Some(crate::payment::Currency::GBP),
            internal_status: Some(PaymentStatus::Completed),
            provider_currency: None,
            provider_status: None,
        },
    ];
    let report = crate::report::build_report(entries);
    let json = serde_json::to_value(&report).expect("Report should serialize to JSON");
    assert_eq!(
        json["summary"],
        serde_json::json!({
            "total_entries": 3,
            "matched": 1,
            "discrepancies": 2
        })
    );
    assert_eq!(
        json["entries"]
            .as_array()
            .expect("Entries should be an array")
            .len(),
        3
    );
}

#[test]
fn rejects_empty_payment_id_in_json() {
    let json = r#"[{"id":"","currency":"GBP","amount_minor":10000,"status":"Completed"}]"#;
    let error = serde_json::from_str::<Vec<Payment>>(json)
        .expect_err("An empty payment ID should be rejected");
    assert!(error.to_string().contains("payment id must not be empty"));
}

#[test]
fn rejects_whitespace_only_payment_id_in_json() {
    for id in ["   ", "\t\r\n", "\u{2003}"] {
        let json = serde_json::json!([{
            "id": id,
            "currency":"GBP","amount_minor": 10000,
            "status": "Completed"
        }]);
        let error = serde_json::from_value::<Vec<Payment>>(json)
            .expect_err("A whitespace-only payment ID should be rejected");
        assert!(error.to_string().contains("payment id must not be empty"));
    }
}

#[test]
fn preserves_nonempty_payment_id_when_parsing_json() {
    let json = r#"[{"id":" PAY-001 ","currency":"GBP","amount_minor":10000,"status":"Completed"}]"#;
    let payments = serde_json::from_str::<Vec<Payment>>(json)
        .expect("A nonempty ID should parse without being silently changed");
    assert_eq!(payments[0].id, " PAY-001 ");
}

#[test]
fn detects_both_amount_and_status_mismatches() {
    let internal = Payment {
        id: "PAY-001".into(),
        currency: crate::payment::Currency::GBP,
        amount_minor: 5000,
        status: PaymentStatus::Completed,
    };
    let provider = Payment {
        id: "PAY-001".into(),
        currency: crate::payment::Currency::GBP,
        amount_minor: 4500,
        status: PaymentStatus::Pending,
    };
    assert_eq!(
        reconcile(&internal, &provider),
        ReconciliationResult::AmountAndStatusMismatch
    );
    assert_eq!(
        reconcile(&provider, &internal),
        ReconciliationResult::AmountAndStatusMismatch
    );
}

#[test]
fn recommends_investigating_both_mismatches() {
    assert_eq!(
        recommend_action(&ReconciliationResult::AmountAndStatusMismatch),
        NextAction::InvestigateAmountAndStatus
    );
}

#[test]
fn different_ids_take_priority_over_field_mismatches() {
    let internal = Payment {
        id: "PAY-001".into(),
        currency: crate::payment::Currency::GBP,
        amount_minor: 5000,
        status: PaymentStatus::Completed,
    };
    let provider = Payment {
        id: "PAY-002".into(),
        currency: crate::payment::Currency::GBP,
        amount_minor: 4500,
        status: PaymentStatus::Pending,
    };
    assert_eq!(
        reconcile(&internal, &provider),
        ReconciliationResult::DifferentPaymentIds
    );
}
