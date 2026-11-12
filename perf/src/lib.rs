//! Benchmark-only alternatives. No production engine code is modified.
use hello_cargo::{
    Currency, NextAction, Payment, PaymentSource, PaymentStatus, ReconciliationError,
    ReconciliationReport, ReconciliationResult as Finding, ReportEntry, ReportMetadata,
    ReportSummary, SourceReference,
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

#[derive(Serialize, Deserialize)]
pub struct Input {
    pub internal_payments: Vec<Payment>,
    pub provider_payments: Vec<Payment>,
}

pub fn dataset(n: usize, scenario: &str) -> Input {
    assert!(n > 0);
    let internal: Vec<_> = (0..n)
        .map(|i| Payment {
            id: if scenario == "long_ids" {
                format!("{}-{i:010}", "P".repeat(240))
            } else {
                format!("PAY-{i:010}")
            },
            amount_minor: if i == 0 { u64::MAX } else { (i as u64) * 100 },
            currency: Currency::GBP,
            status: PaymentStatus::Completed,
        })
        .collect();
    let mut provider = internal.clone();
    if scenario == "mixed" {
        for (i, p) in provider.iter_mut().enumerate() {
            // All seven combinations, plus missing records; no amount overflow.
            let mask = i % 12;
            if mask < 8 {
                if mask & 1 != 0 {
                    p.amount_minor -= 1;
                }
                if mask & 2 != 0 {
                    p.status = PaymentStatus::Pending;
                }
                if mask & 4 != 0 {
                    p.currency = Currency::USD;
                }
            } else if mask == 8 {
                p.id = format!("provider-only-{i}");
            }
        }
    }
    if scenario == "duplicates" {
        provider.push(provider[0].clone());
    }
    // Deterministic Fisher-Yates shuffle of provider order; internal order stays sorted.
    let mut state = 0x123456789abcdefu64;
    for i in (1..provider.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        provider.swap(i, (state as usize) % (i + 1));
    }
    Input {
        internal_payments: internal,
        provider_payments: provider,
    }
}

// Independent validation implementation, preserving original error priority/order.
fn validate(a: &[Payment], b: &[Payment]) -> Result<(), ReconciliationError> {
    for (source, payments) in [(PaymentSource::Internal, a), (PaymentSource::Provider, b)] {
        for (index, p) in payments.iter().enumerate() {
            if p.id.trim().is_empty() {
                return Err(ReconciliationError::InvalidPaymentId { source, index });
            }
        }
    }
    for (source, payments) in [(PaymentSource::Internal, a), (PaymentSource::Provider, b)] {
        let mut seen = HashSet::new();
        let mut repeated = HashSet::new();
        let mut ids = Vec::new();
        for p in payments {
            if !seen.insert(p.id.as_str()) && repeated.insert(p.id.as_str()) {
                ids.push(p.id.clone());
            }
        }
        if !ids.is_empty() {
            return Err(match source {
                PaymentSource::Internal => ReconciliationError::DuplicateInternalIds(ids),
                PaymentSource::Provider => ReconciliationError::DuplicateProviderIds(ids),
            });
        }
    }
    Ok(())
}

fn entry(a: Option<&Payment>, b: Option<&Payment>) -> ReportEntry {
    let result = match (a, b) {
        (Some(a), Some(b)) => {
            let mask = (u8::from(a.amount_minor != b.amount_minor))
                | (u8::from(a.status != b.status) << 1)
                | (u8::from(a.currency != b.currency) << 2);
            match mask {
                0 => Finding::Matched,
                1 => Finding::AmountMismatch,
                2 => Finding::StatusMismatch,
                3 => Finding::AmountAndStatusMismatch,
                4 => Finding::CurrencyMismatch,
                5 => Finding::AmountAndCurrencyMismatch,
                6 => Finding::StatusAndCurrencyMismatch,
                _ => Finding::AmountStatusAndCurrencyMismatch,
            }
        }
        (Some(_), None) => Finding::MissingFromProvider,
        (None, Some(_)) => Finding::MissingFromInternal,
        _ => unreachable!(),
    };
    let action = match result {
        Finding::Matched => NextAction::ReviewPaymentStatus,
        Finding::AmountMismatch => NextAction::InvestigateAmount,
        Finding::AmountAndStatusMismatch => NextAction::InvestigateAmountAndStatus,
        Finding::CurrencyMismatch
        | Finding::AmountAndCurrencyMismatch
        | Finding::StatusAndCurrencyMismatch
        | Finding::AmountStatusAndCurrencyMismatch => NextAction::InvestigateCurrencyAndValues,
        _ => NextAction::RecheckSource,
    };
    ReportEntry {
        payment_id: a.or(b).unwrap().id.clone(),
        result,
        recommended_action: action,
        internal_amount_minor: a.map(|p| p.amount_minor),
        provider_amount_minor: b.map(|p| p.amount_minor),
        internal_currency: a.map(|p| p.currency),
        provider_currency: b.map(|p| p.currency),
        internal_status: a.map(|p| p.status),
        provider_status: b.map(|p| p.status),
    }
}
fn source(payments: &[Payment]) -> SourceReference {
    SourceReference {
        label: None,
        record_count: payments.len(),
        sha256: format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(payments).unwrap())
        ),
    }
}
fn finish(entries: Vec<ReportEntry>, a: &[Payment], b: &[Payment]) -> ReconciliationReport {
    let matched = entries
        .iter()
        .filter(|e| e.result == Finding::Matched)
        .count();
    ReconciliationReport {
        metadata: ReportMetadata {
            report_id: uuid::Uuid::new_v4().to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
            engine_version: env!("ENGINE_VERSION").into(),
            rules_version: "2".into(),
            fingerprint_format: "payment-json-v1-input-order".into(),
            internal_source: source(a),
            provider_source: source(b),
        },
        summary: ReportSummary {
            total_entries: entries.len(),
            matched,
            discrepancies: entries.len() - matched,
        },
        entries,
    }
}

pub fn sorted_merge(
    a: &[Payment],
    b: &[Payment],
) -> Result<ReconciliationReport, ReconciliationError> {
    validate(a, b)?;
    let mut left: Vec<_> = (0..a.len()).collect();
    let mut right: Vec<_> = (0..b.len()).collect();
    left.sort_unstable_by(|&i, &j| a[i].id.cmp(&a[j].id));
    right.sort_unstable_by(|&i, &j| b[i].id.cmp(&b[j].id));
    let mut matches = vec![None; a.len()];
    let mut used = vec![false; b.len()];
    let (mut i, mut j) = (0, 0);
    while i < left.len() && j < right.len() {
        match a[left[i]].id.cmp(&b[right[j]].id) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                matches[left[i]] = Some(right[j]);
                used[right[j]] = true;
                i += 1;
                j += 1;
            }
        }
    }
    let mut entries = Vec::with_capacity(a.len() + b.len());
    for (i, p) in a.iter().enumerate() {
        entries.push(entry(Some(p), matches[i].map(|j| &b[j])));
    }
    for (j, p) in b.iter().enumerate() {
        if !used[j] {
            entries.push(entry(None, Some(p)));
        }
    }
    Ok(finish(entries, a, b))
}

pub fn sqlite_join(
    a: &[Payment],
    b: &[Payment],
) -> Result<ReconciliationReport, ReconciliationError> {
    validate(a, b)?;
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE internal(seq INTEGER PRIMARY KEY,id TEXT NOT NULL UNIQUE COLLATE BINARY); CREATE TABLE provider(seq INTEGER PRIMARY KEY,id TEXT NOT NULL UNIQUE COLLATE BINARY);").unwrap();
    {
        let tx = conn.transaction().unwrap();
        for (table, payments) in [("internal", a), ("provider", b)] {
            let mut insert = tx
                .prepare(&format!("INSERT INTO {table}(seq,id) VALUES(?1,?2)"))
                .unwrap();
            for (i, p) in payments.iter().enumerate() {
                insert.execute(params![i as i64, p.id]).unwrap();
            }
        }
        tx.commit().unwrap();
    }
    // Index join returns row positions. Rust assembles the exact same typed report
    // from immutable input records. SQLite stores IDs/indexes, not payment payloads.
    let mut query=conn.prepare("SELECT 0 side,i.seq ordering,i.seq,p.seq FROM internal i LEFT JOIN provider p ON i.id=p.id UNION ALL SELECT 1,p.seq,NULL,p.seq FROM provider p LEFT JOIN internal i ON p.id=i.id WHERE i.seq IS NULL ORDER BY 1,2").unwrap();
    let rows = query
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<i64>>(2)?
                    .map(|v| usize::try_from(v).expect("valid ordinal")),
                row.get::<_, Option<i64>>(3)?
                    .map(|v| usize::try_from(v).expect("valid ordinal")),
            ))
        })
        .unwrap();
    let mut entries = Vec::with_capacity(a.len() + b.len());
    for pair in rows {
        let (i, j) = pair.unwrap();
        entries.push(entry(i.map(|i| &a[i]), j.map(|j| &b[j])));
    }
    Ok(finish(entries, a, b))
}

pub type Implementation =
    fn(&[Payment], &[Payment]) -> Result<ReconciliationReport, ReconciliationError>;
pub fn implementations() -> [(&'static str, Implementation); 3] {
    [
        ("engine", hello_cargo::reconcile_report),
        ("rust_sorted_merge", sorted_merge),
        ("sqlite_load_index_join", sqlite_join),
    ]
}
pub fn normalized(result: &Result<ReconciliationReport, ReconciliationError>) -> Value {
    let mut value = match result {
        Ok(r) => json!({"ok":r}),
        Err(e) => json!({"error":e}),
    };
    if let Some(meta) = value
        .pointer_mut("/ok/metadata")
        .and_then(Value::as_object_mut)
    {
        meta.remove("report_id");
        meta.remove("generated_at");
    }
    value
}
pub fn check_parity(input: &Input) {
    let expected = normalized(&hello_cargo::reconcile_report(
        &input.internal_payments,
        &input.provider_payments,
    ));
    for (name, run) in implementations() {
        assert_eq!(
            expected,
            normalized(&run(&input.internal_payments, &input.provider_payments)),
            "{name} output differs"
        );
    }
}
pub fn stats(values: &[f64]) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    let percentile = |p: f64| v[((p * v.len() as f64).ceil() as usize).saturating_sub(1)];
    json!({"count":v.len(),"mean_ms":v.iter().sum::<f64>()/v.len() as f64,"p50_ms":percentile(0.5),"p95_ms":percentile(0.95),"p99_ms":percentile(0.99),"max_ms":v[v.len()-1]})
}
pub fn histogram(values: &[u16]) -> HashMap<String, usize> {
    let mut h = HashMap::new();
    for v in values {
        *h.entry(v.to_string()).or_insert(0) += 1;
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn implementations_agree_on_all_scenarios() {
        for n in [1, 17, 100] {
            for s in ["matched", "mixed", "long_ids", "duplicates"] {
                check_parity(&dataset(n, s));
            }
        }
    }
    #[test]
    fn errors_and_empty_inputs_agree() {
        check_parity(&Input {
            internal_payments: vec![],
            provider_payments: vec![],
        });
        let mut d = dataset(3, "matched");
        d.internal_payments[1].id = " \t".into();
        check_parity(&d);
        let mut d = dataset(4, "matched");
        d.internal_payments[1].id = d.internal_payments[0].id.clone();
        d.internal_payments[2].id = d.internal_payments[0].id.clone();
        check_parity(&d);
    }
    #[test]
    fn id_order_case_unicode_and_u64_max_agree() {
        let mut d = dataset(6, "matched");
        for (p, id) in d
            .internal_payments
            .iter_mut()
            .zip(["é", "e\u{301}", "Case", "case", " x ", "x"])
        {
            p.id = id.into();
        }
        d.provider_payments = d.internal_payments.clone();
        d.provider_payments.reverse();
        check_parity(&d);
    }
    #[test]
    fn percentiles_are_nearest_rank() {
        let s = stats(&[4., 1., 3., 2.]);
        assert_eq!(s["p50_ms"], 2.);
        assert_eq!(s["p95_ms"], 4.);
        assert!(stats(&[]).is_null());
    }
    #[test]
    fn seeded_unordered_snapshots_preserve_reports_and_fingerprints() {
        for seed in 1..=100usize {
            let mut input = dataset(seed % 47 + 1, "mixed");
            input.internal_payments.rotate_left(seed % (seed % 47 + 1));
            if seed % 2 == 0 {
                input.internal_payments.reverse();
            }
            if seed % 3 == 0 {
                input.provider_payments.reverse();
            }
            if seed % 5 == 0 {
                input
                    .provider_payments
                    .push(input.provider_payments[0].clone());
            }
            if seed % 7 == 0 {
                input
                    .internal_payments
                    .push(input.internal_payments[0].clone());
            }
            if seed % 11 == 0 {
                input.provider_payments[0].id = "  ".into();
            }
            check_parity(&input);
        }
        let mut input = dataset(200, "long_ids");
        input.internal_payments.reverse();
        check_parity(&input);
    }
}
