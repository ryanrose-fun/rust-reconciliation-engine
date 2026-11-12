use crate::payment::{Currency, Payment, PaymentStatus, validate_payment_id};
use serde::Serialize;
use std::collections::HashMap;

// The possible results of comparing a payment.
#[derive(Debug, PartialEq, Serialize, utoipa::ToSchema)]
pub enum ReconciliationResult {
    Matched,
    DifferentPaymentIds,
    AmountMismatch,
    StatusMismatch,
    AmountAndStatusMismatch,
    CurrencyMismatch,
    AmountAndCurrencyMismatch,
    StatusAndCurrencyMismatch,
    AmountStatusAndCurrencyMismatch,
    MissingFromProvider,
    MissingFromInternal,
}

// A payment ID together with its reconciliation result.
#[derive(Debug, PartialEq)]
pub(crate) struct ReconciliationEntry {
    pub(crate) payment_id: String,
    pub(crate) result: ReconciliationResult,
    pub(crate) internal_amount_minor: Option<u64>,
    pub(crate) provider_amount_minor: Option<u64>,
    pub(crate) internal_currency: Option<Currency>,
    pub(crate) provider_currency: Option<Currency>,
    pub(crate) internal_status: Option<PaymentStatus>,
    pub(crate) provider_status: Option<PaymentStatus>,
}

// Problems that prevent us from reconciling the supplied data.
#[derive(Debug, PartialEq, Serialize, utoipa::ToSchema)]
pub enum ReconciliationError {
    InvalidPaymentId { source: PaymentSource, index: usize },
    DuplicateInternalIds(Vec<String>),
    DuplicateProviderIds(Vec<String>),
}

// Compare one internal payment with one provider payment.
pub(crate) fn reconcile(internal: &Payment, provider: &Payment) -> ReconciliationResult {
    if internal.id != provider.id {
        return ReconciliationResult::DifferentPaymentIds;
    }

    // Evaluate both fields before choosing a result.
    let amount_differs = internal.amount_minor != provider.amount_minor;
    let status_differs = internal.status != provider.status;
    let currency_differs = internal.currency != provider.currency;
    match (amount_differs, status_differs, currency_differs) {
        (true, true, false) => ReconciliationResult::AmountAndStatusMismatch,
        (true, false, false) => ReconciliationResult::AmountMismatch,
        (false, true, false) => ReconciliationResult::StatusMismatch,
        (false, false, true) => ReconciliationResult::CurrencyMismatch,
        (true, false, true) => ReconciliationResult::AmountAndCurrencyMismatch,
        (false, true, true) => ReconciliationResult::StatusAndCurrencyMismatch,
        (true, true, true) => ReconciliationResult::AmountStatusAndCurrencyMismatch,
        (false, false, false) => ReconciliationResult::Matched,
    }
}

// Return each duplicated ID once, even if it appears many times.
#[cfg(test)]
pub(crate) fn find_duplicate_ids(payments: &[Payment]) -> Vec<String> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    let mut duplicates = Vec::new();

    for payment in payments {
        let count = counts.entry(payment.id.as_str()).or_insert(0);
        *count += 1;

        if *count == 2 {
            duplicates.push(payment.id.clone());
        }
    }

    duplicates
}

#[derive(Clone, Copy)]
struct IndexedPayment {
    position: usize,
    repeated: bool,
}

// Build the lookup once while detecting duplicates in second-occurrence order.
fn index(payments: &[Payment]) -> (HashMap<&str, IndexedPayment>, Vec<String>) {
    use std::collections::hash_map::Entry;
    let mut index = HashMap::with_capacity(payments.len());
    let mut duplicates = Vec::new();
    for (position, payment) in payments.iter().enumerate() {
        match index.entry(payment.id.as_str()) {
            Entry::Vacant(slot) => {
                slot.insert(IndexedPayment {
                    position,
                    repeated: false,
                });
            }
            Entry::Occupied(mut slot) => {
                if !slot.get().repeated {
                    duplicates.push(payment.id.clone());
                    slot.get_mut().repeated = true;
                }
            }
        }
    }
    (index, duplicates)
}

#[cfg(test)]
pub(crate) fn reconcile_payments(
    internal: &[Payment],
    provider: &[Payment],
) -> Result<Vec<ReconciliationEntry>, ReconciliationError> {
    reconcile_with(internal, provider, |entry| entry)
}

pub(crate) fn reconcile_report_entries(
    internal: &[Payment],
    provider: &[Payment],
) -> Result<Vec<crate::report::ReportEntry>, ReconciliationError> {
    reconcile_with(internal, provider, Into::into)
}

fn reconcile_with<T>(
    internal_payments: &[Payment],
    provider_payments: &[Payment],
    mut convert: impl FnMut(ReconciliationEntry) -> T,
) -> Result<Vec<T>, ReconciliationError> {
    // Preserve validation/error precedence, including provider blank IDs taking
    // priority over internal duplicates.
    for (source, payments) in [
        (PaymentSource::Internal, internal_payments),
        (PaymentSource::Provider, provider_payments),
    ] {
        for (index, payment) in payments.iter().enumerate() {
            if validate_payment_id(&payment.id).is_err() {
                return Err(ReconciliationError::InvalidPaymentId { source, index });
            }
        }
    }
    let (internal_index, duplicates) = index(internal_payments);
    if !duplicates.is_empty() {
        return Err(ReconciliationError::DuplicateInternalIds(duplicates));
    }
    // Membership on the second pass is tracked by provider position instead.
    drop(internal_index);
    let (provider_index, duplicates) = index(provider_payments);
    if !duplicates.is_empty() {
        return Err(ReconciliationError::DuplicateProviderIds(duplicates));
    }
    let mut seen = vec![false; provider_payments.len()];
    let mut entries = Vec::with_capacity(internal_payments.len().max(provider_payments.len()));
    for internal in internal_payments {
        let provider = provider_index.get(internal.id.as_str()).map(|indexed| {
            seen[indexed.position] = true;
            &provider_payments[indexed.position]
        });
        entries.push(convert(ReconciliationEntry {
            payment_id: internal.id.clone(),
            result: provider.map_or(ReconciliationResult::MissingFromProvider, |p| {
                reconcile(internal, p)
            }),
            internal_amount_minor: Some(internal.amount_minor),
            provider_amount_minor: provider.map(|p| p.amount_minor),
            internal_currency: Some(internal.currency),
            provider_currency: provider.map(|p| p.currency),
            internal_status: Some(internal.status),
            provider_status: provider.map(|p| p.status),
        }));
    }
    for (position, provider) in provider_payments.iter().enumerate() {
        if !seen[position] {
            entries.push(convert(ReconciliationEntry {
                payment_id: provider.id.clone(),
                result: ReconciliationResult::MissingFromInternal,
                internal_amount_minor: None,
                provider_amount_minor: Some(provider.amount_minor),
                internal_currency: None,
                provider_currency: Some(provider.currency),
                internal_status: None,
                provider_status: Some(provider.status),
            }));
        }
    }
    Ok(entries)
}
/// Identifies the snapshot containing an invalid payment.
#[derive(Debug, PartialEq, Clone, Copy, Serialize, utoipa::ToSchema)]
pub enum PaymentSource {
    Internal,
    Provider,
}

impl std::fmt::Display for ReconciliationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPaymentId { source, index } => write!(
                f,
                "{source:?} payment at index {index}: payment id must not be empty or contain only whitespace"
            ),
            Self::DuplicateInternalIds(ids) => write!(f, "Duplicate internal payment IDs: {ids:?}"),
            Self::DuplicateProviderIds(ids) => write!(f, "Duplicate provider payment IDs: {ids:?}"),
        }
    }
}

impl std::error::Error for ReconciliationError {}
