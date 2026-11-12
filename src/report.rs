use crate::payment::{Currency, PaymentStatus};
use serde::Serialize;

use crate::reconciliation::{ReconciliationEntry, ReconciliationResult};
use crate::recovery::{NextAction, recommend_action};

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ReportEntry {
    pub payment_id: String,
    pub result: ReconciliationResult,
    pub recommended_action: NextAction,
    // None serializes as null: the source did not contain this payment.
    pub internal_amount_minor: Option<u64>,
    pub provider_amount_minor: Option<u64>,
    pub internal_currency: Option<Currency>,
    pub provider_currency: Option<Currency>,
    pub internal_status: Option<PaymentStatus>,
    pub provider_status: Option<PaymentStatus>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ReportSummary {
    pub total_entries: usize,
    pub matched: usize,
    pub discrepancies: usize,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ReconciliationReport {
    pub metadata: crate::audit::ReportMetadata,
    pub summary: ReportSummary,
    pub entries: Vec<ReportEntry>,
}

impl From<ReconciliationEntry> for ReportEntry {
    fn from(entry: ReconciliationEntry) -> Self {
        Self {
            recommended_action: recommend_action(&entry.result),
            payment_id: entry.payment_id,
            result: entry.result,
            internal_amount_minor: entry.internal_amount_minor,
            provider_amount_minor: entry.provider_amount_minor,
            internal_currency: entry.internal_currency,
            provider_currency: entry.provider_currency,
            internal_status: entry.internal_status,
            provider_status: entry.provider_status,
        }
    }
}

#[cfg(test)]
pub(crate) fn build_report(entries: Vec<ReconciliationEntry>) -> ReconciliationReport {
    from_report_entries(entries.into_iter().map(Into::into).collect())
}

pub(crate) fn from_report_entries(entries: Vec<ReportEntry>) -> ReconciliationReport {
    let total_entries = entries.len();
    let matched = entries
        .iter()
        .filter(|entry| entry.result == ReconciliationResult::Matched)
        .count();
    ReconciliationReport {
        metadata: crate::audit::ReportMetadata::new(),
        summary: ReportSummary {
            total_entries,
            matched,
            discrepancies: total_entries - matched,
        },
        entries,
    }
}
