//! Payment reconciliation shared by command-line and future HTTP clients.
//! Snapshot reconciliation is in-memory; the ledger module provides SQLite persistence.
mod audit;
pub mod events;
pub mod ledger;
mod payment;
mod reconciliation;
mod recovery;
mod report;

pub use audit::{ReportMetadata, SourceReference};
pub use payment::{Currency, Payment, PaymentStatus};
pub use reconciliation::{PaymentSource, ReconciliationError, ReconciliationResult};
pub use recovery::NextAction;
pub use report::{ReconciliationReport, ReportEntry, ReportSummary};

#[cfg(test)]
mod tests;

/// Validate two snapshots and return their reconciliation report.
///
/// Blank IDs and duplicate IDs are errors. Differences between valid
/// snapshots are findings in a successful report, not request errors.
/// Entries follow internal input order, followed by provider-only entries.
pub fn reconcile_report(
    internal: &[Payment],
    provider: &[Payment],
) -> Result<ReconciliationReport, ReconciliationError> {
    let entries = reconciliation::reconcile_report_entries(internal, provider)?;
    let mut report = report::from_report_entries(entries);
    report.metadata.internal_source = audit::source_reference(internal);
    report.metadata.provider_source = audit::source_reference(provider);
    Ok(report)
}
