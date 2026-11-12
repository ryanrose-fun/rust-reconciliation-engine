use crate::reconciliation::ReconciliationResult;
use serde::Serialize;

#[derive(Debug, PartialEq, Serialize, utoipa::ToSchema)]
pub enum NextAction {
    ReviewPaymentStatus,
    InvestigateCurrencyAndValues,
    InvestigateAmount,
    InvestigateAmountAndStatus,
    RecheckSource,
    InvestigateIdentity,
}

pub fn recommend_action(result: &ReconciliationResult) -> NextAction {
    match result {
        ReconciliationResult::AmountAndStatusMismatch => NextAction::InvestigateAmountAndStatus,
        ReconciliationResult::CurrencyMismatch
        | ReconciliationResult::AmountAndCurrencyMismatch
        | ReconciliationResult::StatusAndCurrencyMismatch
        | ReconciliationResult::AmountStatusAndCurrencyMismatch => {
            NextAction::InvestigateCurrencyAndValues
        }
        ReconciliationResult::Matched => NextAction::ReviewPaymentStatus,
        ReconciliationResult::AmountMismatch => NextAction::InvestigateAmount,
        ReconciliationResult::StatusMismatch
        | ReconciliationResult::MissingFromProvider
        | ReconciliationResult::MissingFromInternal => NextAction::RecheckSource,
        ReconciliationResult::DifferentPaymentIds => NextAction::InvestigateIdentity,
    }
}
