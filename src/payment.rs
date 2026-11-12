use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Copy, Deserialize, Serialize, utoipa::ToSchema)]
pub enum PaymentStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct Payment {
    #[serde(deserialize_with = "deserialize_payment_id")]
    #[schema(min_length = 1, pattern = "\\S")]
    pub id: String,
    pub currency: Currency,
    pub amount_minor: u64,
    pub status: PaymentStatus,
}

// Validate incoming IDs without changing the identifier supplied by the source.
fn deserialize_payment_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let id = String::deserialize(deserializer)?;

    validate_payment_id(&id).map_err(serde::de::Error::custom)?;

    Ok(id)
}

// Shared by JSON deserialization and the engine's validation of Rust values.
pub(crate) fn validate_payment_id(id: &str) -> Result<(), &'static str> {
    if id.trim().is_empty() {
        Err("payment id must not be empty or contain only whitespace")
    } else {
        Ok(())
    }
}

/// Supported currencies. Amounts use the currency's minor unit; no conversion is performed.
#[derive(Debug, PartialEq, Clone, Copy, Deserialize, Serialize, utoipa::ToSchema)]
pub enum Currency {
    GBP,
    USD,
    EUR,
    JPY,
}

impl Currency {
    pub fn minor_unit_exponent(self) -> u8 {
        match self {
            Self::JPY => 0,
            _ => 2,
        }
    }
}
