use std::fs;

use hello_cargo::Payment;

pub(crate) fn load_payments(path: &str) -> Result<Vec<Payment>, String> {
    let contents =
        fs::read_to_string(path).map_err(|error| format!("Could not read {path}: {error}"))?;

    let payments: Vec<Payment> = serde_json::from_str(&contents)
        .map_err(|error| format!("Invalid payment JSON in {path}: {error}"))?;

    Ok(payments)
}
