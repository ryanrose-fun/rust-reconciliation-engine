mod input;

use std::{env, fs};

use input::load_payments;
use reconcile_cli::reconcile_report;

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.len() != 3 {
        return Err(
            "Usage: cargo run -- <internal.json> <provider.json> <report.json>".to_string(),
        );
    }

    let internal_path = &args[0];
    let provider_path = &args[1];
    let report_path = &args[2];

    let internal_payments = load_payments(internal_path)?;
    let provider_payments = load_payments(provider_path)?;

    let report = reconcile_report(&internal_payments, &provider_payments)
        .map_err(|error| format!("Cannot reconcile: {error}"))?;

    let json = serde_json::to_string_pretty(&report)
        .map_err(|error| format!("Could not create report JSON: {error}"))?;

    fs::write(report_path, json)
        .map_err(|error| format!("Could not write {report_path}: {error}"))?;

    println!("Report saved to {report_path}");

    Ok(())
}
