//! Reproducible single-thread in-memory benchmark; not an HTTP capacity claim.
use hello_cargo::{Currency, Payment, PaymentStatus, reconcile_report};
use serde_json::json;
use std::{hint::black_box, time::Instant};
fn main() {
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .map(|s| s.parse().expect("sizes must be integers"))
        .collect();
    let sizes = if sizes.is_empty() {
        vec![1000, 10_000, 100_000]
    } else {
        sizes
    };
    let mut results = Vec::new();
    for n in sizes {
        assert!((1..=1_000_000).contains(&n));
        for scenario in ["matched", "mixed", "long_ids", "duplicates"] {
            let internal: Vec<_> = (0..n)
                .map(|i| Payment {
                    id: if scenario == "long_ids" {
                        format!("{:0>240}-{i}", "P")
                    } else {
                        format!("P{i}")
                    },
                    currency: Currency::GBP,
                    amount_minor: (i as u64) * 100,
                    status: PaymentStatus::Completed,
                })
                .collect();
            let mut provider = internal.clone();
            if scenario == "mixed" {
                for (i, p) in provider.iter_mut().enumerate() {
                    match i % 10 {
                        0 => p.amount_minor += 1,
                        1 => p.status = PaymentStatus::Pending,
                        2 => p.currency = Currency::USD,
                        3 => {
                            p.amount_minor += 1;
                            p.status = PaymentStatus::Pending;
                        }
                        4 => p.id = format!("provider-only-{i}"),
                        _ => {}
                    }
                }
            }
            if scenario == "duplicates" {
                provider.push(provider[0].clone());
            }
            provider.reverse();
            for _ in 0..3 {
                black_box(reconcile_report(&internal, &provider)).ok();
            }
            let mut times = Vec::new();
            for _ in 0..30 {
                let start = Instant::now();
                let result =
                    black_box(reconcile_report(black_box(&internal), black_box(&provider)));
                let ms = start.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(result.is_err(), scenario == "duplicates");
                times.push(ms);
                drop(result);
            }
            times.sort_by(f64::total_cmp);
            let mean = times.iter().sum::<f64>() / times.len() as f64;
            results.push(json!({"scenario":scenario,"payments_per_source":n,"samples":30,"warmups":3,
                "mean_ms":mean,"p50_ms":times[14],"p95_ms":times[28],
                "input_records_per_second":(internal.len()+provider.len()) as f64/(mean/1000.0),
                "operation":if scenario=="duplicates"{"validate_and_reject"}else{"validate_reconcile_report_and_fingerprint"}}));
        }
    }
    println!("{}",serde_json::to_string_pretty(&json!({
        "engine_version":env!("CARGO_PKG_VERSION"),"os":std::env::consts::OS,"arch":std::env::consts::ARCH,
        "logical_parallelism":std::thread::available_parallelism().map(|v|v.get()).unwrap_or(1),
        "generated_at":chrono::Utc::now().to_rfc3339(),"profile":"release required",
        "excludes":["dataset generation","input JSON parsing","output JSON encoding","network","disk","report destruction"],
        "results":results
    })).unwrap());
}
