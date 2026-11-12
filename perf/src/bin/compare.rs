use reconciliation_perf::{Input, check_parity, dataset, implementations, stats};
use serde_json::json;
use std::{hint::black_box, time::Instant};
fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let samples: usize = args.first().map(|s| s.parse().unwrap()).unwrap_or(15);
    assert!((5..=100).contains(&samples));
    let sizes: Vec<usize> = if args.len() > 1 {
        args[1..].iter().map(|s| s.parse().unwrap()).collect()
    } else {
        vec![1000, 10_000, 100_000]
    };
    let mut results = Vec::new();
    for n in sizes {
        assert!((1..=1_000_000).contains(&n));
        for scenario in ["matched", "mixed", "long_ids", "duplicates"] {
            let input = dataset(n, scenario);
            check_parity(&input);
            let bytes = serde_json::to_vec(&input).unwrap();
            for boundary in ["typed_to_report_and_drop", "json_to_json_and_drop"] {
                let mut times: [Vec<f64>; 3] = std::array::from_fn(|_| Vec::new());
                for iteration in 0..samples + 2 {
                    // Rotate execution order to reduce fixed-order thermal/cache bias.
                    for offset in 0..3 {
                        let k = (iteration + offset) % 3;
                        let (_, run) = implementations()[k];
                        let start = Instant::now();
                        if boundary == "typed_to_report_and_drop" {
                            let report = run(
                                black_box(&input.internal_payments),
                                black_box(&input.provider_payments),
                            );
                            black_box(&report);
                            drop(report);
                        } else {
                            let parsed: Input = serde_json::from_slice(black_box(&bytes)).unwrap();
                            let report = run(&parsed.internal_payments, &parsed.provider_payments);
                            let output = match &report {
                                Ok(r) => serde_json::to_vec(r).unwrap(),
                                Err(e) => serde_json::to_vec(e).unwrap(),
                            };
                            black_box(&output);
                            drop(output);
                            drop(report);
                            drop(parsed);
                        }
                        let elapsed = start.elapsed().as_secs_f64() * 1000.;
                        if iteration >= 2 {
                            times[k].push(elapsed);
                        }
                    }
                }
                for (k, (name, _)) in implementations().iter().enumerate() {
                    let summary = stats(&times[k]);
                    results.push(json!({
                "implementation":name,"payments_per_source":n,"scenario":scenario,"boundary":boundary,
                "input_records":input.internal_payments.len()+input.provider_payments.len(),"request_bytes":bytes.len(),
                "operation":if scenario=="duplicates"{"validate_and_reject"}else{"complete_reconciliation"},
                "latency":summary,"raw_ms":times[k],"warmups":2,"output_parity_checked":true
            }));
                }
            }
            eprintln!("Compared {n} payments/source: {scenario}");
        }
    }
    println!("{}",serde_json::to_string_pretty(&json!({"engine_version":env!("ENGINE_VERSION"),"generated_at":chrono::Utc::now().to_rfc3339(),
        "sqlite_version":rusqlite::version(),"samples_per_case":samples,"order":"rotating interleaved","results":results,
        "scope":"SQLite is an in-memory indexed ID join with Rust report assembly; load, index creation and connection teardown are included. No disk durability, remote database, or preloaded-index claims."})).unwrap());
}
