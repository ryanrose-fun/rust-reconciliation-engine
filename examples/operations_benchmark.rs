//! Disk-backed WAL/FULL measurements; run in release mode on a local disk.
use reconcile_cli::{
    Currency, Payment, PaymentStatus,
    events::{EventSource, EventStore, PaymentEvent, recovery_drill},
};
use serde_json::json;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
fn percentiles(mut values: Vec<f64>) -> serde_json::Value {
    values.sort_by(f64::total_cmp);
    let at = |p: f64| {
        values[((values.len() as f64 * p).ceil() as usize)
            .saturating_sub(1)
            .min(values.len() - 1)]
    };
    json!({"p50":at(0.5),"p95":at(0.95),"p99":at(0.99),"max":values.last()})
}
fn event(id: &str, source: EventSource, version: u64, status: PaymentStatus) -> PaymentEvent {
    PaymentEvent {
        event_id: format!("{id}-{source:?}-{version}"),
        source,
        version,
        payment: Payment {
            id: id.into(),
            currency: Currency::GBP,
            amount_minor: 12500,
            status,
        },
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err("Usage: cargo run --release --example operations_benchmark -- <new-directory> <payments-per-case 1..4000>".into());
    }
    let root = PathBuf::from(&args[0]);
    let count: usize = args[1].parse()?;
    if !(1..=4000).contains(&count) {
        return Err("Count must be 1..4000".into());
    }
    std::fs::create_dir(&root)?;
    let mut results = vec![];
    for round in 1..=3 {
        for notifications in [false, true] {
            let dir = root.join(format!("round-{round}-notifications-{notifications}"));
            std::fs::create_dir(&dir)?;
            let db = dir.join("events.sqlite3");
            let store = EventStore::open(&db, Duration::from_secs(30))?;
            if notifications {
                rusqlite::Connection::open(&db)?.execute("INSERT INTO notification_settings VALUES(1,'http://127.0.0.1:1/benchmark-disabled')",[])?;
            }
            let mut latencies = vec![];
            let mut commit = vec![];
            let mut work = vec![];
            let start = Instant::now();
            for i in 0..count {
                let id = format!("p-{i:08}");
                for (source, version, status) in [
                    (EventSource::Internal, 1, PaymentStatus::Completed),
                    (EventSource::Provider, 1, PaymentStatus::Pending),
                    (EventSource::Provider, 2, PaymentStatus::Completed),
                ] {
                    let t = Instant::now();
                    let (_, stages) = store.apply_profiled(event(&id, source, version, status))?;
                    latencies.push(t.elapsed().as_secs_f64() * 1000.);
                    commit.push(stages.commit_ms);
                    work.push(stages.work_ms);
                }
            }
            let seconds = start.elapsed().as_secs_f64();
            let c = rusqlite::Connection::open(&db)?;
            let history: i64 =
                c.query_row("SELECT count(*) FROM payment_history", [], |r| r.get(0))?;
            let deliveries: i64 =
                c.query_row("SELECT count(*) FROM delivery_jobs", [], |r| r.get(0))?;
            assert_eq!(history, count as i64 * 3);
            assert_eq!(deliveries, if notifications { count as i64 * 2 } else { 0 });
            let bytes: u64 = std::fs::read_dir(&dir)?
                .map(|e| e.unwrap().metadata().unwrap().len())
                .sum();
            let drill = recovery_drill(&db, &dir.join("recovery"))?;
            results.push(json!({"round":round,"notifications_enqueued":notifications,"payments":count,"committed_events":count*3,"events_per_second":count as f64*3./seconds,"elapsed_seconds":seconds,"event_latency_ms":percentiles(latencies),"commit_ms":percentiles(commit),"transaction_work_ms":percentiles(work),"history_rows":history,"delivery_rows":deliveries,"database_and_wal_bytes":bytes,"recovery_drill_ms":drill.elapsed_ms,"validation_passed":drill.passed}));
        }
    }
    let deadline_db = root.join("deadlines.sqlite3");
    let store = EventStore::open(&deadline_db, Duration::from_millis(1))?;
    for i in 0..100 {
        store.apply(event(
            &format!("late-{i}"),
            EventSource::Internal,
            1,
            PaymentStatus::Completed,
        ))?;
    }
    std::thread::sleep(Duration::from_millis(2));
    let start = Instant::now();
    let processed = store.process_deadlines(100)?;
    let deadline_ms = start.elapsed().as_secs_f64() * 1000.;
    assert_eq!(processed, 100);
    let start = Instant::now();
    let cases = store.cases(Default::default())?;
    let case_query_ms = start.elapsed().as_secs_f64() * 1000.;
    assert_eq!(cases.entries.len(), 50);
    let report = json!({"engine_version":env!("CARGO_PKG_VERSION"),"scope":"single-thread storage; disk-backed SQLite WAL/FULL; event construction included; no HTTP or webhook delivery; 3 rounds, fresh database per case","os":std::env::consts::OS,"arch":std::env::consts::ARCH,"results":results,"deadline_batch_100_ms":deadline_ms,"case_query_first_50_ms":case_query_ms});
    let output = root.join("report.json");
    std::fs::write(&output, serde_json::to_vec_pretty(&report)?)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
