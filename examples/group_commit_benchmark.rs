use reconcile_cli::{
    Currency, Payment, PaymentStatus,
    events::{EventSource, EventStore, PaymentEvent},
};
use std::{
    fs,
    time::{Duration, Instant},
};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!("reconcile-group-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root)?;
    let mut results = Vec::new();
    for round in 0..3 {
        for size in [1, 8, 32, 128] {
            let store = EventStore::open(
                root.join(format!("{round}-{size}.sqlite3")),
                Duration::from_secs(30),
            )?;
            let events: Vec<_> = (0..4096)
                .map(|i| PaymentEvent {
                    event_id: format!("e{i}"),
                    source: EventSource::Internal,
                    version: 1,
                    payment: Payment {
                        id: format!("p{i}"),
                        currency: Currency::GBP,
                        amount_minor: 100,
                        status: PaymentStatus::Completed,
                    },
                })
                .collect();
            let begin = Instant::now();
            for batch in events.chunks(size) {
                store.apply_batch(batch.to_vec())?;
            }
            let seconds = begin.elapsed().as_secs_f64();
            drop(store);
            let reopened = EventStore::open(
                root.join(format!("{round}-{size}.sqlite3")),
                Duration::from_secs(30),
            )?;
            for e in &events {
                assert_eq!(reopened.status(&e.payment.id)?.internal.unwrap().version, 1);
            }
            results.push(serde_json::json!({"round":round,"batch_size":size,"events":4096,"seconds":seconds,"events_per_second":4096.0/seconds,"reopen_validation":true}));
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"database_directory":root,"cases":results})
        )?
    );
    Ok(())
}
