//! Simulated checkout/provider integration. Run via run-partitioned-integration.ps1.
use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    routing::post,
};
use reconcile_cli::{
    Currency, Payment, PaymentStatus,
    client::{EventClient, RetryPolicy},
    events::{EventSource, EventStore, PaymentEvent},
    webhooks::{WebhookWorker, verify_signature},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Clone)]
struct Receiver {
    secret: Arc<Vec<u8>>,
    journal: Arc<Mutex<(std::fs::File, HashSet<String>)>>,
}
async fn alert(State(state): State<Receiver>, request: Request) -> StatusCode {
    let headers = request.headers().clone();
    let Ok(body) = axum::body::to_bytes(request.into_body(), 65536).await else {
        return StatusCode::PAYLOAD_TOO_LARGE;
    };
    let header = |key| headers.get(key).and_then(|v| v.to_str().ok()).unwrap_or("");
    let id = header("x-reconcile-delivery");
    if !verify_signature(
        &state.secret,
        header("x-reconcile-timestamp"),
        id,
        &body,
        header("x-reconcile-signature"),
        chrono::Utc::now().timestamp_millis(),
    ) {
        return StatusCode::UNAUTHORIZED;
    }
    let Ok(value) = serde_json::from_slice::<Value>(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    let mut journal = state.journal.lock().unwrap_or_else(|e| e.into_inner());
    if journal.1.contains(id) {
        return StatusCode::OK;
    }
    // Demo receiver acknowledges only after its journal is durable.
    let mut line = body.to_vec();
    line.push(b'\n');
    if journal
        .0
        .write_all(&line)
        .and_then(|_| journal.0.sync_all())
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    journal.1.insert(id.into());
    println!(
        "ALERT {}: {} -> {}",
        value["payment"]["payment_id"], value["kind"], value["payment"]["status"]
    );
    StatusCode::OK
}
fn event(
    id: &str,
    source: EventSource,
    version: u64,
    status: PaymentStatus,
    amount: u64,
) -> PaymentEvent {
    PaymentEvent {
        event_id: format!(
            "{:x}:{}",
            Sha256::digest(id.as_bytes()),
            uuid::Uuid::new_v4().simple()
        ),
        source,
        version,
        payment: Payment {
            id: id.into(),
            currency: Currency::GBP,
            amount_minor: amount,
            status,
        },
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::var("DEMO_API_URL")?;
    let root = PathBuf::from(std::env::var("DEMO_RUN_DIRECTORY")?);
    let internal = EventClient::new(
        &base,
        std::env::var("DEMO_INTERNAL_KEY")?,
        RetryPolicy::default(),
    )?;
    let provider = EventClient::new(
        &base,
        std::env::var("DEMO_PROVIDER_KEY")?,
        RetryPolicy::default(),
    )?;
    let auditor = std::env::var("DEMO_AUDITOR_KEY")?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let discovery: Value = serde_json::from_slice(
        &http
            .get(format!("{base}/v1/partitions"))
            .bearer_auth(&auditor)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?,
    )?;
    let count = discovery["count"]
        .as_u64()
        .ok_or("Missing partition count")? as usize;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/alerts", listener.local_addr()?);
    let secret = uuid::Uuid::new_v4().to_string().into_bytes();
    let state = Receiver {
        secret: Arc::new(secret.clone()),
        journal: Arc::new(Mutex::new((
            std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(root.join("alerts.jsonl"))?,
            HashSet::new(),
        ))),
    };
    let app = Router::new()
        .route("/alerts", post(alert))
        .with_state(state);
    let receiver = tokio::spawn(async move { axum::serve(listener, app).await });
    let mut workers = Vec::new();
    for index in 0..count {
        let store = Arc::new(EventStore::open(
            root.join(format!("partitions/partition-{index:02}.sqlite3")),
            Duration::from_secs(30),
        )?);
        workers.push(WebhookWorker::new(store, &endpoint, secret.clone())?);
    }
    // Persist every event before sending. Subsequent retries reuse exact IDs and bodies.
    let plan: Vec<Vec<PaymentEvent>> = (0..32)
        .map(|i| {
            let id = format!("CHECKOUT-{}-{i:02}", uuid::Uuid::new_v4().simple());
            vec![
                event(
                    &id,
                    EventSource::Internal,
                    1,
                    PaymentStatus::Completed,
                    10000,
                ),
                event(&id, EventSource::Provider, 1, PaymentStatus::Pending, 9500),
                event(
                    &id,
                    EventSource::Provider,
                    3,
                    PaymentStatus::Completed,
                    10000,
                ),
                event(&id, EventSource::Provider, 2, PaymentStatus::Pending, 9500),
            ]
        })
        .collect();
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join("event-plan.json"))?;
    file.write_all(&serde_json::to_vec_pretty(&plan)?)?;
    file.sync_all()?;
    let mut receipts = Vec::new();
    for (step, label) in [
        (0, "Checkout completed"),
        (1, "Provider delayed/pending with wrong amount"),
        (2, "Provider completes and corrects amount"),
        (0, "Retry original checkout event"),
        (3, "Late older provider update"),
    ] {
        if step == 1 {
            tokio::time::sleep(Duration::from_millis(750)).await;
        }
        println!("\n{label}");
        let client = if step == 0 { &internal } else { &provider };
        // Eight in-flight producers, bounded independently of the API queue.
        for chunk in plan.chunks(8) {
            let mut tasks = tokio::task::JoinSet::new();
            for payment in chunk {
                let event = payment[step].clone();
                let client = client.clone();
                tasks.spawn(async move { client.deliver(&event).await });
            }
            while let Some(result) = tasks.join_next().await {
                let delivery = result??;
                println!(
                    "{}: {}, duplicate={}",
                    delivery.receipt["payment"]["payment_id"],
                    delivery.receipt["outcome"],
                    delivery.receipt["duplicate_event"]
                );
                receipts.push(delivery.receipt);
            }
        }
        // Run the real durable webhook sender until idle, bounded to 10 seconds/partition.
        for worker in &workers {
            tokio::time::timeout(Duration::from_secs(10), async {
                while worker.deliver_one().await? {}
                Ok::<(), reconcile_cli::events::EventError>(())
            })
            .await??;
        }
    }
    let id = &plan[0][0].payment.id;
    let history = http
        .get(format!("{base}/v1/payments/{id}/history"))
        .bearer_auth(&auditor)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    std::fs::write(root.join("sample-history.json"), history)?;
    std::fs::write(
        root.join("receipts.json"),
        serde_json::to_vec_pretty(&receipts)?,
    )?;
    for index in 0..count {
        let metrics = http
            .get(format!("{base}/metrics"))
            .bearer_auth(&auditor)
            .header("x-reconcile-partition", index.to_string())
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        std::fs::write(root.join(format!("partition-{index}-metrics.txt")), metrics)?;
    }
    receiver.abort();
    println!(
        "\nSaved event-plan.json, receipts.json, alerts.jsonl, sample-history.json and partition metrics in {}",
        root.display()
    );
    println!("This is a lifecycle demonstration, not a throughput benchmark.");
    Ok(())
}
