#![cfg(feature = "client")]
use axum::{
    Router,
    extract::{Request, State},
    response::IntoResponse,
    routing::post,
};
use reconcile_cli::{
    Currency, Payment, PaymentStatus,
    client::{EventClient, RetryPolicy},
    events::{EventSource, EventStore, PaymentEvent},
    outbox::Outbox,
    webhooks::{WebhookWorker, verify_signature},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
type Received = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

#[test]
fn business_change_and_enqueue_share_one_commit() {
    let db = std::env::temp_dir().join(format!("atomic-outbox-{}.sqlite3", uuid::Uuid::new_v4()));
    let outbox = Outbox::open(&db).unwrap();
    let mut c = rusqlite::Connection::open(&db).unwrap();
    c.execute("CREATE TABLE orders(id TEXT PRIMARY KEY)", [])
        .unwrap();
    {
        let tx = c.transaction().unwrap();
        tx.execute("INSERT INTO orders VALUES('order-1')", [])
            .unwrap();
        Outbox::enqueue_in_transaction(
            &tx,
            &event(EventSource::Internal, 1, PaymentStatus::Completed),
        )
        .unwrap();
    }
    assert_eq!(outbox.summary().unwrap().pending, 0);
    assert_eq!(
        c.query_row("SELECT count(*) FROM orders", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    {
        let tx = c.transaction().unwrap();
        tx.execute("INSERT INTO orders VALUES('order-1')", [])
            .unwrap();
        Outbox::enqueue_in_transaction(
            &tx,
            &event(EventSource::Internal, 1, PaymentStatus::Completed),
        )
        .unwrap();
        tx.commit().unwrap();
    }
    assert_eq!(outbox.summary().unwrap().pending, 1);
    assert_eq!(
        c.query_row("SELECT count(*) FROM orders", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    drop(c);
    drop(outbox);
    std::fs::remove_file(db).unwrap();
}
fn event(source: EventSource, version: u64, status: PaymentStatus) -> PaymentEvent {
    PaymentEvent {
        event_id: format!("{source:?}-{version}"),
        source,
        version,
        payment: Payment {
            id: "payment".into(),
            currency: Currency::GBP,
            amount_minor: 100,
            status,
        },
    }
}
#[tokio::test]
async fn persisted_outbox_recovers_lost_receipt_after_reopen() {
    let db = std::env::temp_dir().join(format!("outbox-{}.sqlite3", uuid::Uuid::new_v4()));
    let store = Arc::new(EventStore::in_memory(Duration::from_secs(30)).unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    async fn receive(
        State((store, calls)): State<(Arc<EventStore>, Arc<AtomicUsize>)>,
        request: Request,
    ) -> axum::response::Response {
        let body = axum::body::to_bytes(request.into_body(), 16384)
            .await
            .unwrap();
        let receipt = store.apply(serde_json::from_slice(&body).unwrap()).unwrap();
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        axum::Json(receipt).into_response()
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/v1/events", post(receive))
        .with_state((store.clone(), calls.clone()));
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let e = event(EventSource::Internal, 1, PaymentStatus::Completed);
    {
        let outbox = Outbox::open(&db).unwrap();
        assert!(outbox.enqueue(&e).unwrap());
        assert!(!outbox.enqueue(&e).unwrap());
        let mut changed = e.clone();
        changed.payment.amount_minor = 2;
        assert!(outbox.enqueue(&changed).is_err());
    }
    let client = EventClient::new(
        &base,
        "k".repeat(32),
        RetryPolicy {
            max_attempts: 1,
            attempt_timeout: Duration::from_millis(100),
            total_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .unwrap();
    {
        let outbox = Arc::new(Outbox::open(&db).unwrap());
        assert!(outbox.deliver_one(&client).await.unwrap());
        assert_eq!(outbox.summary().unwrap().pending, 1);
    }
    // Advance persisted retry schedule deterministically instead of sleeping backoff.
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute("UPDATE delivery_jobs SET available_at=0", [])
        .unwrap();
    {
        let outbox = Arc::new(Outbox::open(&db).unwrap());
        let wrong = EventClient::new(&base, "x".repeat(32), RetryPolicy::default()).unwrap();
        assert!(outbox.deliver_one(&wrong).await.is_err());
        assert!(outbox.deliver_one(&client).await.unwrap());
        assert_eq!(outbox.summary().unwrap().delivered, 1);
        assert!(!outbox.deliver_one(&client).await.unwrap());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(store.history("payment", 0, 100).unwrap().entries.len(), 1);
    task.abort();
    std::fs::remove_file(db).unwrap();
}
#[tokio::test]
async fn signed_webhook_retries_preserve_identity_and_survive_restart() {
    let secret = vec![b's'; 32];
    let received = Arc::new(Mutex::new(Vec::<(String, Vec<u8>)>::new()));
    async fn receive(
        State(received): State<Received>,
        request: Request,
    ) -> axum::response::Response {
        let headers = request.headers().clone();
        let body = axum::body::to_bytes(request.into_body(), 65536)
            .await
            .unwrap();
        let id = headers["x-reconcile-delivery"].to_str().unwrap();
        assert!(verify_signature(
            &[b's'; 32],
            headers["x-reconcile-timestamp"].to_str().unwrap(),
            id,
            &body,
            headers["x-reconcile-signature"].to_str().unwrap(),
            chrono::Utc::now().timestamp_millis()
        ));
        let mut records = received.lock().unwrap();
        records.push((id.into(), body.to_vec()));
        if records.len() == 1 {
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                [("retry-after", "0")],
            )
                .into_response()
        } else {
            axum::http::StatusCode::NO_CONTENT.into_response()
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/hook", post(receive))
        .with_state(received.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let db = std::env::temp_dir().join(format!("webhooks-{}.sqlite3", uuid::Uuid::new_v4()));
    {
        let s = Arc::new(EventStore::open(&db, Duration::from_secs(30)).unwrap());
        let worker = WebhookWorker::new(s.clone(), &url, secret.clone()).unwrap();
        s.apply(event(EventSource::Internal, 1, PaymentStatus::Completed))
            .unwrap();
        s.apply(event(EventSource::Provider, 1, PaymentStatus::Pending))
            .unwrap();
        assert!(worker.deliver_one().await.unwrap());
        assert_eq!(s.notification_summary().unwrap().pending, 1);
        assert!(
            WebhookWorker::new(s.clone(), "https://other.example/hook", secret.clone()).is_err()
        );
    }
    {
        let s = Arc::new(EventStore::open(&db, Duration::from_secs(30)).unwrap());
        let worker = WebhookWorker::new(s.clone(), &url, secret).unwrap();
        assert!(worker.deliver_one().await.unwrap());
        assert_eq!(s.notification_summary().unwrap().delivered, 1);
        assert!(!worker.deliver_one().await.unwrap());
    }
    let rows = received.lock().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], rows[1]);
    let body: serde_json::Value = serde_json::from_slice(&rows[0].1).unwrap();
    assert_eq!(body["kind"], "Opened");
    drop(rows);
    assert!(!verify_signature(
        &[b's'; 32],
        "0",
        "id",
        b"body",
        &"0".repeat(64),
        chrono::Utc::now().timestamp_millis()
    ));
    task.abort();
    std::fs::remove_file(db).unwrap();
}
