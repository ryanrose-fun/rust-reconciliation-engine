#![cfg(feature = "client")]
use axum::{
    Router,
    extract::{Request, State},
    response::IntoResponse,
    routing::post,
};
use reconcile_cli::{
    Currency, Payment, PaymentStatus,
    client::{EventClient, Failure, RetryPolicy},
    events::{EventSource, EventStore, PaymentEvent},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
#[derive(Clone)]
struct Mock {
    store: Arc<EventStore>,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    calls: Arc<AtomicUsize>,
    status: u16,
}
async fn handler(State(mock): State<Mock>, request: Request) -> axum::response::Response {
    let bytes = axum::body::to_bytes(request.into_body(), 16384)
        .await
        .unwrap();
    mock.bodies.lock().unwrap().push(bytes.to_vec());
    let n = mock.calls.fetch_add(1, Ordering::SeqCst);
    if mock.status != 200 {
        return (
            axum::http::StatusCode::from_u16(mock.status).unwrap(),
            [("retry-after", "3600")],
            "{}",
        )
            .into_response();
    }
    let receipt = mock
        .store
        .apply(serde_json::from_slice(&bytes).unwrap())
        .unwrap();
    if n == 0 {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    axum::Json(receipt).into_response()
}
async fn server(status: u16) -> (String, Mock, tokio::task::JoinHandle<()>) {
    let mock = Mock {
        store: Arc::new(EventStore::in_memory(Duration::from_secs(30)).unwrap()),
        bodies: Default::default(),
        calls: Default::default(),
        status,
    };
    let app = Router::new()
        .route("/v1/events", post(handler))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, mock, task)
}
fn event() -> PaymentEvent {
    PaymentEvent {
        event_id: "stable-id".into(),
        source: EventSource::Internal,
        version: 1,
        payment: Payment {
            id: "p".into(),
            amount_minor: 100,
            currency: Currency::GBP,
            status: PaymentStatus::Completed,
        },
    }
}
fn policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        attempt_timeout: Duration::from_millis(100),
        total_timeout: Duration::from_secs(2),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(20),
    }
}
#[tokio::test]
async fn committed_but_timed_out_receipt_retries_identical_event_once() {
    let (base, mock, task) = server(200).await;
    let client = EventClient::new(&base, "k".repeat(32), policy()).unwrap();
    let delivered = client.deliver(&event()).await.unwrap();
    assert_eq!(delivered.attempts, 2);
    assert_eq!(delivered.receipt["duplicate_event"], true);
    let bodies = mock.bodies.lock().unwrap();
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0], bodies[1]);
    drop(bodies);
    let history = mock.store.history("p", 0, 100).unwrap();
    assert_eq!(history.entries.len(), 1);
    assert_eq!(mock.store.status("p").unwrap().internal.unwrap().version, 1);
    task.abort();
}
#[tokio::test]
async fn permanent_errors_do_not_retry_and_retry_after_cannot_exceed_budget() {
    for status in [401, 403, 409, 422, 503] {
        let (base, mock, task) = server(status).await;
        let client = EventClient::new(&base, "k".repeat(32), policy()).unwrap();
        let error = client.deliver(&event()).await.unwrap_err();
        assert_eq!(error.attempts, 1);
        assert_eq!(mock.calls.load(Ordering::SeqCst), 1);
        if status == 503 {
            assert!(matches!(error.reason, Failure::Deadline));
            assert!(error.outcome_unknown);
        } else {
            assert!(matches!(error.reason, Failure::Rejected(_)));
            assert!(!error.outcome_unknown);
        }
        task.abort();
    }
}
#[test]
fn client_rejects_unsafe_urls_and_unbounded_policies() {
    for url in [
        "http://example.com",
        "https://user:secret@example.com",
        "https://example.com?token=x",
        "file:///tmp/foo",
    ] {
        assert!(EventClient::new(url, "k".repeat(32), policy()).is_err());
    }
    let bad = RetryPolicy {
        max_attempts: 0,
        ..policy()
    };
    assert!(EventClient::new("http://127.0.0.1:3000", "k".repeat(32), bad).is_err());
}

#[tokio::test]
async fn exhausted_timeout_preserves_identity_and_reports_uncertain_commit() {
    let (base, mock, task) = server(200).await;
    let client = EventClient::new(
        &base,
        "k".repeat(32),
        RetryPolicy {
            max_attempts: 1,
            ..policy()
        },
    )
    .unwrap();
    let error = client.deliver(&event()).await.unwrap_err();
    assert!(error.outcome_unknown);
    assert_eq!(error.event_id, "stable-id");
    assert_eq!(error.attempts, 1);
    assert_eq!(mock.calls.load(Ordering::SeqCst), 1);
    assert_eq!(mock.store.status("p").unwrap().internal.unwrap().version, 1);
    let retry = client.deliver(&event()).await.unwrap();
    assert_eq!(retry.receipt["duplicate_event"], true);
    assert_eq!(mock.store.history("p", 0, 100).unwrap().entries.len(), 1);
    task.abort();
}

#[tokio::test]
async fn transient_http_failures_retry_only_up_to_the_attempt_limit() {
    async fn busy_then_ready(State(calls): State<Arc<AtomicUsize>>) -> axum::response::Response {
        if calls.fetch_add(1, Ordering::SeqCst) < 2 {
            return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        axum::Json(serde_json::json!({"event_id":"stable-id","duplicate_event":false}))
            .into_response()
    }
    for max_attempts in [2, 3] {
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/v1/events", post(busy_then_ready))
            .with_state(calls.clone());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = EventClient::new(
            &base,
            "k".repeat(32),
            RetryPolicy {
                max_attempts,
                ..policy()
            },
        )
        .unwrap();
        let result = client.deliver(&event()).await;
        assert_eq!(calls.load(Ordering::SeqCst), max_attempts);
        if max_attempts == 2 {
            let error = result.unwrap_err();
            assert_eq!(error.attempts, 2);
            assert!(error.outcome_unknown);
            assert!(matches!(error.reason, Failure::Rejected(503)));
        } else {
            assert_eq!(result.unwrap().attempts, 3);
        }
        task.abort();
    }
}
