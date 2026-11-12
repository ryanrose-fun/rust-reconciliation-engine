use super::*;
use std::{future::Future, task::Poll, time::Duration};

fn state() -> AppState {
    AppState {
        event_store: None,
        event_queue: Arc::new(EventQueue::new(2)),
        config: Config {
            timeout: Duration::from_millis(20),
            ..Config::default()
        },
        workers: Arc::new(Semaphore::new(1)),
        requests: Arc::new(Semaphore::new(1)),
        event_admission: Arc::new(Semaphore::new(2)),
        rate: Arc::new(Mutex::new((Instant::now(), 0))),
        rejection_drains: Arc::new(Semaphore::new(1)),
    }
}
async fn pending<F: Future>(mut future: std::pin::Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn event_admission_is_bounded_and_fifo() {
    let state = state();
    let headers = axum::http::HeaderMap::new();
    let active = state.requests.clone().acquire_owned().await.unwrap();
    let first = admit(&state, &headers, true);
    let second = admit(&state, &headers, true);
    tokio::pin!(first, second);
    pending(first.as_mut()).await;
    pending(second.as_mut()).await;
    let error = admit(&state, &headers, true).await.err().unwrap();
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    drop(active);
    let first = first
        .await
        .unwrap_or_else(|_| panic!("first waiter rejected"));
    pending(second.as_mut()).await;
    drop(first);
    let second = second
        .await
        .unwrap_or_else(|_| panic!("second waiter rejected"));
    drop(second);
    assert_eq!(state.requests.available_permits(), 1);
    assert_eq!(state.event_admission.available_permits(), 2);
}

#[tokio::test]
async fn cancelling_admission_releases_its_bounded_slot() {
    let state = state();
    let headers = axum::http::HeaderMap::new();
    let active = state.requests.clone().acquire_owned().await.unwrap();
    {
        let waiting = admit(&state, &headers, true);
        tokio::pin!(waiting);
        pending(waiting.as_mut()).await;
        assert_eq!(state.event_admission.available_permits(), 1);
    }
    assert_eq!(state.event_admission.available_permits(), 2);
    drop(active);
    assert!(admit(&state, &headers, true).await.is_ok());
}

#[tokio::test]
async fn admission_wait_uses_request_deadline_and_returns_json() {
    use axum::body::{Body, to_bytes};
    use tower::ServiceExt;
    let state = state();
    let active = state.requests.clone().acquire_owned().await.unwrap();
    let app = Router::new()
        .route("/v1/events", post(|| async { "unexpected" }))
        .route_layer(middleware::from_fn_with_state(state.clone(), guard));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/events")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let value: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16384).await.unwrap()).unwrap();
    assert_eq!(value["error"]["code"], "timeout");
    assert_eq!(state.event_admission.available_permits(), 2);
    assert_eq!(state.requests.available_permits(), 0);
    drop(active);
}
