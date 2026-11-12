use super::*;
use axum::body::{Body, to_bytes};
use serde_json::{Value, json};
use tower::ServiceExt;
fn body(id: &str, source: &str, version: u64, status: &str) -> Value {
    json!({"event_id":id,"source":source,"version":version,"payment":{
        "id":"PAY-001","currency":"GBP","amount_minor":100,"status":status
    }})
}
async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    body: Value,
    authorized: bool,
) -> (StatusCode, Value, axum::http::HeaderMap) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if authorized {
        request = request.header("authorization", format!("Bearer {}", "k".repeat(32)));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap(), headers)
}
fn app() -> Router {
    configured_router(Config {
        api_key: Some("k".repeat(32)),
        ..Config::default()
    })
}
#[tokio::test]
async fn event_and_status_routes_require_authentication() {
    let app = app();
    for (method, path) in [("POST", "/v1/events"), ("GET", "/v1/payments/PAY-001")] {
        let (status, _, headers) = call(
            &app,
            method,
            path,
            body("e", "Internal", 1, "Completed"),
            false,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(headers["cache-control"], "no-store");
    }
}
#[tokio::test]
async fn event_lifecycle_conflicts_and_current_status_match_schema() {
    let app = app();
    let first = body("i1", "Internal", 1, "Completed");
    let (code, receipt, headers) = call(&app, "POST", "/v1/events", first.clone(), true).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(headers["cache-control"], "no-store");
    assert_eq!(receipt["payment"]["status"], "AwaitingProvider");
    let doc = api_doc();
    let schema = json!({"$ref":"#/components/schemas/EventReceipt","components":doc["components"]});
    assert!(
        jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&receipt)
    );
    let (_, retry, _) = call(&app, "POST", "/v1/events", first, true).await;
    assert_eq!(retry["duplicate_event"], true);
    let (code, conflict, _) = call(
        &app,
        "POST",
        "/v1/events",
        body("i1", "Internal", 2, "Failed"),
        true,
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(conflict["error"]["code"], "event_id_conflict");
    let (_, pending, _) = call(
        &app,
        "POST",
        "/v1/events",
        body("p1", "Provider", 1, "Pending"),
        true,
    )
    .await;
    assert_eq!(pending["payment"]["status"], "StatusMismatch");
    let (_, matched, _) = call(
        &app,
        "POST",
        "/v1/events",
        body("p2", "Provider", 2, "Completed"),
        true,
    )
    .await;
    assert_eq!(matched["payment"]["status"], "Matched");
    let (_, stale, _) = call(
        &app,
        "POST",
        "/v1/events",
        body("old", "Provider", 1, "Failed"),
        true,
    )
    .await;
    assert_eq!(stale["outcome"], "Stale");
    assert_eq!(stale["payment"]["status"], "Matched");
    let (code, conflict, _) = call(
        &app,
        "POST",
        "/v1/events",
        body("same-version", "Provider", 2, "Failed"),
        true,
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(conflict["error"]["code"], "version_conflict");
    let (code, current, headers) =
        call(&app, "GET", "/v1/payments/PAY-001", Value::Null, true).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(current["status"], "Matched");
    assert_eq!(headers["cache-control"], "no-store");
    let schema = json!({"$ref":"#/components/schemas/LivePayment","components":doc["components"]});
    assert!(
        jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&current)
    );
    let (code, _, _) = call(&app, "GET", "/v1/payments/unknown", Value::Null, true).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
}
#[tokio::test]
async fn event_validation_is_strict_and_size_bounded() {
    let app = app();
    for invalid in [
        body("x", "Unknown", 1, "Completed"),
        body("x", "Internal", 0, "Completed"),
        body("x", "Internal", u64::MAX, "Completed"),
        body(" ", "Internal", 1, "Completed"),
    ] {
        let (code, _, _) = call(&app, "POST", "/v1/events", invalid, true).await;
        assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY);
    }
    let mut invalid = body("x", "Internal", 1, "Completed");
    invalid["unknown"] = json!(true);
    assert_eq!(
        call(&app, "POST", "/v1/events", invalid, true).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/events",
            json!("x".repeat(MAX_EVENT_BYTES)),
            true
        )
        .await
        .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn concurrent_exact_retries_serialize_and_publish_stage_timings() {
    let app = configured_router(Config {
        concurrency: 32,
        event_queue_capacity: 32,
        requests_per_second: 10000,
        ..Config::default()
    });
    let event = body("retry-event", "Internal", 1, "Completed");
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let app = app.clone();
        let event = event.clone();
        jobs.spawn(async move { call(&app, "POST", "/v1/events", event, false).await });
    }
    let mut duplicates = 0;
    while let Some(result) = jobs.join_next().await {
        let (status, value, headers) = result.unwrap();
        assert_eq!(status, StatusCode::OK);
        duplicates += usize::from(value["duplicate_event"] == true);
        let header = headers["server-timing"].to_str().unwrap();
        for name in [
            "event_body",
            "event_resume",
            "event_queue",
            "event_dispatch",
            "event_service",
            "event_prepare",
            "event_begin",
            "event_work",
            "event_commit",
            "event_encode",
        ] {
            let metric = header
                .split(',')
                .map(str::trim)
                .find(|v| v.starts_with(&format!("{name};dur=")))
                .unwrap();
            let duration: f64 = metric.split('=').nth(1).unwrap().parse().unwrap();
            assert!(duration.is_finite() && duration >= 0.);
        }
    }
    assert_eq!(duplicates, 15);
}
