use super::tenancy::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use tower::ServiceExt;
async fn call(
    router: &Router,
    key: &str,
    method: &str,
    path: &str,
    body: serde_json::Value,
) -> (u16, String) {
    let r = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("Bearer {key}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    (
        r.status().as_u16(),
        String::from_utf8(to_bytes(r.into_body(), 1_000_000).await.unwrap().to_vec()).unwrap(),
    )
}
#[tokio::test]
async fn tenants_isolate_events_lookups_reports_metrics_and_restarts() {
    let root = std::env::temp_dir().join(format!("reconcile-tenants-{}", uuid::Uuid::new_v4()));
    for id in ["a", "b"] {
        std::fs::create_dir_all(root.join(id)).unwrap();
    }
    let a = "a".repeat(32);
    let b = "b".repeat(32);
    let entries = || {
        vec![
            Tenant {
                id: "a".into(),
                api_key: a.clone(),
                database: root.join("a/events.sqlite3"),
            },
            Tenant {
                id: "b".into(),
                api_key: b.clone(),
                database: root.join("b/events.sqlite3"),
            },
        ]
    };
    let (router, queues) = build(&super::config::Config::default(), entries()).unwrap();
    let event = |amount| serde_json::json!({"event_id":"same-event","source":"Internal","version":1,"payment":{"id":"same-payment","currency":"GBP","amount_minor":amount,"status":"Completed"}});
    assert_eq!(
        call(&router, &a, "POST", "/v1/events", event(100)).await.0,
        200
    );
    assert_eq!(
        call(
            &router,
            &b,
            "GET",
            "/v1/payments/same-payment",
            serde_json::Value::Null
        )
        .await
        .0,
        404
    );
    assert_eq!(
        call(&router, &b, "POST", "/v1/events", event(200)).await.0,
        200
    );
    let value = call(
        &router,
        &a,
        "GET",
        "/v1/payments/same-payment",
        serde_json::Value::Null,
    )
    .await;
    assert!(value.1.contains("\"amount_minor\":100"));
    for path in [
        "/v1/payments/same-payment",
        "/v1/discrepancies",
        "/v1/storage-health",
        "/metrics",
    ] {
        assert_eq!(
            call(&router, "wrong", "GET", path, serde_json::Value::Null)
                .await
                .0,
            401
        );
    }
    assert_eq!(
        call(
            &router,
            &a,
            "GET",
            "/v1/discrepancies?limit=0",
            serde_json::Value::Null
        )
        .await
        .0,
        422
    );
    assert_eq!(
        call(
            &router,
            &a,
            "GET",
            "/v1/discrepancies?unknown=1",
            serde_json::Value::Null
        )
        .await
        .0,
        422
    );
    let metrics = call(&router, &a, "GET", "/metrics", serde_json::Value::Null).await;
    assert_eq!(metrics.0, 200);
    assert!(metrics.1.contains("reconcile_requests_total"));
    assert!(!metrics.1.contains("same-payment"));
    drop(router);
    for q in queues {
        q.wait_idle().await;
    }
    let (router, queues) = build(&super::config::Config::default(), entries()).unwrap();
    for (key, amount) in [(&a, 100), (&b, 200)] {
        let r = call(&router, key, "POST", "/v1/events", event(amount)).await;
        assert_eq!(r.0, 200);
        assert!(r.1.contains("\"duplicate_event\":true"));
    }
    drop(router);
    for q in queues {
        q.wait_idle().await;
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn rejects_reused_tenant_keys_and_database_directories() {
    let root = std::env::temp_dir();
    let entries = vec![
        Tenant {
            id: "a".into(),
            api_key: "a".repeat(32),
            database: root.join("events.sqlite3"),
        },
        Tenant {
            id: "b".into(),
            api_key: "b".repeat(32),
            database: root.join("events.sqlite3"),
        },
    ];
    assert!(build(&super::config::Config::default(), entries).is_err());
    assert!(build(&super::config::Config::default(), vec![]).is_err());
}
