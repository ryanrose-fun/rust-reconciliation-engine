use super::tenancy::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use tower::ServiceExt;

#[tokio::test]
async fn scoped_credentials_enforce_sources_read_only_audits_and_investigation_identity() {
    use crate::permissions::Role;
    let dir = std::env::temp_dir().join(format!("scopes-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let keys = [
        "i".repeat(32),
        "p".repeat(32),
        "a".repeat(32),
        "n".repeat(32),
    ];
    let credentials = [
        Role::InternalWriter,
        Role::ProviderWriter,
        Role::Auditor,
        Role::Investigator,
    ]
    .into_iter()
    .enumerate()
    .map(|(i, role)| Credential {
        id: format!("credential-{i}"),
        api_key: keys[i].clone(),
        role,
    })
    .collect();
    let (router, queues) = build(
        &crate::config::Config::default(),
        vec![Tenant {
            id: "scoped".into(),
            api_key: String::new(),
            credentials,
            database: dir.join("events.sqlite3"),
        }],
    )
    .unwrap();
    let input = serde_json::json!({"event_id":"i1","source":"Internal","version":1,"payment":{"id":"p","currency":"GBP","amount_minor":100,"status":"Completed"}});
    for key in [&keys[1], &keys[2], &keys[3]] {
        assert_eq!(
            call(&router, key, "POST", "/v1/events", input.clone())
                .await
                .0,
            403
        );
    }
    assert_eq!(
        call(
            &router,
            &keys[2],
            "GET",
            "/v1/payments/p",
            serde_json::Value::Null
        )
        .await
        .0,
        404
    );
    assert_eq!(
        call(&router, &keys[0], "POST", "/v1/events", input.clone())
            .await
            .0,
        200
    );
    let mut provider = input;
    provider["event_id"] = "p1".into();
    provider["source"] = "Provider".into();
    provider["payment"]["status"] = "Pending".into();
    assert_eq!(
        call(&router, &keys[0], "POST", "/v1/events", provider.clone())
            .await
            .0,
        403
    );
    assert_eq!(
        call(&router, &keys[1], "POST", "/v1/events", provider)
            .await
            .0,
        200
    );
    let (code, body) = call(
        &router,
        &keys[2],
        "GET",
        "/v1/payments/p/history",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(code, 200);
    let h: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(h["current_status"], "StatusMismatch");
    let note = serde_json::json!({"id":"note1","action":"Acknowledge","note":"Investigating","expected_revision":h["current_revision"]});
    for key in [&keys[0], &keys[1], &keys[2]] {
        assert_eq!(
            call(
                &router,
                key,
                "POST",
                "/v1/payments/p/investigation",
                note.clone()
            )
            .await
            .0,
            403
        );
    }
    assert_eq!(
        call(
            &router,
            &keys[3],
            "POST",
            "/v1/payments/p/investigation",
            note.clone()
        )
        .await
        .0,
        200
    );
    let mut spoof = note;
    spoof["actor"] = "administrator".into();
    assert_eq!(
        call(
            &router,
            &keys[3],
            "POST",
            "/v1/payments/p/investigation",
            spoof
        )
        .await
        .0,
        422
    );
    let (_, body) = call(
        &router,
        &keys[2],
        "GET",
        "/v1/payments/p/history",
        serde_json::Value::Null,
    )
    .await;
    let h: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(h["acknowledged"], true);
    assert_eq!(
        h["entries"].as_array().unwrap().last().unwrap()["actor"],
        "credential-3"
    );
    assert_eq!(h["current_status"], "StatusMismatch");
    drop(router);
    for q in queues {
        q.wait_idle().await;
    }
    std::fs::remove_dir_all(dir).unwrap();
}
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
                credentials: vec![],
                api_key: a.clone(),
                database: root.join("a/events.sqlite3"),
            },
            Tenant {
                id: "b".into(),
                credentials: vec![],
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
            credentials: vec![],
            api_key: "a".repeat(32),
            database: root.join("events.sqlite3"),
        },
        Tenant {
            id: "b".into(),
            credentials: vec![],
            api_key: "b".repeat(32),
            database: root.join("events.sqlite3"),
        },
    ];
    assert!(build(&super::config::Config::default(), entries).is_err());
    assert!(build(&super::config::Config::default(), vec![]).is_err());
}
