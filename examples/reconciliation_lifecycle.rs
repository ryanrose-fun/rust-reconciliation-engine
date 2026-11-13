//! Run with examples/run-lifecycle.ps1, or configure the four DEMO_* keys.
use axum::{
    Router,
    extract::{Request, State},
    response::IntoResponse,
    routing::post,
};
use reconcile_cli::{
    Currency, Payment, PaymentStatus,
    client::{EventClient, RetryPolicy},
    events::{EventSource, PaymentEvent},
};
use serde::{Deserialize, Serialize};
use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
#[derive(Deserialize, Serialize)]
struct Plan {
    events: Vec<PaymentEvent>,
    investigation_id: String,
}
#[derive(Clone)]
struct Proxy {
    url: String,
    first: Arc<AtomicBool>,
    http: reqwest::Client,
}
async fn delayed_receipt(State(state): State<Proxy>, request: Request) -> axum::response::Response {
    let Some(auth) = request.headers().get("authorization").cloned() else {
        return axum::http::StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(bytes) = axum::body::to_bytes(request.into_body(), 16384).await else {
        return axum::http::StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    let Ok(response) = state
        .http
        .post(&state.url)
        .header("authorization", auth)
        .header("content-type", "application/json")
        .body(bytes)
        .send()
        .await
    else {
        return axum::http::StatusCode::BAD_GATEWAY.into_response();
    };
    let status = response.status();
    let Ok(body) = response.bytes().await else {
        return axum::http::StatusCode::BAD_GATEWAY.into_response();
    };
    // The real API has already committed. Delay only the receipt, not the write.
    if status.is_success() && state.first.swap(false, Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    (status, [("content-type", "application/json")], body).into_response()
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::var("DEMO_API_URL")?;
    let internal = std::env::var("DEMO_INTERNAL_KEY")?;
    let provider = std::env::var("DEMO_PROVIDER_KEY")?;
    let auditor = std::env::var("DEMO_AUDITOR_KEY")?;
    let investigator = std::env::var("DEMO_INVESTIGATOR_KEY")?;
    let file = std::path::PathBuf::from(std::env::var("DEMO_EVENTS_FILE")?);
    let plan: Plan = if file.exists() {
        serde_json::from_slice(&std::fs::read(&file)?)?
    } else {
        let id = format!("CHECKOUT-{}", uuid::Uuid::new_v4());
        let events = [
            (EventSource::Internal, 1, PaymentStatus::Completed),
            (EventSource::Provider, 1, PaymentStatus::Pending),
            (EventSource::Provider, 3, PaymentStatus::Completed),
            (EventSource::Provider, 2, PaymentStatus::Pending),
        ]
        .into_iter()
        .map(|(source, version, status)| PaymentEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            source,
            version,
            payment: Payment {
                id: id.clone(),
                currency: Currency::GBP,
                amount_minor: 12500,
                status,
            },
        })
        .collect();
        let plan = Plan {
            events,
            investigation_id: uuid::Uuid::new_v4().to_string(),
        };
        let mut out = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file)?;
        out.write_all(&serde_json::to_vec_pretty(&plan)?)?;
        out.sync_all()?;
        plan
    };
    if plan.events.len() != 4 {
        return Err("Demo plan requires exactly four persisted events".into());
    }
    let policy = RetryPolicy {
        attempt_timeout: Duration::from_millis(500),
        total_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let direct = EventClient::new(&base, internal.clone(), policy.clone())?;
    let upstream = EventClient::new(&base, provider, policy.clone())?;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let proxy_base = format!("http://{}", listener.local_addr()?);
    let app = Router::new()
        .route("/v1/events", post(delayed_receipt))
        .with_state(Proxy {
            url: format!("{}/v1/events", base.trim_end_matches('/')),
            first: Arc::new(AtomicBool::new(true)),
            http: http.clone(),
        });
    let proxy = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let checkout = EventClient::new(&proxy_base, internal, policy)?;
    let result = checkout.deliver(&plan.events[0]).await?;
    if result.attempts < 2 || result.receipt["duplicate_event"] != true {
        return Err("Expected timeout recovery with the original event ID".into());
    }
    println!(
        "Checkout: {} attempts, duplicate receipt: {}, state: {}",
        result.attempts, result.receipt["duplicate_event"], result.receipt["payment"]["status"]
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    println!(
        "Delayed provider: {}",
        upstream.deliver(&plan.events[1]).await?.receipt["payment"]["status"]
    );
    let mut url = reqwest::Url::parse(&base)?;
    url.path_segments_mut()
        .map_err(|_| "Invalid base URL")?
        .pop_if_empty()
        .extend(["v1", "payments", &plan.events[0].payment.id, "history"]);
    let history: serde_json::Value = serde_json::from_slice(
        &http
            .get(url.clone())
            .bearer_auth(&auditor)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?,
    )?;
    if history["current_status"] != "Matched" {
        let mut action_url = url.clone();
        action_url
            .path_segments_mut()
            .unwrap()
            .pop()
            .push("investigation");
        let body = serde_json::json!({"id":plan.investigation_id,"action":"Acknowledge","note":"Provider is pending; awaiting the delayed completion event.","expected_revision":history["current_revision"]});
        http.post(action_url)
            .bearer_auth(investigator)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await?
            .error_for_status()?;
        println!("Investigator acknowledged this revision; payment evidence unchanged");
    }
    println!(
        "Provider completes: {}",
        upstream.deliver(&plan.events[2]).await?.receipt["payment"]["status"]
    );
    println!(
        "Late provider v2: {}",
        upstream.deliver(&plan.events[3]).await?.receipt["outcome"]
    );
    println!(
        "Retry checkout: duplicate={}",
        direct.deliver(&plan.events[0]).await?.receipt["duplicate_event"]
    );
    let history: serde_json::Value = serde_json::from_slice(
        &http
            .get(url)
            .bearer_auth(auditor)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?,
    )?;
    if history["current_status"] != "Matched" {
        return Err("Lifecycle did not resolve to Matched".into());
    }
    println!("Audit timeline:");
    for entry in history["entries"]
        .as_array()
        .ok_or("Missing history entries")?
    {
        println!(
            "  {}: {} -> {} (actor: {})",
            entry["sequence"], entry["kind"], entry["status"], entry["actor"]
        );
    }
    proxy.abort();
    println!("Persisted event plan: {}", file.display());
    Ok(())
}
