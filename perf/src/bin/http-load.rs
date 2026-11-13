use bytes::Bytes;
use reconciliation_perf::{dataset, normalized, stats};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, task::JoinSet};

#[derive(Clone)]
struct Context {
    client: reqwest::Client,
    url: String,
    payload: Bytes,
    expected: Arc<Value>,
}
struct Observation {
    status: u16,
    wire_ms: f64,
    arrival_ms: f64,
    verified_ms: f64,
    bytes: usize,
    valid: bool,
    error: Option<String>,
}

async fn one(ctx: &Context, scheduled: Instant) -> Observation {
    let start = Instant::now();
    let response = ctx
        .client
        .post(&ctx.url)
        .header("content-type", "application/json")
        .body(ctx.payload.clone())
        .send()
        .await;
    let (status, body) = match response {
        Ok(r) => {
            let status = r.status().as_u16();
            match r.bytes().await {
                Ok(b) => (status, b),
                Err(e) => return failed(start, scheduled, e),
            }
        }
        Err(e) => return failed(start, scheduled, e),
    };
    let wire_ms = start.elapsed().as_secs_f64() * 1000.;
    let arrival_ms = scheduled.elapsed().as_secs_f64() * 1000.;
    let len = body.len();
    let valid = if status == 200 {
        match serde_json::from_slice::<Value>(&body) {
            Ok(mut value) => {
                if let Some(meta) = value.get_mut("metadata").and_then(Value::as_object_mut) {
                    meta.remove("report_id");
                    meta.remove("generated_at");
                }
                value == *ctx.expected
            }
            Err(_) => false,
        }
    } else {
        false
    };
    Observation {
        status,
        wire_ms,
        arrival_ms,
        verified_ms: start.elapsed().as_secs_f64() * 1000.,
        bytes: len,
        valid,
        error: None,
    }
}
fn failed(start: Instant, scheduled: Instant, e: reqwest::Error) -> Observation {
    let category = if e.is_timeout() {
        "timeout"
    } else if e.is_connect() {
        "connect"
    } else {
        "transport"
    };
    let error = e.without_url();
    let mut detail = format!("{category}: {error}");
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        detail.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    Observation {
        status: 0,
        wire_ms: start.elapsed().as_secs_f64() * 1000.,
        arrival_ms: scheduled.elapsed().as_secs_f64() * 1000.,
        verified_ms: start.elapsed().as_secs_f64() * 1000.,
        bytes: 0,
        valid: false,
        error: Some(detail),
    }
}
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert!(
        args.len() == 5,
        "Usage: http-load BASE_URL PAYMENTS_PER_SOURCE CONCURRENCY SECONDS ARRIVAL_RPS (0=closed loop)"
    );
    let base = reqwest::Url::parse(&args[0]).unwrap();
    assert!(
        base.scheme() == "http"
            && matches!(base.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")),
        "This harness is restricted to local test servers"
    );
    let n: usize = args[1].parse().unwrap();
    let concurrency: usize = args[2].parse().unwrap();
    let seconds: f64 = args[3].parse().unwrap();
    let rps: f64 = args[4].parse().unwrap();
    assert!(
        (1..=5000).contains(&n)
            && (1..=64).contains(&concurrency)
            && (1.0..=300.0).contains(&seconds)
            && (0.0..=10000.0).contains(&rps)
    );
    let input = dataset(n, "mixed");
    let payload = Bytes::from(serde_json::to_vec(&input).unwrap());
    let expected = normalized(&reconcile_cli::reconcile_report(
        &input.internal_payments,
        &input.provider_payments,
    ))["ok"]
        .clone();
    let mut headers = reqwest::header::HeaderMap::new();
    if let Ok(key) = std::env::var("RECONCILE_API_KEY") {
        let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {key}")).unwrap();
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .default_headers(headers)
        .pool_max_idle_per_host(concurrency)
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let ctx = Context {
        client,
        url: format!("{}/v1/reconcile", args[0].trim_end_matches('/')),
        payload,
        expected: Arc::new(expected),
    };
    for _ in 0..3 {
        let o = one(&ctx, Instant::now()).await;
        assert!(
            o.status == 200 && o.valid,
            "Warmup failed: status {}, validation {}",
            o.status,
            o.valid
        );
    }
    // Reset the API's one-second fixed-window counter after warmups.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let start = Instant::now();
    let duration = Duration::from_secs_f64(seconds);
    let deadline = start + duration;
    let mut observations = Vec::new();
    let mut drops = 0usize;
    let mut scheduled_count = 0usize;
    if rps == 0. {
        let mut tasks = JoinSet::new();
        for _ in 0..concurrency {
            let ctx = ctx.clone();
            tasks.spawn(async move {
                let mut results = Vec::new();
                while Instant::now() < deadline {
                    results.push(one(&ctx, Instant::now()).await);
                }
                results
            });
        }
        while let Some(r) = tasks.join_next().await {
            observations.extend(r.unwrap());
        }
        scheduled_count = observations.len();
    } else {
        let gate = Arc::new(Semaphore::new(concurrency));
        let mut tasks = JoinSet::new();
        let total = (seconds * rps).floor() as usize;
        for i in 0..total {
            let scheduled = start + Duration::from_secs_f64(i as f64 / rps);
            tokio::time::sleep_until(scheduled.into()).await;
            scheduled_count += 1;
            if let Ok(permit) = gate.clone().try_acquire_owned() {
                let ctx = ctx.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    one(&ctx, scheduled).await
                });
            } else {
                drops += 1;
            }
            while let Some(r) = tasks.try_join_next() {
                observations.push(r.unwrap());
            }
        }
        while let Some(r) = tasks.join_next().await {
            observations.push(r.unwrap());
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let denominator = elapsed.max(seconds);
    let successful: Vec<_> = observations
        .iter()
        .filter(|o| o.status == 200 && o.valid)
        .collect();
    let success_wire: Vec<_> = successful.iter().map(|o| o.wire_ms).collect();
    let arrival: Vec<_> = successful.iter().map(|o| o.arrival_ms).collect();
    let verification: Vec<_> = successful.iter().map(|o| o.verified_ms).collect();
    let failure_lat: Vec<_> = observations
        .iter()
        .filter(|o| o.status != 200)
        .map(|o| o.wire_ms)
        .collect();
    let mut statuses = BTreeMap::new();
    for o in &observations {
        *statuses.entry(o.status.to_string()).or_insert(0usize) += 1;
    }
    let mut transport_errors = BTreeMap::new();
    for o in &observations {
        if let Some(e) = &o.error {
            *transport_errors.entry(e.clone()).or_insert(0usize) += 1;
        }
    }
    println!("{}",serde_json::to_string_pretty(&json!({
        "generated_at":chrono::Utc::now().to_rfc3339(),"engine_version":env!("ENGINE_VERSION"),"payments_per_source":n,
        "concurrency":concurrency,"scenario":"mixed","mode":if rps==0.{"closed_loop"}else{"scheduled_arrivals"},"target_arrival_rps":rps,
        "measurement_seconds":seconds,"elapsed_with_drain_seconds":denominator,"request_bytes":ctx.payload.len(),
        "scheduled_requests":scheduled_count,"submitted_requests":observations.len(),"client_admission_drops":drops,
        "successful_requests":successful.len(),"invalid_success_responses":observations.iter().filter(|o|o.status==200&&!o.valid).count(),
        "status_counts":statuses,"transport_errors":transport_errors,"success_rps":successful.len() as f64/denominator,
        "successful_input_records_per_second":successful.len() as f64*(2*n) as f64/denominator,
        "response_bytes":observations.iter().map(|o|o.bytes).sum::<usize>(),
        "success_http_latency":stats(&success_wire),"success_scheduled_arrival_latency":stats(&arrival),
        "success_latency_including_client_validation":stats(&verification),"rejected_or_transport_latency":stats(&failure_lat),
        "raw_success_http_ms":success_wire,"raw_success_arrival_ms":arrival,
        "tail_sample_warning":successful.len()<1000,
        "notes":"Every successful JSON response is fully validated outside the HTTP-latency timer. Closed-loop throughput includes that client work and can under-drive the server. Scheduled latency includes scheduler delay; skipped arrivals are counted as client admission drops. No retries, no TLS, localhost connection pooling; sender JSON is prebuilt. Client and server share hardware. Status 0 is a transport failure."
    })).unwrap());
    assert!(
        observations.iter().all(|o| o.status != 200 || o.valid),
        "Incorrect success response"
    );
}
