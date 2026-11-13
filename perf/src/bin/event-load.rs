use reconciliation_perf::stats;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tokio::task::JoinSet;

#[derive(Default)]
struct Measurements {
    statuses: BTreeMap<u16, usize>,
    rejection_codes: BTreeMap<String, usize>,
    post_by_outcome: BTreeMap<String, Vec<f64>>,
    event_stages: BTreeMap<String, Vec<f64>>,
    attempt_ms: Vec<f64>,
    accepted_post_ms: Vec<f64>,
    logical_post_ms: Vec<f64>,
    get_ms: Vec<f64>,
    workflows: usize,
    unique_commits: usize,
    duplicates: usize,
    stale: usize,
    errors: Vec<String>,
    sample_payment: Option<String>,
}
impl Measurements {
    fn merge(&mut self, other: Self) {
        for (status, count) in other.statuses {
            *self.statuses.entry(status).or_default() += count;
        }
        for (code, count) in other.rejection_codes {
            *self.rejection_codes.entry(code).or_default() += count;
        }
        for (outcome, times) in other.post_by_outcome {
            self.post_by_outcome
                .entry(outcome)
                .or_default()
                .extend(times);
        }
        for (key, values) in other.event_stages {
            self.event_stages.entry(key).or_default().extend(values);
        }
        self.attempt_ms.extend(other.attempt_ms);
        self.accepted_post_ms.extend(other.accepted_post_ms);
        self.logical_post_ms.extend(other.logical_post_ms);
        self.get_ms.extend(other.get_ms);
        self.workflows += other.workflows;
        self.unique_commits += other.unique_commits;
        self.duplicates += other.duplicates;
        self.stale += other.stale;
        self.errors.extend(other.errors);
        self.sample_payment = self.sample_payment.take().or(other.sample_payment);
    }
}
fn parse_timing(header: &str) -> Vec<(String, f64)> {
    header
        .split(',')
        .filter_map(|entry| {
            let (name, duration) = entry.trim().split_once(";dur=")?;
            if !name.starts_with("event_") {
                return None;
            }
            let duration: f64 = duration.parse().ok()?;
            (duration.is_finite() && duration >= 0.).then(|| (name.to_owned(), duration))
        })
        .collect()
}
fn transport_detail(error: reqwest::Error, elapsed: Duration) -> String {
    use std::error::Error;
    let category = if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else {
        "transport"
    };
    let error = error.without_url();
    let mut detail = format!("{category} after {:.3}s: {error}", elapsed.as_secs_f64());
    let mut source = error.source();
    while let Some(cause) = source {
        detail.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    detail
}
async fn request(
    client: &reqwest::Client,
    url: &str,
    body: Option<&Value>,
    m: &mut Measurements,
) -> Result<Value, String> {
    let bytes = body.map(|v| serde_json::to_vec(v).unwrap());
    let logical = Instant::now();
    loop {
        let begin = Instant::now();
        let builder = match &bytes {
            Some(bytes) => client
                .post(url)
                .header("content-type", "application/json")
                .body(bytes.clone()),
            None => client.get(url),
        };
        let response = match builder.send().await {
            Ok(r) => r,
            Err(e) => {
                *m.statuses.entry(0).or_default() += 1;
                m.attempt_ms.push(begin.elapsed().as_secs_f64() * 1000.);
                return Err(transport_detail(e, begin.elapsed()));
            }
        };
        let timing = parse_timing(
            response
                .headers()
                .get("server-timing")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(""),
        );
        let status = response.status().as_u16();
        let delay = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        *m.statuses.entry(status).or_default() += 1;
        let bytes = response
            .bytes()
            .await
            .map_err(|e| transport_detail(e, begin.elapsed()))?;
        let elapsed = begin.elapsed().as_secs_f64() * 1000.;
        m.attempt_ms.push(elapsed);
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| format!("unreadable HTTP {status} JSON"))?;
        if status == 200 {
            let label = if body.is_none() {
                "Read"
            } else if value["duplicate_event"] == true {
                "Duplicate"
            } else {
                value["outcome"].as_str().unwrap_or("Invalid")
            };
            for (name, duration) in timing {
                m.event_stages
                    .entry(format!("{label}.{name}"))
                    .or_default()
                    .push(duration);
            }
            if body.is_some() {
                let label = if value["duplicate_event"] == true {
                    "Duplicate"
                } else {
                    value["outcome"].as_str().unwrap_or("Invalid")
                };
                m.post_by_outcome
                    .entry(label.into())
                    .or_default()
                    .push(elapsed);
                m.accepted_post_ms.push(elapsed);
                m.logical_post_ms
                    .push(logical.elapsed().as_secs_f64() * 1000.);
            } else {
                m.get_ms.push(logical.elapsed().as_secs_f64() * 1000.);
            }
            return Ok(value);
        }
        let code = value["error"]["code"].as_str().unwrap_or("missing_code");
        *m.rejection_codes.entry(code.into()).or_default() += 1;
        if matches!(status, 429 | 503) {
            if !value["error"]["code"].is_string() || delay.is_none() {
                return Err(format!(
                    "HTTP {status} lacked structured error or Retry-After"
                ));
            }
            if logical.elapsed() >= Duration::from_secs(10) {
                return Err("retry budget exhausted".into());
            }
            tokio::time::sleep(Duration::from_secs(delay.unwrap().clamp(1, 2))).await;
        } else {
            return Err(format!("unexpected HTTP {status}: {value}"));
        }
    }
}
fn payment(id: &str, status: &str) -> Value {
    json!({"id":id,"currency":"GBP","amount_minor":10000,"status":status})
}
fn event(id: &str, suffix: &str, source: &str, version: u64, status: &str) -> Value {
    json!({"event_id":format!("{id}-{suffix}"),"source":source,"version":version,"payment":payment(id,status)})
}
async fn workflow(
    client: &reqwest::Client,
    base: &str,
    id: &str,
    m: &mut Measurements,
) -> Result<(), String> {
    let internal = event(id, "i", "Internal", 1, "Completed");
    let steps = [
        (internal.clone(), "Applied", false, "AwaitingProvider"),
        (
            event(id, "p1", "Provider", 1, "Pending"),
            "Applied",
            false,
            "StatusMismatch",
        ),
        (
            event(id, "p2", "Provider", 2, "Completed"),
            "Applied",
            false,
            "Matched",
        ),
        (internal, "Applied", true, "Matched"),
        (
            event(id, "old", "Provider", 1, "Failed"),
            "Stale",
            false,
            "Matched",
        ),
    ];
    for (body, outcome, duplicate, status) in steps {
        let result = request(client, &format!("{base}/v1/events"), Some(&body), m).await?;
        if result["event_id"] != body["event_id"]
            || result["outcome"] != outcome
            || result["duplicate_event"] != duplicate
            || result["payment"]["payment_id"] != id
            || result["payment"]["status"] != status
        {
            return Err(format!("incorrect event receipt for {id}"));
        }
        if duplicate {
            m.duplicates += 1;
        } else {
            m.unique_commits += 1;
        }
        if outcome == "Stale" {
            m.stale += 1;
        }
    }
    let result = request(client, &format!("{base}/v1/payments/{id}"), None, m).await?;
    if !valid_final(&result, id) {
        return Err(format!("incorrect current state for {id}"));
    }
    m.workflows += 1;
    if m.sample_payment.is_none() {
        m.sample_payment = Some(id.into());
    }
    Ok(())
}
fn valid_final(value: &Value, id: &str) -> bool {
    value["payment_id"] == id
        && value["status"] == "Matched"
        && value["internal"]["version"] == 1
        && value["provider"]["version"] == 2
        && value["internal"]["payment"] == payment(id, "Completed")
        && value["provider"]["payment"] == payment(id, "Completed")
}
fn audit(path: &str, expected: usize, run_id: Option<&str>) -> Value {
    let expected = i64::try_from(expected).unwrap();
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let count = |table: &str| -> i64 {
        db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    };
    let integrity: String = db
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    let mut bad = 0;
    let mut stmt = db
        .prepare("SELECT payment_id,source,version,payload FROM source_state")
        .unwrap();
    for row in stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .unwrap()
    {
        let (id, source, version, payload) = row.unwrap();
        let expected_version = if source == "Internal" {
            1
        } else if source == "Provider" {
            2
        } else {
            0
        };
        if version != expected_version
            || serde_json::from_str::<Value>(&payload).unwrap() != payment(&id, "Completed")
        {
            bad += 1;
        }
    }
    let applied: i64 = db
        .query_row(
            "SELECT count(*) FROM event_log WHERE outcome='\"Applied\"'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let stale: i64 = db
        .query_row(
            "SELECT count(*) FROM event_log WHERE outcome='\"Stale\"'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let foreign_workload_payments: Option<i64> = run_id.map(|id| {
        db.query_row(
            "SELECT count(*) FROM payments WHERE payment_id NOT LIKE ?1",
            [format!("LOAD-{id}-%")],
            |r| r.get(0),
        )
        .unwrap()
    });
    let valid = foreign_workload_payments.is_none_or(|n| n == 0)
        && integrity == "ok"
        && count("payments") == expected
        && count("source_state") == 2 * expected
        && count("event_log") == 4 * expected
        && applied == 3 * expected
        && stale == expected
        && bad == 0;
    json!({"passed":valid,"expected_completed_payments":expected,"expected_run_id":run_id,"foreign_workload_payments":foreign_workload_payments,"scope":"Complete-lifecycle expectations; interrupted requests can leave valid partial state","integrity":integrity,"payments":count("payments"),"source_states":count("source_state"),
        "event_log_rows":count("event_log"),"applied_rows":applied,"stale_rows":stale,"incorrect_source_states":bad})
}
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--audit") {
        let result = audit(
            &args[1],
            args[2].parse().unwrap(),
            args.get(3).map(String::as_str),
        );
        println!("{result}");
        if result["passed"] != true {
            std::process::exit(1);
        }
        return;
    }
    assert!(
        args.len() == 3,
        "Usage: event-load BASE_URL CONCURRENCY SECONDS | --audit DB COMPLETED_PAYMENTS"
    );
    let base = reqwest::Url::parse(&args[0]).unwrap();
    assert!(
        base.scheme() == "http" && base.host_str() == Some("127.0.0.1"),
        "Local isolated servers only"
    );
    let concurrency: usize = args[1].parse().unwrap();
    let seconds: u64 = args[2].parse().unwrap();
    assert!((1..=32).contains(&concurrency) && (2..=600).contains(&seconds));
    let mut headers = reqwest::header::HeaderMap::new();
    let key = std::env::var("RECONCILE_API_KEY").expect("Isolated API key required");
    let mut auth = reqwest::header::HeaderValue::from_str(&format!("Bearer {key}")).unwrap();
    auth.set_sensitive(true);
    headers.insert(reqwest::header::AUTHORIZATION, auth);
    let client = reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .default_headers(headers)
        .timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(concurrency)
        .build()
        .unwrap();
    client
        .get(format!("{}/health", args[0]))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let run = uuid::Uuid::new_v4().simple().to_string();
    let probe_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop = probe_stop.clone();
    let probe = std::thread::spawn(move || {
        let mut gaps = Vec::new();
        let mut instant = Instant::now();
        let mut utc = chrono::Utc::now().timestamp_millis();
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(100));
            let now = Instant::now();
            let wall = chrono::Utc::now().timestamp_millis();
            let elapsed = now.duration_since(instant).as_secs_f64() * 1000.;
            if elapsed > 250. && gaps.len() < 64 {
                gaps.push(json!({"monotonic_gap_ms":elapsed,"utc_gap_ms":wall-utc}));
            }
            instant = now;
            utc = wall;
        }
        gaps
    });
    let start = Instant::now();
    let deadline = start + Duration::from_secs(seconds);
    let mut tasks = JoinSet::new();
    for worker in 0..concurrency {
        let (client, base, run) = (client.clone(), args[0].clone(), run.clone());
        tasks.spawn(async move {
            let mut m = Measurements::default();
            let mut n = 0;
            while Instant::now() < deadline {
                let id = format!("LOAD-{run}-{worker}-{n}");
                if let Err(e) = workflow(&client, &base, &id, &mut m).await {
                    m.errors.push(e);
                    break;
                }
                n += 1;
            }
            m
        });
    }
    let mut m = Measurements::default();
    while let Some(result) = tasks.join_next().await {
        m.merge(result.unwrap());
    }
    let elapsed = start.elapsed().as_secs_f64();
    probe_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let process_scheduling_gaps = probe.join().unwrap();
    let outcome_latency: BTreeMap<_, _> = m
        .post_by_outcome
        .iter()
        .map(|(key, values)| (key, stats(values)))
        .collect();
    let event_stage_latency: BTreeMap<_, _> = m
        .event_stages
        .iter()
        .map(|(key, values)| (key, stats(values)))
        .collect();
    let result = json!({
        "schema_version":4,"run_id":run,"process_scheduling_gaps":process_scheduling_gaps,"event_stage_latency":event_stage_latency,"workload":"five event posts and one status read per payment",
        "load_model":"closed loop; sequential lifecycle per worker; retry 429/503 using Retry-After; 10s retry budget",
        "concurrency":concurrency,"admission_seconds":seconds,"elapsed_seconds_including_drain":elapsed,
        "drain_seconds":(elapsed-seconds as f64).max(0.),"completed_payments":m.workflows,
        "unique_committed_events":m.unique_commits,"unique_committed_events_per_second":m.unique_commits as f64/elapsed,
        "applied_updates":m.unique_commits-m.stale,"applied_updates_per_second":(m.unique_commits-m.stale) as f64/elapsed,
        "duplicate_receipts":m.duplicates,"stale_receipts":m.stale,"http_status_counts":m.statuses,"rejection_codes":m.rejection_codes,"successful_post_latency_by_outcome":outcome_latency,
        "all_attempt_latency":stats(&m.attempt_ms),"successful_post_attempt_latency":stats(&m.accepted_post_ms),
        "logical_post_latency_including_retries":stats(&m.logical_post_ms),"status_read_latency_including_retries":stats(&m.get_ms),
        "errors":m.errors,"sample_payment_id":m.sample_payment,
        "correctness_passed":m.errors.is_empty() && m.workflows>0 && m.unique_commits==4*m.workflows
    });
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
    if result["correctness_passed"] != true {
        std::process::exit(1);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn final_validation_rejects_stale_state_and_wrong_amounts() {
        let mut value = json!({"payment_id":"x","status":"Matched","internal":{"version":1,"payment":payment("x","Completed")},"provider":{"version":2,"payment":payment("x","Completed")}});
        assert!(valid_final(&value, "x"));
        value["provider"]["version"] = json!(1);
        assert!(!valid_final(&value, "x"));
        value["provider"]["version"] = json!(2);
        value["internal"]["payment"]["amount_minor"] = json!(9999);
        assert!(!valid_final(&value, "x"));
    }

    #[test]
    fn database_audit_checks_retries_counts_and_latest_values() {
        let path =
            std::env::temp_dir().join(format!("event-load-audit-{}.sqlite3", uuid::Uuid::new_v4()));
        let store =
            reconcile_cli::events::EventStore::open(&path, Duration::from_secs(60)).unwrap();
        let internal = event("x", "i", "Internal", 1, "Completed");
        for value in [
            internal.clone(),
            event("x", "p1", "Provider", 1, "Pending"),
            event("x", "p2", "Provider", 2, "Completed"),
            internal,
            event("x", "old", "Provider", 1, "Failed"),
        ] {
            store.apply(serde_json::from_value(value).unwrap()).unwrap();
        }
        drop(store);
        assert_eq!(
            audit(path.to_str().unwrap(), 1, Some("different-run"))["foreign_workload_payments"],
            1
        );
        assert_eq!(audit(path.to_str().unwrap(), 1, None)["passed"], true);
        assert_eq!(audit(path.to_str().unwrap(), 2, None)["passed"], false);
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute(
            "UPDATE source_state SET version=1 WHERE source='Provider'",
            [],
        )
        .unwrap();
        drop(db);
        assert_eq!(audit(path.to_str().unwrap(), 1, None)["passed"], false);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
}
