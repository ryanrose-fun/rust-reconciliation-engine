#![cfg(all(feature = "gpu", feature = "client"))]
//! Explicit hardware benchmark; never executed by ordinary cargo test.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
const KEY: &str = "synthetic-gpu-benchmark-key-0123456789";
struct Server {
    child: Child,
    url: String,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn start(root: &Path, capacity: usize) -> (Server, f64) {
    let began = Instant::now();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_api"));
    for (key, _) in std::env::vars().filter(|(k, _)| k.starts_with("RECONCILE_")) {
        cmd.env_remove(key);
    }
    let mut child = cmd
        .env("RECONCILE_BIND", "127.0.0.1:0")
        .env("RECONCILE_API_KEY", KEY)
        .env("RECONCILE_GPU_DB_PATH", root.join("gpu.sqlite3"))
        .env("RECONCILE_DB_PATH", root.join("events.sqlite3"))
        .env("RECONCILE_GPU_CAPACITY", capacity.to_string())
        .env("RECONCILE_REQUESTS_PER_SECOND", "10000")
        .env("RECONCILE_TIMEOUT_MS", "120000")
        .env("RECONCILE_MAX_BODY_BYTES", "16777216")
        .env("RECONCILE_GPU_PROFILE", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::from(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(root.join("api.log"))
                .unwrap(),
        ))
        .spawn()
        .unwrap();
    let out = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines().map_while(Result::ok) {
            if let Some(url) = line.strip_prefix("API listening on ") {
                let _ = tx.send(url.to_owned());
            }
        }
    });
    match rx.recv_timeout(Duration::from_secs(900)) {
        Ok(url) => (Server { child, url }, began.elapsed().as_secs_f64()),
        Err(e) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("Startup: {e}; see {}", root.join("api.log").display());
        }
    }
}
fn amount(i: usize) -> u64 {
    10000 + i as u64
}
fn expected(i: usize) -> u64 {
    1 + if i.is_multiple_of(10) { 2 } else { 0 } + if i.is_multiple_of(20) { 8 } else { 0 }
}
fn batch(begin: usize, end: usize) -> Vec<u8> {
    let mut events = Vec::with_capacity((end - begin) * 2);
    for i in begin..end {
        for source in ["Internal", "Provider"] {
            let provider = source == "Provider";
            events.push(json!({"event_id":format!("{source}-{i:07}"),"source":source,"version":1,"payment":{"id":format!("PAY-{i:07}"),"amount_minor":amount(i)-u64::from(provider && i.is_multiple_of(10)),"currency":"GBP","status":if provider && i.is_multiple_of(20) {"Pending"} else {"Completed"}}}));
        }
    }
    serde_json::to_vec(&json!({"request_id":format!("batch-{begin:07}"),"events":events})).unwrap()
}
async fn get(client: &reqwest::Client, server: &Server, path: &str) -> Value {
    let r = client
        .get(format!("{}{path}", server.url))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    serde_json::from_slice(&r.bytes().await.unwrap()).unwrap()
}
fn percentile(values: &[f64], p: f64) -> f64 {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    v[((v.len() as f64 * p).ceil() as usize)
        .saturating_sub(1)
        .min(v.len() - 1)]
}
fn sizes(root: &Path) -> Value {
    let mut result = serde_json::Map::new();
    for name in ["gpu.sqlite3", "gpu.sqlite3-wal", "gpu.sqlite3-shm"] {
        result.insert(
            name.into(),
            json!(
                std::fs::metadata(root.join(name))
                    .map(|m| m.len())
                    .unwrap_or(0)
            ),
        );
    }
    Value::Object(result)
}
#[tokio::test]
#[ignore = "explicit durable million-payment GPU benchmark"]
async fn durable_million_payment_benchmark() {
    if cfg!(debug_assertions) {
        panic!("Run with --release");
    }
    let count: usize = std::env::var("GPU_BENCH_PAYMENTS")
        .unwrap_or("1000000".into())
        .parse()
        .unwrap();
    assert!((1..=2_000_000).contains(&count));
    let concurrency: usize = std::env::var("GPU_BENCH_CONCURRENCY")
        .unwrap_or("1".into())
        .parse()
        .unwrap();
    assert!((1..=8).contains(&concurrency));
    let root = std::env::temp_dir().join(format!("gpu-api-bench-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    println!("Evidence: {}", root.display());
    let (server, cold_start_s) = start(&root, count);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let health = get(&client, &server, "/v1/gpu/health").await;
    let started = Instant::now();
    let mut latency = Vec::new();
    let mut payload_bytes = 0usize;
    let mut response_bytes = 0usize;
    let mut preparation_s = 0.0;
    let mut validation_s = 0.0;
    let mut last = Vec::new();
    let mut last_sequence = 0;
    let mut sequences = std::collections::HashSet::new();
    let mut tasks = tokio::task::JoinSet::new();
    let mut next = 0;
    while next < count || !tasks.is_empty() {
        while next < count && tasks.len() < concurrency {
            let begin = next;
            let end = (begin + 2048).min(count);
            next = end;
            let prep = Instant::now();
            let body = batch(begin, end);
            preparation_s += prep.elapsed().as_secs_f64();
            payload_bytes += body.len();
            last = body.clone();
            let client = client.clone();
            let url = format!("{}/v1/gpu/events", server.url);
            tasks.spawn(async move {
                let request_start = Instant::now();
                let r = client
                    .post(url)
                    .bearer_auth(KEY)
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await
                    .unwrap();
                let status = r.status();
                let bytes = r.bytes().await.unwrap();
                (
                    begin,
                    end,
                    status,
                    bytes,
                    request_start.elapsed().as_secs_f64() * 1000.0,
                )
            });
        }
        let (begin, end, status, bytes, ms) = tasks.join_next().await.unwrap().unwrap();
        latency.push(ms);
        response_bytes += bytes.len();
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&bytes));
        let validation = Instant::now();
        let reply: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reply["duplicate"], false);
        let seq = reply["commit_sequence"].as_u64().unwrap();
        assert!(sequences.insert(seq));
        if end == count {
            last_sequence = seq;
        }
        let rows = reply["gpu"]["receipts"].as_array().unwrap();
        assert_eq!(rows.len(), end - begin);
        let mut seen = std::collections::HashSet::new();
        for row in rows {
            let id = row["id"].as_str().unwrap();
            let i: usize = id.strip_prefix("PAY-").unwrap().parse().unwrap();
            assert!((begin..end).contains(&i));
            assert!(seen.insert(i));
            assert_eq!(row["internal"], 1);
            assert_eq!(row["provider"], 1);
            assert_eq!(row["result"], expected(i));
        }
        validation_s += validation.elapsed().as_secs_f64();
        if latency.len().is_multiple_of(25) || end == count {
            println!(
                "{end}/{count} payments durable; {:.1}s elapsed",
                started.elapsed().as_secs_f64()
            );
        }
    }
    let ingestion_s = started.elapsed().as_secs_f64();
    assert_eq!(sequences, (1..=latency.len() as u64).collect());
    let disk_after_ingestion = sizes(&root);
    let end_health = get(&client, &server, "/v1/gpu/health").await;
    assert_eq!(end_health["committed_batches"], latency.len());
    // Publish a complete checkpoint, measured separately from ingestion. Automatic
    // checkpoints during ingestion remain included in ingestion wall time/latency.
    let cp_start = Instant::now();
    let cp = client
        .post(format!("{}/v1/gpu/checkpoint", server.url))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(cp.status(), 200);
    let _: Value = serde_json::from_slice(&cp.bytes().await.unwrap()).unwrap();
    let checkpoint_s = cp_start.elapsed().as_secs_f64();
    let repeat = client
        .post(format!("{}/v1/gpu/checkpoint", server.url))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(repeat.status(), 200);
    let repeat: Value = serde_json::from_slice(&repeat.bytes().await.unwrap()).unwrap();
    assert_eq!(repeat["checkpoint_sequence"], latency.len());
    let disk_before = sizes(&root);
    drop(server); // Force kill: do not rely on graceful shutdown.
    let (server, restart_to_ready_s) = start(&root, count);
    let recovered = get(&client, &server, "/v1/gpu/health").await;
    assert_eq!(recovered["committed_batches"], latency.len());
    assert_eq!(recovered["checkpoint_sequence"], latency.len());
    let validation = Instant::now();
    let mut sampled = std::collections::BTreeSet::from([0, count - 1]);
    for i in (0..count).step_by((count / 1000).max(1)) {
        sampled.insert(i);
        if i + 1 < count {
            sampled.insert(i + 1);
        }
    }
    for &i in &sampled {
        let state = get(&client, &server, &format!("/v1/gpu/payments/PAY-{i:07}")).await;
        assert_eq!(state["result"], expected(i));
        assert_eq!(state["internal"]["amount_minor"], amount(i));
        assert_eq!(
            state["provider"]["amount_minor"],
            amount(i) - u64::from(i.is_multiple_of(10))
        );
        assert_eq!(state["internal"]["version"], 1);
        assert_eq!(state["provider"]["version"], 1);
    }
    let retry = client
        .post(format!("{}/v1/gpu/events", server.url))
        .bearer_auth(KEY)
        .header("content-type", "application/json")
        .body(last)
        .send()
        .await
        .unwrap();
    assert_eq!(retry.status(), 200);
    let retry: Value = serde_json::from_slice(&retry.bytes().await.unwrap()).unwrap();
    assert_eq!(retry["duplicate"], true);
    assert_eq!(retry["commit_sequence"], last_sequence);
    let validation_after_restart_s = validation.elapsed().as_secs_f64();
    let disk_after = sizes(&root);
    drop(server);
    let db = rusqlite::Connection::open_with_flags(
        root.join("gpu.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let identities: i64 = db
        .query_row("SELECT count(*) FROM gpu_event_ids", [], |r| r.get(0))
        .unwrap();
    let batches: i64 = db
        .query_row("SELECT count(*) FROM gpu_requests", [], |r| r.get(0))
        .unwrap();
    assert_eq!(identities, count as i64 * 2);
    assert_eq!(batches, latency.len() as i64);
    assert_eq!(
        db.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    let profiles: Vec<Value> = std::fs::read_to_string(root.join("api.log"))
        .unwrap()
        .lines()
        .filter_map(|l| l.strip_prefix("GPU_PROFILE "))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let mut phase_totals = serde_json::Map::new();
    for field in [
        "preparation_ms",
        "identity_ms",
        "compute_ms",
        "journal_ms",
        "commit_ms",
        "total_ms",
    ] {
        let total: f64 = profiles
            .iter()
            .filter(|p| p["phase"] == "apply")
            .map(|p| p[field].as_f64().unwrap())
            .sum();
        phase_totals.insert(field.into(), json!(total));
    }
    let mut report = json!({"schema":1,"payments":count,"source_events":count*2,"payments_per_full_request":2048,"concurrency":1,"durability":"SQLite WAL synchronous FULL; acknowledgement after commit","health":health,"cold_start_s":cold_start_s,"ingestion_s":ingestion_s,"payments_per_s":count as f64/ingestion_s,"events_per_s":count as f64*2.0/ingestion_s,"requests":latency.len(),"request_latency_ms":{"p50":percentile(&latency,0.5),"p95":percentile(&latency,0.95),"p99":percentile(&latency,0.99),"max":percentile(&latency,1.0)},"preparation_s":preparation_s,"response_validation_s":validation_s,"request_bytes":payload_bytes,"response_bytes":response_bytes,"phase_totals_ms":phase_totals,"startup_profiles":profiles.iter().filter(|p|p["phase"]=="startup").collect::<Vec<_>>(),"restart_to_ready_s":restart_to_ready_s,"recovered_batches":batches,"recovered_events":identities,"snapshot_checks":sampled.len(),"validation_after_restart_s":validation_after_restart_s,"disk_before_kill_bytes":disk_before,"disk_after_recovery_bytes":disk_after,"validated":true,"latency_samples_ms":latency});
    report["apply_profiles"] = json!(
        profiles
            .iter()
            .filter(|p| p["phase"] == "apply")
            .collect::<Vec<_>>()
    );
    report["concurrency"] = json!(concurrency);
    report["disk_after_ingestion_bytes"] = disk_after_ingestion;
    report["automatic_checkpoint_interval_batches"] = json!(256);
    report["final_checkpoint_s"] = json!(checkpoint_s);
    report["ingestion_plus_final_checkpoint_s"] = json!(ingestion_s + checkpoint_s);
    report["checkpoint_profiles"] = json!(
        profiles
            .iter()
            .filter(|p| p["phase"] == "checkpoint")
            .collect::<Vec<_>>()
    );
    std::fs::write(
        root.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!(
        "PASS: {} payments/s; p95 {} ms; restart {} s; report {}",
        report["payments_per_s"],
        report["request_latency_ms"]["p95"],
        restart_to_ready_s,
        root.join("report.json").display()
    );
}
