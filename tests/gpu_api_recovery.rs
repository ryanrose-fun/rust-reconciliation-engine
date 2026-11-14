#![cfg(all(feature = "gpu", feature = "client"))]
//! Live discrete-GPU acceptance. Not silently skipped or emulated on CPU.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
const KEY: &str = "synthetic-gpu-acceptance-key-0123456789";
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
fn command(root: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_api"));
    for key in ["RECONCILE_TENANTS_FILE", "RECONCILE_GPU_ADAPTER"] {
        cmd.env_remove(key);
    }
    cmd.env("RECONCILE_BIND", "127.0.0.1:0")
        .env("RECONCILE_API_KEY", KEY)
        .env("RECONCILE_GPU_DB_PATH", root.join("gpu.sqlite3"))
        .env("RECONCILE_DB_PATH", root.join("events.sqlite3"))
        .env("RECONCILE_GPU_CAPACITY", "4096")
        .env("RECONCILE_REQUESTS_PER_SECOND", "10000")
        .env("RECONCILE_TIMEOUT_MS", "30000")
        .stdout(Stdio::piped())
        .stderr(Stdio::from(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(root.join("api.log"))
                .unwrap(),
        ));
    cmd
}
fn start(root: &Path) -> Server {
    let mut child = command(root).spawn().unwrap();
    let out = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines().map_while(Result::ok) {
            if let Some(url) = line.strip_prefix("API listening on ") {
                let _ = tx.send(url.to_owned());
            }
        }
    });
    match rx.recv_timeout(Duration::from_secs(60)) {
        Ok(url) => Server { child, url },
        Err(e) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "GPU startup failed: {e}; {}",
                std::fs::read_to_string(root.join("api.log")).unwrap()
            );
        }
    }
}
async fn request(
    server: &Server,
    method: &str,
    path: &str,
    body: Option<Value>,
    key: &str,
) -> (u16, Value) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(40))
        .build()
        .unwrap();
    let mut req = client
        .request(method.parse().unwrap(), format!("{}{path}", server.url))
        .bearer_auth(key);
    if let Some(body) = body {
        req = req
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&body).unwrap());
    }
    let r = req.send().await.unwrap();
    let status = r.status().as_u16();
    let bytes = r.bytes().await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
fn event(id: &str, source: &str, version: u64, amount: u64, status: &str, event_id: &str) -> Value {
    json!({"event_id":event_id,"source":source,"version":version,"payment":{"id":id,"amount_minor":amount,"currency":"GBP","status":status}})
}
fn failure_exit(mut child: Child) {
    let start = std::time::Instant::now();
    loop {
        if let Some(s) = child.try_wait().unwrap() {
            assert!(!s.success());
            break;
        }
        if start.elapsed() > Duration::from_secs(30) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("Expected startup rejection");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
#[tokio::test]
#[ignore = "requires a real discrete GPU; run explicitly with --ignored --nocapture"]
async fn gpu_http_commit_crash_replay_and_fail_closed() {
    let root: PathBuf =
        std::env::temp_dir().join(format!("rust-gpu-finance-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    println!("GPU acceptance evidence: {}", root.display());
    let mut server = start(&root);
    assert_eq!(
        request(&server, "GET", "/v1/gpu/health", None, "wrong")
            .await
            .0,
        401
    );
    assert_eq!(
        request(&server, "GET", "/v1/gpu/health", None, KEY).await.1["status"],
        "ready"
    );
    assert_eq!(
        request(&server, "GET", "/v1/gpu/payments/missing", None, KEY)
            .await
            .0,
        404
    );
    // A second process cannot race the GPU state against the same journal.
    failure_exit(command(&root).spawn().unwrap());
    let seed = json!({"request_id":"seed","events":[event("P1","Internal",1,u64::MAX,"Completed","internal-1"),event("P1","Provider",1,u64::MAX,"Completed","provider-1")]});
    let (code, original) =
        request(&server, "POST", "/v1/gpu/events", Some(seed.clone()), KEY).await;
    assert_eq!(code, 200);
    assert_eq!(original["gpu"]["receipts"][0]["status"], "Matched");
    drop(server);
    server = start(&root);
    let state = request(&server, "GET", "/v1/gpu/payments/P1", None, KEY)
        .await
        .1;
    assert_eq!(state["internal"]["amount_minor"], json!(u64::MAX));
    assert_eq!(state["status"], "Matched");
    let retry = request(&server, "POST", "/v1/gpu/events", Some(seed.clone()), KEY)
        .await
        .1;
    assert_eq!(retry["duplicate"], true);
    assert_eq!(retry["commit_sequence"], original["commit_sequence"]);
    assert_eq!(retry["gpu"], original["gpu"]);
    let mut conflict = seed.clone();
    conflict["events"][0]["payment"]["amount_minor"] = json!(0);
    assert_eq!(
        request(
            &server,
            "POST",
            "/v1/gpu/events",
            Some(conflict.clone()),
            KEY
        )
        .await
        .0,
        409
    );
    conflict["request_id"] = json!("new-request-old-event-id");
    assert_eq!(
        request(&server, "POST", "/v1/gpu/events", Some(conflict), KEY)
            .await
            .0,
        409
    );
    let mismatch = json!({"request_id":"mismatch","events":[event("P1","Provider",2,9500,"Pending","provider-2")]});
    let r = request(&server, "POST", "/v1/gpu/events", Some(mismatch), KEY).await;
    assert_eq!(r.0, 200);
    assert_eq!(
        r.1["gpu"]["receipts"][0]["status"],
        "AmountAndStatusMismatch"
    );
    for (request_id, version, expected) in [("stale", 1, 4), ("version-conflict", 2, 3)] {
        let r=request(&server,"POST","/v1/gpu/events",Some(json!({"request_id":request_id,"events":[event("P1","Provider",version,0,"Failed",request_id)]})),KEY).await;
        assert_eq!(r.0, 200);
        assert_eq!(r.1["gpu"]["receipts"][0]["provider"], expected);
        assert_eq!(r.1["gpu"]["changes"], json!([]));
    }
    let bad = event("P1", "Internal", 2, 1, "Completed", "bad");
    assert_eq!(
        request(
            &server,
            "POST",
            "/v1/gpu/events",
            Some(json!({"request_id":"invalid","events":[bad.clone(),bad]})),
            KEY
        )
        .await
        .0,
        422
    );
    // Force a real SQLite write failure AFTER GPU mutation; no uncommitted state
    // may be served and retry after recovery must apply exactly once.
    let db = rusqlite::Connection::open(root.join("gpu.sqlite3")).unwrap();
    db.execute_batch("CREATE TRIGGER reject_commit BEFORE INSERT ON gpu_requests BEGIN SELECT RAISE(ABORT,'acceptance injected commit failure'); END;").unwrap();
    drop(db);
    let restore = json!({"request_id":"restore","events":[event("P1","Provider",3,u64::MAX,"Completed","provider-3")]});
    assert_eq!(
        request(
            &server,
            "POST",
            "/v1/gpu/events",
            Some(restore.clone()),
            KEY
        )
        .await
        .0,
        503
    );
    assert_eq!(
        request(&server, "GET", "/v1/gpu/payments/P1", None, KEY)
            .await
            .0,
        503
    );
    drop(server);
    let db = rusqlite::Connection::open(root.join("gpu.sqlite3")).unwrap();
    db.execute_batch("DROP TRIGGER reject_commit").unwrap();
    drop(db);
    server = start(&root);
    assert_eq!(
        request(&server, "GET", "/v1/gpu/payments/P1", None, KEY)
            .await
            .1["status"],
        "AmountAndStatusMismatch"
    );
    assert_eq!(
        request(
            &server,
            "POST",
            "/v1/gpu/events",
            Some(restore.clone()),
            KEY
        )
        .await
        .0,
        200
    );
    // Kill while an HTTP update may be queued, executing or committed. The caller
    // cannot know which; stable request IDs resolve the outcome after restart.
    let events: Vec<_> = (0..1000)
        .map(|i| {
            event(
                &format!("B{i}"),
                "Internal",
                1,
                100,
                "Completed",
                &format!("bulk-{i}"),
            )
        })
        .collect();
    let bulk = json!({"request_id":"ambiguous-bulk","events":events});
    let url = format!("{}/v1/gpu/events", server.url);
    let bytes = serde_json::to_vec(&bulk).unwrap();
    let pending = tokio::spawn(async move {
        reqwest::Client::new()
            .post(url)
            .bearer_auth(KEY)
            .header("content-type", "application/json")
            .body(bytes)
            .send()
            .await
    });
    tokio::time::sleep(Duration::from_millis(2)).await;
    drop(server);
    let _ = pending.await;
    server = start(&root);
    assert_eq!(
        request(&server, "POST", "/v1/gpu/events", Some(bulk.clone()), KEY)
            .await
            .0,
        200
    );
    let retry = request(&server, "POST", "/v1/gpu/events", Some(bulk), KEY)
        .await
        .1;
    assert_eq!(retry["duplicate"], true);
    assert_eq!(
        request(&server, "GET", "/v1/gpu/payments/B999", None, KEY)
            .await
            .1["status"],
        "MissingFromProvider"
    );
    assert_eq!(
        request(&server, "GET", "/v1/gpu/payments/P1", None, KEY)
            .await
            .1["status"],
        "Matched"
    );
    let before =
        request(&server, "GET", "/v1/gpu/health", None, KEY).await.1["committed_batches"].clone();
    let overflow: Vec<_> = (0..4096)
        .map(|i| {
            event(
                &format!("C{i}"),
                "Internal",
                1,
                0,
                "Pending",
                &format!("capacity-{i}"),
            )
        })
        .collect();
    assert_eq!(
        request(
            &server,
            "POST",
            "/v1/gpu/events",
            Some(json!({"request_id":"capacity-overflow","events":overflow})),
            KEY
        )
        .await
        .0,
        422
    );
    assert_eq!(
        request(&server, "GET", "/v1/gpu/payments/C0", None, KEY)
            .await
            .0,
        404
    );
    assert_eq!(
        request(&server, "GET", "/v1/gpu/health", None, KEY).await.1["committed_batches"],
        before
    );
    drop(server);
    let db = rusqlite::Connection::open(root.join("gpu.sqlite3")).unwrap();
    let n: i64 = db
        .query_row(
            "SELECT count(*) FROM gpu_requests WHERE request_id='ambiguous-bulk'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(
        db.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    db.execute(
        "UPDATE gpu_requests SET input_sha256='tampered' WHERE request_id='seed'",
        [],
    )
    .unwrap();
    drop(db);
    failure_exit(command(&root).spawn().unwrap());
    println!(
        "PASS: authenticated HTTP, commit-before-ack, forced restart/replay, stable request/event IDs, stale/conflicting versions, write-failure fail-closed, ambiguous retry, corruption rejection, exclusive ownership"
    );
}
