//! Single-owner GPU actor. Acknowledgements follow a FULL SQLite commit.
use gpu_compute::{
    Gpu,
    resident::{Change, Event, Receipt, Worker},
};
use reconcile_cli::{Payment, events::EventSource};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};
use tokio::sync::oneshot;

pub const MAX_EVENTS: usize = 4096;
#[derive(Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct GpuEvent {
    pub event_id: String,
    pub source: EventSource,
    pub version: u64,
    pub payment: Payment,
}
#[derive(Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct Submit {
    pub request_id: String,
    pub events: Vec<GpuEvent>,
}
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Computed {
    pub receipts: Vec<Receipt>,
    pub changes: Vec<Change>,
}
#[derive(Clone, Deserialize, Serialize)]
pub struct Reply {
    pub request_id: String,
    pub commit_sequence: i64,
    pub duplicate: bool,
    pub gpu: Computed,
}
#[derive(Debug)]
pub enum Error {
    Invalid(String),
    Conflict,
    Busy,
    Unavailable,
    NotFound,
}
type Answer = Result<serde_json::Value, Error>;
enum Command {
    Submit(Submit, oneshot::Sender<Answer>),
    Get(String, oneshot::Sender<Answer>),
    Health(oneshot::Sender<Answer>),
    Stop(oneshot::Sender<()>),
}
pub struct Client {
    tx: mpsc::SyncSender<Command>,
    alive: Arc<AtomicBool>,
}
impl Client {
    pub fn start(path: PathBuf, capacity: usize) -> Result<Arc<Self>, String> {
        if !path.is_absolute() || path.file_name() != Some(std::ffi::OsStr::new("gpu.sqlite3")) {
            return Err("GPU database must be an absolute path ending in gpu.sqlite3".into());
        }
        if !(1..=2_000_000).contains(&capacity) {
            return Err("GPU capacity must be 1..2000000".into());
        }
        if path
            .symlink_metadata()
            .is_ok_and(|m| m.file_type().is_symlink())
        {
            return Err("GPU database must not be a symbolic link".into());
        }
        let path = path
            .parent()
            .ok_or("GPU database directory required")?
            .canonicalize()
            .map_err(|e| e.to_string())?
            .join("gpu.sqlite3");
        let (tx, rx) = mpsc::sync_channel(32);
        let (ready, started) = mpsc::sync_channel(1);
        let alive = Arc::new(AtomicBool::new(true));
        let status = alive.clone();
        std::thread::Builder::new()
            .name("gpu-journal-owner".into())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(path, capacity, rx, &ready)
                }));
                status.store(false, Ordering::Release);
                match outcome {
                    Ok(Err(e)) => {
                        eprintln!("GPU worker stopped: {e}");
                        let _ = ready.try_send(Err(e));
                    }
                    Err(_) => {
                        eprintln!("GPU worker panicked; restart required");
                        let _ = ready.try_send(Err("GPU worker initialization failed".into()));
                    }
                    _ => {}
                }
            })
            .map_err(|e| e.to_string())?;
        started
            .recv()
            .map_err(|_| "GPU worker did not initialize")??;
        Ok(Arc::new(Self { tx, alive }))
    }
    async fn call(&self, make: impl FnOnce(oneshot::Sender<Answer>) -> Command) -> Answer {
        if !self.alive.load(Ordering::Acquire) {
            return Err(Error::Unavailable);
        }
        let (tx, rx) = oneshot::channel();
        self.tx.try_send(make(tx)).map_err(|e| match e {
            mpsc::TrySendError::Full(_) => Error::Busy,
            mpsc::TrySendError::Disconnected(_) => Error::Unavailable,
        })?;
        rx.await.unwrap_or(Err(Error::Unavailable))
    }
    pub async fn submit(&self, r: Submit) -> Answer {
        self.call(|tx| Command::Submit(r, tx)).await
    }
    pub async fn get(&self, id: String) -> Answer {
        self.call(|tx| Command::Get(id, tx)).await
    }
    pub async fn health(&self) -> Answer {
        self.call(Command::Health).await
    }
    pub async fn shutdown(&self) {
        let sender = self.tx.clone();
        let (tx, rx) = oneshot::channel();
        let _ = tokio::task::spawn_blocking(move || sender.send(Command::Stop(tx))).await;
        let _ = rx.await;
    }
}
fn canonical<T: Serialize>(x: &T) -> String {
    serde_json::to_string(x).expect("serializable GPU protocol")
}
fn compute(worker: &mut Worker<'_>, request: &Submit) -> Result<Computed, String> {
    use reconcile_cli::{Currency, PaymentStatus};
    let events: Vec<_> = request
        .events
        .iter()
        .map(|e| Event {
            id: e.payment.id.clone(),
            source: match e.source {
                EventSource::Internal => 0,
                EventSource::Provider => 1,
            },
            version: e.version,
            amount: e.payment.amount_minor,
            currency: match e.payment.currency {
                Currency::GBP => 0,
                Currency::USD => 1,
                Currency::EUR => 2,
                Currency::JPY => 3,
            },
            status: match e.payment.status {
                PaymentStatus::Pending => 0,
                PaymentStatus::Completed => 1,
                PaymentStatus::Failed => 2,
            },
        })
        .collect();
    let response = worker.submit(&events)?;
    let mut changes = response.changes;
    changes.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(Computed {
        receipts: response.receipts,
        changes,
    })
}
fn validate(r: &Submit) -> Result<(), Error> {
    let valid = |s: &str, max: usize| !s.trim().is_empty() && s.len() <= max;
    if !valid(&r.request_id, 128) || r.events.is_empty() || r.events.len() > MAX_EVENTS {
        return Err(Error::Invalid(
            "request_id and 1..4096 events required".into(),
        ));
    }
    let mut ids = HashSet::new();
    let mut pairs = HashSet::new();
    for e in &r.events {
        if !valid(&e.event_id, 128)
            || !valid(&e.payment.id, 256)
            || e.version == 0
            || !ids.insert(&e.event_id)
            || !pairs.insert((&e.payment.id, matches!(e.source, EventSource::Provider)))
        {
            return Err(Error::Invalid(
                "Invalid or repeated event ID/source/payment pair".into(),
            ));
        }
    }
    Ok(())
}
fn run(
    path: PathBuf,
    capacity: usize,
    rx: mpsc::Receiver<Command>,
    ready: &mpsc::SyncSender<Result<(), String>>,
) -> Result<(), String> {
    let startup = std::time::Instant::now();
    let profile = std::env::var_os("RECONCILE_GPU_PROFILE").is_some();
    let _lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))
        .map_err(|e| e.to_string())?;
    fs2::FileExt::try_lock_exclusive(&_lock)
        .map_err(|_| "GPU database is already owned by another process")?;
    let mut db = Connection::open(&path).map_err(|e| e.to_string())?;
    db.busy_timeout(std::time::Duration::from_secs(2))
        .map_err(|e| e.to_string())?;
    let tables: i64 = db
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    let app: i64 = db
        .query_row("PRAGMA application_id", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    if tables > 0 && app != 0x47505531 {
        return Err("Not a GPU journal database".into());
    }
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA application_id=1196447025;
        CREATE TABLE IF NOT EXISTS gpu_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1), format INTEGER NOT NULL, shader TEXT NOT NULL, capacity INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS gpu_requests(seq INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL UNIQUE, input TEXT NOT NULL, input_sha256 TEXT NOT NULL, computed TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS gpu_event_ids(event_id TEXT PRIMARY KEY, canonical TEXT NOT NULL);").map_err(|e|e.to_string())?;
    let integrity: String = db
        .query_row("PRAGMA quick_check", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    if integrity != "ok" {
        return Err("GPU journal integrity check failed".into());
    }
    let shader = format!(
        "{:x}",
        Sha256::digest(include_bytes!("../../../perf/gpu/src/resident.wgsl"))
    );
    db.execute(
        "INSERT OR IGNORE INTO gpu_meta VALUES(1,1,?1,?2)",
        params![shader, capacity as i64],
    )
    .map_err(|e| e.to_string())?;
    let meta: (i64, String, i64) = db
        .query_row(
            "SELECT format,shader,capacity FROM gpu_meta WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(|e| e.to_string())?;
    if meta != (1, shader, capacity as i64) {
        return Err(
            "GPU journal format/shader/capacity mismatch; explicit migration required".into(),
        );
    }
    let gpu = Gpu::try_new()?;
    let adapter = gpu.info.name.clone();
    let mut worker = Worker::new(&gpu, capacity, MAX_EVENTS);
    let mut committed = 0i64;
    let initialized_ms = startup.elapsed().as_secs_f64() * 1000.0;
    let mut replay_compute_ms = 0.0;
    {
        let mut statement = db
            .prepare(
                "SELECT seq,request_id,input,input_sha256,computed FROM gpu_requests ORDER BY seq",
            )
            .map_err(|e| e.to_string())?;
        let mut rows = statement.query([]).map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let seq: i64 = row.get(0).map_err(|e| e.to_string())?;
            let id: String = row.get(1).map_err(|e| e.to_string())?;
            let input: String = row.get(2).map_err(|e| e.to_string())?;
            let digest: String = row.get(3).map_err(|e| e.to_string())?;
            let stored: String = row.get(4).map_err(|e| e.to_string())?;
            if seq != committed + 1 || format!("{:x}", Sha256::digest(input.as_bytes())) != digest {
                return Err("GPU journal sequence or checksum failure".into());
            }
            let request: Submit = serde_json::from_str(&input).map_err(|e| e.to_string())?;
            validate(&request).map_err(|_| "Invalid journal request")?;
            if request.request_id != id {
                return Err("GPU journal request ID mismatch".into());
            }
            let compute_start = std::time::Instant::now();
            let got = compute(&mut worker, &request)?;
            replay_compute_ms += compute_start.elapsed().as_secs_f64() * 1000.0;
            let expected: Computed = serde_json::from_str(&stored).map_err(|e| e.to_string())?;
            if got != expected {
                return Err("GPU recovery result mismatch; refusing readiness".into());
            }
            for event in request.events {
                let saved: Option<String> = db
                    .query_row(
                        "SELECT canonical FROM gpu_event_ids WHERE event_id=?1",
                        [&event.event_id],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(|e| e.to_string())?;
                if saved.as_deref() != Some(canonical(&event).as_str()) {
                    return Err("GPU journal event identity mismatch".into());
                }
            }
            committed = seq;
        }
    }
    if profile {
        eprintln!(
            "GPU_PROFILE {}",
            serde_json::json!({"phase":"startup","batches":committed,"initialized_ms":initialized_ms,"total_ms":startup.elapsed().as_secs_f64()*1000.0,"replay_compute_ms":replay_compute_ms})
        );
    }
    eprintln!("GPU ready: {adapter}; recovered {committed} committed batches");
    ready.send(Ok(())).map_err(|_| "Startup cancelled")?;
    while let Ok(command) = rx.recv() {
        match command {
            Command::Health(reply) => {
                let _=reply.send(Ok(serde_json::json!({"status":"ready","backend":"gpu","adapter":adapter,"committed_batches":committed,"capacity":capacity,"durability":"sqlite-wal-full","recovered_before_ready":true})));
            }
            Command::Get(id, reply) => {
                let response = worker
                    .snapshot(&id)
                    .map(|s| serde_json::to_value(s).unwrap())
                    .ok_or(Error::NotFound);
                let _ = reply.send(response);
            }
            Command::Stop(reply) => {
                let _ = reply.send(());
                break;
            }
            Command::Submit(request, reply) => {
                // Client timeout does not cancel a command already admitted. Retry its ID.
                let result = apply(&mut db, &mut worker, &request);
                match result {
                    Ok(answer) => {
                        committed = committed.max(answer.commit_sequence);
                        let _ = reply.send(Ok(serde_json::to_value(answer).unwrap()));
                    }
                    Err(ApplyError::User(e)) => {
                        let _ = reply.send(Err(e));
                    }
                    Err(ApplyError::Fatal(e)) => {
                        let _ = reply.send(Err(Error::Unavailable));
                        return Err(e);
                    }
                }
            }
        }
    }
    Ok(())
}
enum ApplyError {
    User(Error),
    Fatal(String),
}
impl From<rusqlite::Error> for ApplyError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Fatal(e.to_string())
    }
}
fn apply(
    db: &mut Connection,
    worker: &mut Worker<'_>,
    request: &Submit,
) -> Result<Reply, ApplyError> {
    let start = std::time::Instant::now();
    validate(request).map_err(ApplyError::User)?;
    let input = canonical(request);
    let existing: Option<(i64, String, String)> = db
        .query_row(
            "SELECT seq,input,computed FROM gpu_requests WHERE request_id=?1",
            [&request.request_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    if let Some((seq, old, computed)) = existing {
        if old != input {
            return Err(ApplyError::User(Error::Conflict));
        }
        return Ok(Reply {
            request_id: request.request_id.clone(),
            commit_sequence: seq,
            duplicate: true,
            gpu: serde_json::from_str(&computed).map_err(|e| ApplyError::Fatal(e.to_string()))?,
        });
    }
    for event in &request.events {
        let existing: Option<String> = db
            .query_row(
                "SELECT canonical FROM gpu_event_ids WHERE event_id=?1",
                [&event.event_id],
                |r| r.get(0),
            )
            .optional()?;
        if existing.is_some_and(|old| old != canonical(event)) {
            return Err(ApplyError::User(Error::Conflict));
        }
    }
    let identity_ms = start.elapsed().as_secs_f64() * 1000.0;
    let compute_start = std::time::Instant::now();
    let computed = compute(worker, request).map_err(|e| ApplyError::User(Error::Invalid(e)))?;
    let compute_ms = compute_start.elapsed().as_secs_f64() * 1000.0;
    let journal_start = std::time::Instant::now();
    // After GPU mutation, ANY storage failure is fatal to this worker. Never serve
    // potentially uncommitted GPU state: restart reconstructs only committed inputs.
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    tx.execute(
        "INSERT INTO gpu_requests(request_id,input,input_sha256,computed) VALUES(?1,?2,?3,?4)",
        params![
            request.request_id,
            input,
            format!("{:x}", Sha256::digest(input.as_bytes())),
            canonical(&computed)
        ],
    )?;
    let seq = tx.last_insert_rowid();
    for event in &request.events {
        tx.execute(
            "INSERT OR IGNORE INTO gpu_event_ids(event_id,canonical) VALUES(?1,?2)",
            params![event.event_id, canonical(event)],
        )?;
    }
    let journal_ms = journal_start.elapsed().as_secs_f64() * 1000.0;
    let commit_start = std::time::Instant::now();
    tx.commit()?;
    let commit_ms = commit_start.elapsed().as_secs_f64() * 1000.0;
    if std::env::var_os("RECONCILE_GPU_PROFILE").is_some() {
        eprintln!(
            "GPU_PROFILE {}",
            serde_json::json!({"phase":"apply","sequence":seq,"events":request.events.len(),"identity_ms":identity_ms,"compute_ms":compute_ms,"journal_ms":journal_ms,"commit_ms":commit_ms,"total_ms":start.elapsed().as_secs_f64()*1000.0})
        );
    }
    Ok(Reply {
        request_id: request.request_id.clone(),
        commit_sequence: seq,
        duplicate: false,
        gpu: computed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn bounded_queue_rejects_overload_and_closed_worker() {
        let (tx, rx) = mpsc::sync_channel(1);
        let client = Client {
            tx,
            alive: Arc::new(AtomicBool::new(true)),
        };
        let (reply, _) = oneshot::channel();
        client.tx.try_send(Command::Health(reply)).unwrap();
        assert!(matches!(client.health().await, Err(Error::Busy)));
        drop(rx);
        assert!(matches!(client.health().await, Err(Error::Unavailable)));
    }
    #[test]
    fn malformed_batches_reject_before_execution() {
        let mut r:Submit=serde_json::from_value(serde_json::json!({"request_id":"r","events":[{"event_id":"e","source":"Internal","version":1,"payment":{"id":"p","amount_minor":0,"currency":"GBP","status":"Pending"}}]})).unwrap();
        assert!(validate(&r).is_ok());
        r.events.push(r.events[0].clone());
        assert!(matches!(validate(&r), Err(Error::Invalid(_))));
        r.events.pop();
        r.events[0].version = 0;
        assert!(validate(&r).is_err());
        r.events[0].version = 1;
        r.events[0].payment.id = "x".repeat(257);
        assert!(validate(&r).is_err());
    }
}
