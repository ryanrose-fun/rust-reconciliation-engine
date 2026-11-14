//! One immutable snapshot in flight. A separate FULL SQLite database avoids
//! holding the ingestion database's writer lock for a large checkpoint blob.
use super::*;
use std::path::Path;
pub(super) type Stored = (i64, String, String, Vec<u8>, String);
const APP_ID: i64 = 0x47504331;
fn open(root: &Path) -> Result<Connection, String> {
    let path = root.with_file_name("gpu-checkpoints.sqlite3");
    if path
        .symlink_metadata()
        .is_ok_and(|m| m.file_type().is_symlink())
    {
        return Err("Checkpoint database must not be a symbolic link".into());
    }
    let db = Connection::open(path).map_err(|e| e.to_string())?;
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
    if tables > 0 && app != APP_ID {
        return Err("Not a GPU checkpoint database".into());
    }
    db.busy_timeout(std::time::Duration::from_secs(2))
        .map_err(|e| e.to_string())?;
    db.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| e.to_string())?;
    db.pragma_update(None, "synchronous", "FULL")
        .map_err(|e| e.to_string())?;
    db.pragma_update(None, "application_id", APP_ID)
        .map_err(|e| e.to_string())?;
    db.execute_batch("CREATE TABLE IF NOT EXISTS gpu_checkpoint(singleton INTEGER PRIMARY KEY CHECK(singleton=1),seq INTEGER NOT NULL,chain TEXT NOT NULL,identities TEXT NOT NULL,payload BLOB NOT NULL,sha256 TEXT NOT NULL);").map_err(|e|e.to_string())?;
    Ok(db)
}
pub(super) fn load(root: &Path) -> Result<Option<Stored>, String> {
    let db = open(root)?;
    let integrity: String = db
        .query_row("PRAGMA quick_check", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    if integrity != "ok" {
        return Err("Checkpoint database integrity failure".into());
    }
    db.query_row(
        "SELECT seq,chain,identities,payload,sha256 FROM gpu_checkpoint WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    )
    .optional()
    .map_err(|e| e.to_string())
}
pub(super) struct Pending {
    pub seq: i64,
    handle: Option<std::thread::JoinHandle<Result<i64, String>>>,
}
impl Pending {
    pub fn finished(&self) -> bool {
        self.handle.as_ref().unwrap().is_finished()
    }
    pub fn finish(mut self) -> Result<i64, String> {
        self.handle
            .take()
            .unwrap()
            .join()
            .map_err(|_| "Checkpoint publisher panicked".to_owned())?
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        // The owning actor's exclusive lock must outlive this publisher, including
        // failures and shutdown. No detached writer may outlive ownership.
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
pub(super) fn start(
    path: &Path,
    worker: &Worker<'_>,
    db: &Connection,
    seq: i64,
    chain: &str,
) -> Result<Pending, String> {
    let began = std::time::Instant::now();
    let watermark: i64 = db
        .query_row(
            "SELECT coalesce(max(rowid),0) FROM gpu_event_ids",
            [],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    let captured = worker.capture_checkpoint();
    let capture_ms = began.elapsed().as_secs_f64() * 1000.0;
    let path = path.to_owned();
    let chain = chain.to_owned();
    let handle=std::thread::Builder::new().name("gpu-checkpoint-publisher".into()).spawn(move || {
        let payload=captured.encode();
        let digest=format!("{:x}",Sha256::digest(&payload));
        let source=Connection::open_with_flags(&path,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|e|e.to_string())?;
        let identities=format!("{watermark}:{}",identity_digest(&source,watermark)?);
        drop(source);
        let mut db=open(&path)?;
        publish(&mut db,seq,&chain,&identities,&payload,&digest)?;
        drop(db); // Include connection-close work before reporting completion.
        if std::env::var_os("RECONCILE_GPU_PROFILE").is_some() {
            eprintln!("GPU_PROFILE {}",serde_json::json!({"phase":"checkpoint","sequence":seq,"bytes":payload.len(),"capture_ms":capture_ms,"total_ms":began.elapsed().as_secs_f64()*1000.0,"background":true}));
        }
        Ok(seq)
    }).map_err(|e|e.to_string())?;
    Ok(Pending {
        seq,
        handle: Some(handle),
    })
}
fn publish(
    db: &mut Connection,
    seq: i64,
    chain: &str,
    identities: &str,
    payload: &[u8],
    digest: &str,
) -> Result<(), String> {
    let tx = db
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(|e| e.to_string())?;
    let old: Option<i64> = tx
        .query_row(
            "SELECT seq FROM gpu_checkpoint WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    if old.is_some_and(|n| n >= seq) {
        return Err("Checkpoint publication would move boundary backwards or repeat".into());
    }
    tx.execute(
        "INSERT OR REPLACE INTO gpu_checkpoint VALUES(1,?1,?2,?3,?4,?5)",
        params![seq, chain, identities, payload, digest],
    )
    .map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn publication_is_monotonic_and_failure_retains_predecessor() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE gpu_checkpoint(singleton INTEGER PRIMARY KEY,seq INTEGER,chain TEXT,identities TEXT,payload BLOB,sha256 TEXT);").unwrap();
        publish(&mut db, 2, "c", "i", b"a", "d").unwrap();
        assert!(publish(&mut db, 1, "c", "i", b"b", "d").is_err());
        assert!(publish(&mut db, 2, "c", "i", b"b", "d").is_err());
        db.execute_batch("CREATE TRIGGER fail BEFORE INSERT ON gpu_checkpoint BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
        assert!(publish(&mut db, 3, "c", "i", b"b", "d").is_err());
        assert_eq!(
            db.query_row("SELECT seq FROM gpu_checkpoint", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
}
