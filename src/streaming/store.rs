use super::Result;
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    path::Path,
};
#[derive(Debug, Serialize, Deserialize)]
pub struct Pending {
    pub topic: String,
    pub offset: i64,
    pub body: String,
    pub created_ms: i64,
    pub events: usize,
    pub high_watermark: i64,
}
pub struct Store {
    db: Connection,
    _lock: File,
}
impl Store {
    pub fn open(path: impl AsRef<Path>, binding: &str) -> Result<Self> {
        let path = path.as_ref();
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.with_extension("lock"))?;
        lock.try_lock_exclusive()?;
        let db = Connection::open(path)?;
        let tables: i64 = db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )?;
        let app: i64 = db.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        if tables > 0 && app != 0x4b475031 {
            return Err("Not a Kafka GPU cursor database".into());
        }
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        db.pragma_update(None, "application_id", 0x4b475031)?;
        let integrity: String = db.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
        if integrity != "ok" {
            return Err("Cursor database integrity failure".into());
        }
        db.execute_batch("CREATE TABLE IF NOT EXISTS config(singleton INTEGER PRIMARY KEY CHECK(singleton=1),binding TEXT NOT NULL); CREATE TABLE IF NOT EXISTS cursors(topic TEXT PRIMARY KEY,next_offset INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS completions(topic TEXT NOT NULL,offset INTEGER NOT NULL,metrics TEXT NOT NULL,PRIMARY KEY(topic,offset)); CREATE TABLE IF NOT EXISTS pending(singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload TEXT NOT NULL);")?;
        db.execute("INSERT OR IGNORE INTO config VALUES(1,?1)", [binding])?;
        let old: String = db.query_row("SELECT binding FROM config", [], |r| r.get(0))?;
        if old != binding {
            return Err("Cursor database belongs to another broker/run/API".into());
        }
        Ok(Self { db, _lock: lock })
    }
    pub fn offset(&self, topic: &str) -> Result<i64> {
        Ok(self
            .db
            .query_row(
                "SELECT next_offset FROM cursors WHERE topic=?1",
                [topic],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }
    pub fn pending(&self) -> Result<Option<Pending>> {
        let raw: Option<String> = self
            .db
            .query_row("SELECT payload FROM pending", [], |r| r.get(0))
            .optional()?;
        Ok(raw.map(|s| serde_json::from_str(&s)).transpose()?)
    }
    pub fn prepare(&mut self, p: &Pending) -> Result<()> {
        if self.offset(&p.topic)? != p.offset {
            return Err("Non-contiguous prepared cursor".into());
        }
        self.db.execute(
            "INSERT INTO pending VALUES(1,?1)",
            [serde_json::to_string(p)?],
        )?;
        Ok(())
    }
    pub fn complete(&mut self, p: &Pending, metrics: &serde_json::Value) -> Result<()> {
        let tx = self.db.transaction()?;
        let raw: String = tx.query_row("SELECT payload FROM pending", [], |r| r.get(0))?;
        if raw != serde_json::to_string(p)? {
            return Err("Pending record changed".into());
        }
        tx.execute("INSERT INTO cursors VALUES(?1,?2) ON CONFLICT(topic) DO UPDATE SET next_offset=excluded.next_offset",params![p.topic,p.offset+1])?;
        tx.execute(
            "INSERT INTO completions VALUES(?1,?2,?3)",
            params![p.topic, p.offset, serde_json::to_string(metrics)?],
        )?;
        tx.execute("DELETE FROM pending", [])?;
        tx.commit()?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pending_survives_restart_and_cursor_only_advances_on_completion() {
        let dir = std::env::temp_dir().join(format!("kafka-cursor-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("state.db");
        let p = Pending {
            topic: "a".into(),
            offset: 0,
            body: "exact bytes".into(),
            created_ms: 1,
            events: 2,
            high_watermark: 1,
        };
        {
            let mut s = Store::open(&path, "binding").unwrap();
            s.prepare(&p).unwrap();
            assert_eq!(s.offset("a").unwrap(), 0);
            assert!(Store::open(&path, "binding").is_err());
        }
        {
            let mut s = Store::open(&path, "binding").unwrap();
            assert_eq!(s.pending().unwrap().unwrap().body, p.body);
            s.db.execute_batch("CREATE TRIGGER fail_completion BEFORE INSERT ON completions BEGIN SELECT RAISE(ABORT,'injected storage failure'); END;").unwrap();
            assert!(
                s.complete(&p, &serde_json::json!({"duplicate":true}))
                    .is_err()
            );
            assert_eq!(s.offset("a").unwrap(), 0);
            assert!(s.pending().unwrap().is_some());
            s.db.execute_batch("DROP TRIGGER fail_completion;").unwrap();
            s.complete(&p, &serde_json::json!({"duplicate":true}))
                .unwrap();
            assert_eq!(s.offset("a").unwrap(), 1);
            assert!(s.pending().unwrap().is_none());
            assert!(s.prepare(&p).is_err());
        }
        assert!(Store::open(&path, "different").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
