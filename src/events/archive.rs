//! Non-destructive, verified archival and repeatable recovery drills.
use super::*;
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader, Read, Write};
fn digest_file(path: &Path) -> Result<String, EventError> {
    let mut file = std::fs::File::open(path).map_err(EventError::Io)?;
    let mut hash = Sha256::new();
    let mut bytes = [0u8; 65536];
    loop {
        let n = file.read(&mut bytes).map_err(EventError::Io)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn write_json(path: &Path, value: &impl Serialize) -> Result<(), EventError> {
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(EventError::Io)?;
    file.write_all(&serde_json::to_vec_pretty(value).map_err(|_| EventError::CorruptData)?)
        .map_err(EventError::Io)?;
    file.sync_all().map_err(EventError::Io)?;
    Ok(())
}
#[derive(Serialize, Deserialize)]
pub struct ArchiveManifest {
    pub format: String,
    pub cutoff_ms: i64,
    pub history_records: u64,
    pub snapshot_sha256: String,
    pub history_sha256: String,
    pub created_at_ms: i64,
}
/// Retain the complete snapshot (including dedupe identities) and export older history.
/// No deletion from the live store. The manifest is written last as completion marker.
pub fn archive_database(
    source: &Path,
    directory: &Path,
    cutoff_ms: i64,
) -> Result<ArchiveManifest, EventError> {
    if cutoff_ms < 0 || cutoff_ms > chrono::Utc::now().timestamp_millis() {
        return Err(EventError::InvalidInput(
            "Archive cutoff must be a past UTC millisecond timestamp",
        ));
    }
    std::fs::create_dir(directory).map_err(EventError::Io)?;
    let snapshot = directory.join("snapshot.sqlite3");
    super::backup_database(source, &snapshot)?;
    let c = Connection::open_with_flags(&snapshot, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(directory.join("history.jsonl"))
        .map_err(EventError::Io)?;
    let mut count = 0;
    let version: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version >= 2 {
        let mut query=c.prepare("SELECT sequence,payment_id,kind,recorded_at_ms,effective_at_ms,from_status,status,evidence,actor,note,revision,note_id,request FROM payment_history WHERE recorded_at_ms<=?1 ORDER BY sequence")?;
        let mut rows = query.query([cutoff_ms])?;
        while let Some(row) = rows.next()? {
            let fields = [
                "sequence",
                "payment_id",
                "kind",
                "recorded_at_ms",
                "effective_at_ms",
                "from_status",
                "status",
                "evidence",
                "actor",
                "note",
                "revision",
                "note_id",
                "request",
            ];
            let mut record = serde_json::Map::new();
            for (i, name) in fields.iter().enumerate() {
                let v = match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => serde_json::Value::Null,
                    rusqlite::types::ValueRef::Integer(v) => v.into(),
                    rusqlite::types::ValueRef::Text(v) => serde_json::Value::String(
                        String::from_utf8(v.to_vec()).map_err(|_| EventError::CorruptData)?,
                    ),
                    _ => return Err(EventError::CorruptData),
                };
                record.insert((*name).into(), v);
            }
            serde_json::to_writer(&mut file, &record).map_err(|_| EventError::CorruptData)?;
            file.write_all(b"\n").map_err(EventError::Io)?;
            count += 1;
        }
    }
    file.sync_all().map_err(EventError::Io)?;
    drop(file);
    let manifest = ArchiveManifest {
        format: "reconcile-archive-v1".into(),
        cutoff_ms,
        history_records: count,
        snapshot_sha256: digest_file(&snapshot)?,
        history_sha256: digest_file(&directory.join("history.jsonl"))?,
        created_at_ms: chrono::Utc::now().timestamp_millis(),
    };
    write_json(&directory.join("manifest.json"), &manifest)?;
    #[cfg(unix)]
    std::fs::File::open(directory)
        .and_then(|f| f.sync_all())
        .map_err(EventError::Io)?;
    verify_archive(directory)?;
    Ok(manifest)
}
pub fn verify_archive(directory: &Path) -> Result<ArchiveManifest, EventError> {
    let manifest: ArchiveManifest = serde_json::from_slice(
        &std::fs::read(directory.join("manifest.json")).map_err(EventError::Io)?,
    )
    .map_err(|_| EventError::CorruptData)?;
    if manifest.format != "reconcile-archive-v1"
        || manifest.snapshot_sha256 != digest_file(&directory.join("snapshot.sqlite3"))?
        || manifest.history_sha256 != digest_file(&directory.join("history.jsonl"))?
    {
        return Err(EventError::CorruptData);
    }
    super::verify_backup(&directory.join("snapshot.sqlite3"))?;
    let count = BufReader::new(
        std::fs::File::open(directory.join("history.jsonl")).map_err(EventError::Io)?,
    )
    .lines()
    .try_fold(0u64, |count, line| -> Result<_, EventError> {
        let line = line.map_err(EventError::Io)?;
        let _: serde_json::Value =
            serde_json::from_str(&line).map_err(|_| EventError::CorruptData)?;
        Ok(count + 1)
    })?;
    if count != manifest.history_records {
        return Err(EventError::CorruptData);
    }
    Ok(manifest)
}
fn logical_digest(path: &Path) -> Result<String, EventError> {
    let c = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut tables=c.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")?;
    let tables = tables
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut hash = Sha256::new();
    for table in tables {
        hash.update(table.as_bytes());
        let mut q = c.prepare(&format!(
            "SELECT * FROM \"{}\" ORDER BY rowid",
            table.replace('"', "\"\"")
        ))?;
        let n = q.column_count();
        let mut rows = q.query([])?;
        while let Some(row) = rows.next()? {
            for i in 0..n {
                let bytes = format!("{:?}", row.get_ref(i)?);
                hash.update((bytes.len() as u64).to_le_bytes());
                hash.update(bytes.as_bytes());
            }
        }
    }
    Ok(format!("{:x}", hash.finalize()))
}
#[derive(Serialize)]
pub struct RecoveryDrill {
    pub passed: bool,
    pub logical_sha256: String,
    pub elapsed_ms: u128,
}
pub fn recovery_drill(source: &Path, directory: &Path) -> Result<RecoveryDrill, EventError> {
    let started = std::time::Instant::now();
    std::fs::create_dir(directory).map_err(EventError::Io)?;
    let backup = directory.join("backup.sqlite3");
    let restored = directory.join("restored.sqlite3");
    super::backup_database(source, &backup)?;
    super::backup_database(&backup, &restored)?;
    let digest = logical_digest(&backup)?;
    if digest != logical_digest(&restored)? {
        return Err(EventError::CorruptData);
    }
    let result = RecoveryDrill {
        passed: true,
        logical_sha256: digest,
        elapsed_ms: started.elapsed().as_millis(),
    };
    write_json(&directory.join("drill.json"), &result)?;
    Ok(result)
}
