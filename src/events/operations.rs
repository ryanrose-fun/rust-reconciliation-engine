use super::*;

#[derive(Default, Deserialize, utoipa::ToSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DiscrepancyQuery {
    pub after: Option<String>,
    pub limit: Option<usize>,
    pub status: Option<String>,
    pub currency: Option<crate::Currency>,
    pub source: Option<EventSource>,
    pub min_age_ms: Option<u64>,
}
#[derive(Serialize, utoipa::ToSchema)]
pub struct DiscrepancyPage {
    pub entries: Vec<LivePayment>,
    /// Last scanned ID, not necessarily a returned entry. Follow even empty pages.
    pub next_after: Option<String>,
    pub scanned: usize,
    pub evaluated_at_ms: i64,
}
#[derive(Serialize, utoipa::ToSchema)]
pub struct StorageHealth {
    pub payments: i64,
    pub awaiting_counterpart: i64,
    pub overdue_counterpart: i64,
}
impl EventStore {
    /// A bounded keyset scan: never inspect more than 1,000 payments per call.
    /// Each page is a consistent read snapshot; pages across calls are live views.
    pub fn discrepancies(&self, query: DiscrepancyQuery) -> Result<DiscrepancyPage, EventError> {
        let limit = query.limit.unwrap_or(100);
        if !(1..=100).contains(&limit) || query.min_age_ms.is_some_and(|v| v > i64::MAX as u64) {
            return Err(EventError::InvalidInput(
                "Limit must be 1..100 and age must fit i64",
            ));
        }
        if let Some(id) = &query.after {
            validate_id(id, 256)?;
        }
        if let Some(status) = &query.status
            && ![
                "AmountMismatch",
                "StatusMismatch",
                "AmountAndStatusMismatch",
                "CurrencyMismatch",
                "AmountAndCurrencyMismatch",
                "StatusAndCurrencyMismatch",
                "AmountStatusAndCurrencyMismatch",
                "MissingFromProvider",
                "MissingFromInternal",
            ]
            .contains(&status.as_str())
        {
            return Err(EventError::InvalidInput("Unknown discrepancy status"));
        }
        let now = chrono::Utc::now().timestamp_millis();
        let mut connection = self.lock()?;
        let tx = connection.transaction()?;
        let ids = {
            let mut stmt = tx.prepare("SELECT payment_id FROM payments WHERE payment_id > ?1 ORDER BY payment_id LIMIT 1001")?;
            stmt.query_map([query.after.as_deref().unwrap_or("")], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        let mut entries = Vec::new();
        let mut scanned = 0;
        for id in ids.iter().take(1000) {
            scanned += 1;
            let value = read_payment(&tx, id, now)?;
            if !matches!(value.status, LiveStatus::Compared(ref r) if *r != ReconciliationResult::Matched)
            {
                continue;
            }
            if query.status.as_ref().is_some_and(|s| {
                serde_json::to_value(&value.status)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .as_ref()
                    != Some(s)
            }) {
                continue;
            }
            if now.saturating_sub(value.first_seen_at_ms) < query.min_age_ms.unwrap_or(0) as i64 {
                continue;
            }
            let sources: Vec<_> = match query.source {
                Some(EventSource::Internal) => value.internal.iter().collect(),
                Some(EventSource::Provider) => value.provider.iter().collect(),
                None => value.internal.iter().chain(value.provider.iter()).collect(),
            };
            if sources.is_empty()
                || query
                    .currency
                    .is_some_and(|c| !sources.iter().any(|v| v.payment.currency == c))
            {
                continue;
            }
            entries.push(value);
            if entries.len() == limit {
                break;
            }
        }
        let next_after = if scanned < ids.len() {
            ids.get(scanned - 1).cloned()
        } else {
            None
        };
        tx.commit()?;
        Ok(DiscrepancyPage {
            entries,
            next_after,
            scanned,
            evaluated_at_ms: now,
        })
    }
    pub fn health(&self) -> Result<StorageHealth, EventError> {
        let connection = self.lock()?;
        // One SQL statement gives one snapshot. No payment payloads are decoded.
        Ok(connection.query_row("SELECT count(*), coalesce(sum(CASE WHEN (i.payment_id IS NULL OR p.payment_id IS NULL) AND a.waiting_until_ms > ?1 THEN 1 ELSE 0 END),0), coalesce(sum(CASE WHEN (i.payment_id IS NULL OR p.payment_id IS NULL) AND a.waiting_until_ms <= ?1 THEN 1 ELSE 0 END),0) FROM payments a LEFT JOIN source_state i ON a.payment_id=i.payment_id AND i.source='Internal' LEFT JOIN source_state p ON a.payment_id=p.payment_id AND p.source='Provider'", [chrono::Utc::now().timestamp_millis()], |r| Ok(StorageHealth { payments:r.get(0)?, awaiting_counterpart:r.get(1)?, overdue_counterpart:r.get(2)? }))?)
    }
    /// SQLite creates a consistent standalone backup, including committed WAL data.
    /// The destination must not exist. Never copy a live database file directly.
    pub fn backup(&self, destination: &Path) -> Result<(), EventError> {
        snapshot(&*self.lock()?, destination)
    }
}
pub fn backup_database(source: &Path, destination: &Path) -> Result<(), EventError> {
    verify_backup(source)?;
    let connection =
        Connection::open_with_flags(source, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.busy_timeout(Duration::from_millis(250))?;
    snapshot(&connection, destination)
}
fn snapshot(connection: &Connection, destination: &Path) -> Result<(), EventError> {
    if destination.exists() {
        return Err(EventError::InvalidInput(
            "Backup destination already exists",
        ));
    }
    connection.execute(
        "VACUUM INTO ?1",
        [destination
            .to_str()
            .ok_or(EventError::InvalidInput("Backup path must be UTF-8"))?],
    )?;
    // Do not rely on VACUUM INTO alone to flush the output before reporting
    // success. Sync the standalone file, and its directory on Unix.
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(destination)
        .and_then(|file| file.sync_all())
        .map_err(EventError::Io)?;
    #[cfg(unix)]
    std::fs::File::open(
        destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )
    .and_then(|directory| directory.sync_all())
    .map_err(EventError::Io)?;
    verify_backup(destination)?;
    Ok(())
}
pub fn verify_backup(path: &Path) -> Result<(), EventError> {
    let c = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let integrity: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if integrity != "ok" {
        return Err(EventError::CorruptData);
    }
    let version: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version != 1 && version != 2 {
        return Err(EventError::UnsupportedSchema);
    }
    if version == 2 {
        c.prepare("SELECT sequence,note_id,request FROM payment_history LIMIT 0")?;
        c.prepare("SELECT status,signature,revision FROM case_state LIMIT 0")?;
    }
    let violations: i64 =
        c.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
            r.get(0)
        })?;
    if violations != 0 {
        return Err(EventError::CorruptData);
    }
    c.prepare("SELECT p.payment_id,p.first_seen_at_ms,p.waiting_until_ms,s.version,s.payload,s.event_id,s.received_at_ms,e.outcome,e.received_at_ms,e.payload FROM payments p LEFT JOIN source_state s ON s.payment_id=p.payment_id LEFT JOIN event_log e ON e.event_id=s.event_id LIMIT 0")?;
    Ok(())
}
