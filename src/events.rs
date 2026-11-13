//! Durable, versioned reconciliation of individual payment updates.
//! This synchronous store is intended to run on a bounded blocking worker.
use crate::{NextAction, Payment, ReconciliationResult};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::{Mutex, MutexGuard, TryLockError},
    time::Duration,
};

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize, utoipa::ToSchema)]
pub enum EventSource {
    Internal,
    Provider,
}
impl EventSource {
    fn key(self) -> &'static str {
        match self {
            Self::Internal => "Internal",
            Self::Provider => "Provider",
        }
    }
}
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PaymentEvent {
    /// Unique across both sources for the lifetime of this database.
    #[schema(min_length = 1, max_length = 128)]
    pub event_id: String,
    pub source: EventSource,
    /// Monotonically increasing per (payment ID, source); 1 through i64::MAX.
    #[schema(minimum = 1, maximum = 9223372036854775807u64)]
    pub version: u64,
    pub payment: Payment,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub enum EventOutcome {
    Applied,
    Stale,
    Unchanged,
}
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub enum WaitingStatus {
    AwaitingInternal,
    AwaitingProvider,
}
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum LiveStatus {
    Waiting(WaitingStatus),
    Compared(ReconciliationResult),
}
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct VersionedPayment {
    pub version: u64,
    pub event_id: String,
    pub received_at_ms: i64,
    pub payment: Payment,
}
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct LivePayment {
    pub payment_id: String,
    pub status: LiveStatus,
    pub recommended_action: Option<NextAction>,
    pub internal: Option<VersionedPayment>,
    pub provider: Option<VersionedPayment>,
    pub first_seen_at_ms: i64,
    pub waiting_until_ms: i64,
    pub evaluated_at_ms: i64,
}
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct EventReceipt {
    pub event_id: String,
    /// True only for an exact retry of a previously committed event ID.
    pub duplicate_event: bool,
    /// Original outcome for a retry; payment contains the current state.
    pub outcome: EventOutcome,
    pub payment: LivePayment,
}

#[derive(Debug)]
pub enum EventError {
    InvalidInput(&'static str),
    EventIdConflict,
    VersionConflict,
    RevisionConflict,
    NotFound,
    Busy,
    Database(rusqlite::Error),
    Io(std::io::Error),
    CorruptData,
    UnsupportedSchema,
}
impl std::fmt::Display for EventError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidInput(message) => message,
            Self::EventIdConflict => "Event ID was already used with different content",
            Self::RevisionConflict => {
                "Investigation revision or request identity conflicts; reload history"
            }
            Self::VersionConflict => "Source version was already used with different payment data",
            Self::NotFound => "Payment not found",
            Self::Busy => "Event storage is busy; retry the same event",
            Self::Database(_) | Self::Io(_) | Self::CorruptData => "Event storage operation failed",
            Self::UnsupportedSchema => "Unsupported event database schema",
        })
    }
}
impl std::error::Error for EventError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(e) => Some(e),
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}
impl From<rusqlite::Error> for EventError {
    fn from(error: rusqlite::Error) -> Self {
        if matches!(&error, rusqlite::Error::SqliteFailure(e, _) if matches!(e.code,
            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked))
        {
            Self::Busy
        } else {
            Self::Database(error)
        }
    }
}

/// Successful operation timings in milliseconds. Commit includes SQLite's
/// synchronization/checkpoint work; it is not a direct disk-only measurement.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TransactionTimings {
    pub prepare_ms: f64,
    pub begin_ms: f64,
    pub work_ms: f64,
    pub commit_ms: f64,
}
fn elapsed_ms(start: std::time::Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}
pub struct EventStore {
    connection: Mutex<Connection>,
    wait_ms: i64,
}
impl EventStore {
    /// Opens a local database. Do not put a WAL database on a network share.
    pub fn open(path: impl AsRef<Path>, waiting_period: Duration) -> Result<Self, EventError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(EventError::Io)?;
        }
        let connection = Connection::open(path)?;
        Self::initialize(connection, waiting_period, true)
    }
    /// Explicit ephemeral storage for tests or embedded experiments only.
    pub fn in_memory(waiting_period: Duration) -> Result<Self, EventError> {
        Self::initialize(Connection::open_in_memory()?, waiting_period, false)
    }
    fn initialize(
        mut connection: Connection,
        wait: Duration,
        durable: bool,
    ) -> Result<Self, EventError> {
        let wait_ms = i64::try_from(wait.as_millis())
            .map_err(|_| EventError::InvalidInput("Waiting period is too large"))?;
        if wait_ms > 86_400_000 {
            return Err(EventError::InvalidInput(
                "Waiting period must not exceed one day",
            ));
        }
        connection.busy_timeout(Duration::from_millis(250))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        if durable {
            let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
            if mode != "wal" {
                return Err(EventError::CorruptData);
            }
        }
        connection.pragma_update(None, "synchronous", "FULL")?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version == 0 {
            // Refuse to initialize over an unrelated existing database.
            let tables: i64 = tx.query_row("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'", [], |r| r.get(0))?;
            if tables != 0 {
                return Err(EventError::UnsupportedSchema);
            }
            tx.execute_batch(
                "
                CREATE TABLE payments(
                    payment_id TEXT PRIMARY KEY NOT NULL,
                    first_seen_at_ms INTEGER NOT NULL,
                    waiting_until_ms INTEGER NOT NULL
                );
                CREATE TABLE event_log(
                    event_id TEXT PRIMARY KEY NOT NULL,
                    payload TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    received_at_ms INTEGER NOT NULL
                );
                CREATE TABLE source_state(
                    payment_id TEXT NOT NULL REFERENCES payments(payment_id),
                    source TEXT NOT NULL CHECK(source IN ('Internal','Provider')),
                    version INTEGER NOT NULL CHECK(version > 0),
                    payload TEXT NOT NULL,
                    event_id TEXT NOT NULL REFERENCES event_log(event_id),
                    received_at_ms INTEGER NOT NULL,
                    PRIMARY KEY(payment_id, source)
                );
                PRAGMA user_version=1;
            ",
            )?;
        } else if version != 1 && version != 2 && version != 3 {
            return Err(EventError::UnsupportedSchema);
        }
        if version < 2 {
            history::migrate(&tx)?;
        }
        if version < 3 {
            workflow::migrate(&tx)?;
        }
        tx.prepare("SELECT status,signature,revision,opened_at FROM case_state LIMIT 0")?;
        tx.prepare("SELECT sequence,note_id,request FROM payment_history LIMIT 0")?;
        tx.prepare("SELECT id,payload,state,attempts,available_at,lease_token,lease_until,last_error,created_at,completed_at FROM delivery_jobs LIMIT 0")?;
        tx.prepare("SELECT owner,workflow,version FROM case_workflow LIMIT 0")?;
        tx.prepare("SELECT payment_id,deadline FROM pending_deadlines LIMIT 0")?;
        tx.prepare("SELECT endpoint FROM notification_settings LIMIT 0")?;
        // Verify expected tables/columns before accepting traffic.
        tx.prepare("SELECT p.payment_id,p.first_seen_at_ms,p.waiting_until_ms,s.version,s.payload,s.event_id,s.received_at_ms,e.outcome,e.received_at_ms,e.payload FROM payments p LEFT JOIN source_state s ON s.payment_id=p.payment_id LEFT JOIN event_log e ON e.event_id=s.event_id LIMIT 0")?;
        tx.commit()?;
        Ok(Self {
            connection: Mutex::new(connection),
            wait_ms,
        })
    }
    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, Connection>, EventError> {
        self.connection.try_lock().map_err(|e| match e {
            TryLockError::WouldBlock => EventError::Busy,
            TryLockError::Poisoned(_) => EventError::CorruptData,
        })
    }
    pub fn apply(&self, event: PaymentEvent) -> Result<EventReceipt, EventError> {
        self.apply_at(event, chrono::Utc::now().timestamp_millis())
    }
    pub fn apply_profiled(
        &self,
        event: PaymentEvent,
    ) -> Result<(EventReceipt, TransactionTimings), EventError> {
        self.apply_at_profiled(event, chrono::Utc::now().timestamp_millis())
    }
    fn apply_at(&self, event: PaymentEvent, now: i64) -> Result<EventReceipt, EventError> {
        self.apply_at_profiled(event, now).map(|(value, _)| value)
    }
    fn apply_at_profiled(
        &self,
        event: PaymentEvent,
        now: i64,
    ) -> Result<(EventReceipt, TransactionTimings), EventError> {
        let prepare = std::time::Instant::now();
        validate_id(&event.event_id, 128)?;
        validate_id(&event.payment.id, 256)?;
        let version = i64::try_from(event.version).ok().filter(|v| *v > 0).ok_or(
            EventError::InvalidInput("Version must be between 1 and 9223372036854775807"),
        )?;
        let canonical = serde_json::to_string(&event).map_err(|_| EventError::CorruptData)?;
        let payload = serde_json::to_string(&event.payment).map_err(|_| EventError::CorruptData)?;
        let mut connection = self.lock()?;
        let mut timings = TransactionTimings {
            prepare_ms: elapsed_ms(prepare),
            ..Default::default()
        };
        let begin = std::time::Instant::now();
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        timings.begin_ms = elapsed_ms(begin);
        let work = std::time::Instant::now();
        if tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM payments WHERE payment_id=?1)",
            [&event.payment.id],
            |r| r.get::<_, bool>(0),
        )? {
            history::capture(&tx, &event.payment.id, now)?;
        }
        let existing: Option<(String, String)> = tx
            .query_row(
                "SELECT payload,outcome FROM event_log WHERE event_id=?1",
                [&event.event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((stored, outcome)) = existing {
            if stored != canonical {
                return Err(EventError::EventIdConflict);
            }
            let receipt = EventReceipt {
                event_id: event.event_id,
                duplicate_event: true,
                outcome: serde_json::from_str(&outcome).map_err(|_| EventError::CorruptData)?,
                payment: history::capture(&tx, &event.payment.id, now)?,
            };
            timings.work_ms = elapsed_ms(work);
            let commit = std::time::Instant::now();
            tx.commit()?;
            timings.commit_ms = elapsed_ms(commit);
            return Ok((receipt, timings));
        }
        let previous: Option<(i64, String)> = tx
            .query_row(
                "SELECT version,payload FROM source_state WHERE payment_id=?1 AND source=?2",
                params![event.payment.id, event.source.key()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let outcome = match previous {
            Some((old, _)) if version < old => EventOutcome::Stale,
            Some((old, old_payload)) if version == old => {
                if old_payload != payload {
                    return Err(EventError::VersionConflict);
                }
                EventOutcome::Unchanged
            }
            _ => EventOutcome::Applied,
        };
        let deadline = now
            .checked_add(self.wait_ms)
            .ok_or(EventError::InvalidInput("Invalid receipt time"))?;
        tx.execute(
            "INSERT INTO event_log(event_id,payload,outcome,received_at_ms) VALUES(?1,?2,?3,?4)",
            params![
                event.event_id,
                canonical,
                serde_json::to_string(&outcome).map_err(|_| EventError::CorruptData)?,
                now
            ],
        )?;
        if outcome == EventOutcome::Applied {
            tx.execute("INSERT INTO payments(payment_id,first_seen_at_ms,waiting_until_ms) VALUES(?1,?2,?3) ON CONFLICT(payment_id) DO NOTHING",
                params![event.payment.id, now, deadline])?;
            tx.execute("INSERT INTO source_state(payment_id,source,version,payload,event_id,received_at_ms) VALUES(?1,?2,?3,?4,?5,?6)
                ON CONFLICT(payment_id,source) DO UPDATE SET version=excluded.version,payload=excluded.payload,event_id=excluded.event_id,received_at_ms=excluded.received_at_ms",
                params![event.payment.id, event.source.key(), version, payload, event.event_id, now])?;
        }
        let receipt = EventReceipt {
            event_id: event.event_id,
            duplicate_event: false,
            outcome,
            payment: history::capture(&tx, &event.payment.id, now)?,
        };
        // Acknowledgement follows commit; event identity and current state are atomic.
        timings.work_ms = elapsed_ms(work);
        let commit = std::time::Instant::now();
        tx.commit()?;
        timings.commit_ms = elapsed_ms(commit);
        Ok((receipt, timings))
    }
    pub fn status(&self, payment_id: &str) -> Result<LivePayment, EventError> {
        self.status_at(payment_id, chrono::Utc::now().timestamp_millis())
    }
    pub fn status_profiled(
        &self,
        payment_id: &str,
    ) -> Result<(LivePayment, TransactionTimings), EventError> {
        self.status_at_profiled(payment_id, chrono::Utc::now().timestamp_millis())
    }
    fn status_at(&self, payment_id: &str, now: i64) -> Result<LivePayment, EventError> {
        self.status_at_profiled(payment_id, now)
            .map(|(value, _)| value)
    }
    fn status_at_profiled(
        &self,
        payment_id: &str,
        now: i64,
    ) -> Result<(LivePayment, TransactionTimings), EventError> {
        let prepare = std::time::Instant::now();
        validate_id(payment_id, 256)?;
        let mut connection = self.lock()?;
        // One read snapshot, including when another process writes to this DB.
        let mut timings = TransactionTimings {
            prepare_ms: elapsed_ms(prepare),
            ..Default::default()
        };
        let begin = std::time::Instant::now();
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        timings.begin_ms = elapsed_ms(begin);
        let work = std::time::Instant::now();
        let payment = history::capture(&tx, payment_id, now)?;
        timings.work_ms = elapsed_ms(work);
        let commit = std::time::Instant::now();
        tx.commit()?;
        timings.commit_ms = elapsed_ms(commit);
        Ok((payment, timings))
    }
}
fn validate_id(id: &str, max: usize) -> Result<(), EventError> {
    if id.trim().is_empty() || id.len() > max || id.chars().any(char::is_control) {
        Err(EventError::InvalidInput(
            "IDs must be nonblank, within their byte limit and contain no control characters",
        ))
    } else {
        Ok(())
    }
}
fn read_payment(connection: &Connection, id: &str, now: i64) -> Result<LivePayment, EventError> {
    let (first, deadline): (i64, i64) = connection
        .query_row(
            "SELECT first_seen_at_ms,waiting_until_ms FROM payments WHERE payment_id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(EventError::NotFound)?;
    let source = |name: &str| -> Result<Option<VersionedPayment>, EventError> {
        let value: Option<(i64, String, String, i64)> = connection.query_row(
            "SELECT version,payload,event_id,received_at_ms FROM source_state WHERE payment_id=?1 AND source=?2",
            params![id,name], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        value
            .map(|(version, payload, event_id, received_at_ms)| {
                Ok(VersionedPayment {
                    version: u64::try_from(version).map_err(|_| EventError::CorruptData)?,
                    payment: serde_json::from_str(&payload).map_err(|_| EventError::CorruptData)?,
                    event_id,
                    received_at_ms,
                })
            })
            .transpose()
    };
    let internal = source("Internal")?;
    let provider = source("Provider")?;
    let status = match (&internal, &provider) {
        (Some(a), Some(b)) => {
            LiveStatus::Compared(crate::reconciliation::reconcile(&a.payment, &b.payment))
        }
        (Some(_), None) if now < deadline => LiveStatus::Waiting(WaitingStatus::AwaitingProvider),
        (None, Some(_)) if now < deadline => LiveStatus::Waiting(WaitingStatus::AwaitingInternal),
        (Some(_), None) => LiveStatus::Compared(ReconciliationResult::MissingFromProvider),
        (None, Some(_)) => LiveStatus::Compared(ReconciliationResult::MissingFromInternal),
        (None, None) => return Err(EventError::CorruptData),
    };
    let recommended_action = match &status {
        LiveStatus::Compared(result) => Some(crate::recovery::recommend_action(result)),
        LiveStatus::Waiting(_) => None,
    };
    Ok(LivePayment {
        payment_id: id.into(),
        status,
        recommended_action,
        internal,
        provider,
        first_seen_at_ms: first,
        waiting_until_ms: deadline,
        evaluated_at_ms: now,
    })
}
#[cfg(test)]
mod tests;

mod operations;
pub use operations::{
    DiscrepancyPage, DiscrepancyQuery, StorageHealth, backup_database, verify_backup,
};

#[cfg(test)]
mod operations_tests;

mod history;
pub use history::{
    HistoryEntry, HistoryPage, Investigation, InvestigationAction, InvestigationReceipt,
};

#[cfg(test)]
mod history_tests;

mod workflow;
pub use workflow::{CaseEntry, CasePage, CaseQuery, CaseUpdate, WorkflowStatus};

mod archive;
pub use archive::{
    ArchiveManifest, RecoveryDrill, archive_database, recovery_drill, verify_archive,
};

#[cfg(test)]
mod workflow_tests;
