//! Durable sender-side storage. Synchronous methods belong on a blocking worker.
use crate::{
    client::{EventClient, Failure},
    delivery,
    events::{EventError, PaymentEvent},
};
use rusqlite::{Connection, TransactionBehavior};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
pub struct Outbox {
    connection: Mutex<Connection>,
}
impl Outbox {
    pub fn list(&self, after: &str) -> Result<Vec<crate::DeliveryRecord>, EventError> {
        delivery::list(
            &*self
                .connection
                .lock()
                .map_err(|_| EventError::CorruptData)?,
            after,
        )
    }
    /// Explicit credential rotation. Stop workers first; active leases block rebinding.
    pub fn rebind(&self, client: &EventClient) -> Result<(), EventError> {
        let mut c = self
            .connection
            .lock()
            .map_err(|_| EventError::CorruptData)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active: i64 = tx.query_row(
            "SELECT count(*) FROM delivery_jobs WHERE lease_until>?1",
            [chrono::Utc::now().timestamp_millis()],
            |r| r.get(0),
        )?;
        if active != 0 {
            return Err(EventError::Busy);
        }
        tx.execute("INSERT INTO outbox_binding(singleton,destination) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET destination=excluded.destination",[client.binding()])?;
        tx.commit()?;
        Ok(())
    }
    pub fn open(path: &Path) -> Result<Self, EventError> {
        let mut c = Connection::open(path)?;
        c.busy_timeout(Duration::from_millis(250))?;
        c.pragma_update(None, "journal_mode", "WAL")?;
        c.pragma_update(None, "synchronous", "FULL")?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version == 0 {
            let tables:i64=tx.query_row("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",[],|r|r.get(0))?;
            if tables != 0 {
                return Err(EventError::UnsupportedSchema);
            }
            delivery::schema(&tx)?;
            tx.execute_batch("CREATE TABLE outbox_binding(singleton INTEGER PRIMARY KEY CHECK(singleton=1),destination TEXT NOT NULL);")?;
            tx.pragma_update(None, "application_id", 0x524f4258)?;
            tx.pragma_update(None, "user_version", 1)?;
        }
        let application: i64 = tx.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        if application != 0x524f4258 || (version != 0 && version != 1) {
            return Err(EventError::UnsupportedSchema);
        }
        tx.commit()?;
        Ok(Self {
            connection: Mutex::new(c),
        })
    }
    pub fn enqueue(&self, event: &PaymentEvent) -> Result<bool, EventError> {
        let mut c = self
            .connection
            .lock()
            .map_err(|_| EventError::CorruptData)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let inserted = Self::enqueue_in_transaction(&tx, event)?;
        tx.commit()?;
        Ok(inserted)
    }
    /// Use on an initialized outbox database to atomically save business rows and an event.
    /// The caller owns commit/rollback. Do not change this database's application/user version.
    pub fn enqueue_in_transaction(
        tx: &rusqlite::Transaction<'_>,
        event: &PaymentEvent,
    ) -> Result<bool, EventError> {
        let payload = serde_json::to_string(event).map_err(|_| EventError::CorruptData)?;
        if payload.len() > 16 * 1024
            || event.version == 0
            || event.version > i64::MAX as u64
            || event.payment.id.trim().is_empty()
            || event.payment.id.len() > 256
        {
            return Err(EventError::InvalidInput("Invalid event"));
        }
        delivery::enqueue(
            tx,
            &event.event_id,
            &payload,
            chrono::Utc::now().timestamp_millis(),
        )
    }
    pub fn summary(&self) -> Result<delivery::DeliverySummary, EventError> {
        delivery::summary(
            &*self
                .connection
                .lock()
                .map_err(|_| EventError::CorruptData)?,
        )
    }
    pub fn redrive(&self, id: &str) -> Result<(), EventError> {
        delivery::redrive(
            &*self
                .connection
                .lock()
                .map_err(|_| EventError::CorruptData)?,
            id,
        )
    }
    /// One persisted claim. Cancellation leaves a recoverable lease; do not change IDs.
    pub async fn deliver_one(self: &Arc<Self>, client: &EventClient) -> Result<bool, EventError> {
        let binding = client.binding();
        let store = self.clone();
        let job = tokio::task::spawn_blocking(move || {
            {
                let c=store.connection.lock().map_err(|_|EventError::CorruptData)?;
                c.execute("INSERT INTO outbox_binding(singleton,destination) VALUES(1,?1) ON CONFLICT(singleton) DO NOTHING",[&binding])?;
            }
            delivery::claim(
                &mut *store
                    .connection
                    .lock()
                    .map_err(|_| EventError::CorruptData)?,
                chrono::Utc::now().timestamp_millis(),
                ("outbox",&binding),
            )
        })
        .await
        .map_err(|_| EventError::CorruptData)??;
        let Some(job) = job else { return Ok(false) };
        let event = serde_json::from_str(&job.payload).map_err(|_| EventError::CorruptData)?;
        let result = client.deliver(&event).await;
        let permanent = result.as_ref().err().is_some_and(|e| {
            matches!(e.reason,Failure::Rejected(code) if !matches!(code,429|500|502|503|504))
                || matches!(e.reason, Failure::InvalidEvent)
        });
        let error = result
            .as_ref()
            .err()
            .map(|e| format!("{:?}; outcome_unknown={}", e.reason, e.outcome_unknown))
            .unwrap_or_default();
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            delivery::finish(
                &*store
                    .connection
                    .lock()
                    .map_err(|_| EventError::CorruptData)?,
                &job,
                chrono::Utc::now().timestamp_millis(),
                result.is_ok(),
                permanent,
                &error,
                None,
            )
        })
        .await
        .map_err(|_| EventError::CorruptData)??;
        Ok(true)
    }
}
