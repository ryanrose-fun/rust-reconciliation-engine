//! SQLite delivery queue shared by sender outboxes and transactional webhooks.
use crate::events::EventError;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;
pub(crate) const CAPACITY: i64 = 10_000;
#[cfg(feature = "client")]
pub(crate) const MAX_ATTEMPTS: i64 = 8;
pub(crate) fn schema(c: &Connection) -> Result<(), EventError> {
    c.execute_batch("CREATE TABLE delivery_jobs(id TEXT PRIMARY KEY, payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, available_at INTEGER NOT NULL, lease_token TEXT, lease_until INTEGER NOT NULL DEFAULT 0, last_error TEXT, created_at INTEGER NOT NULL, completed_at INTEGER);
        CREATE INDEX delivery_due ON delivery_jobs(state,available_at,lease_until); CREATE INDEX delivery_active ON delivery_jobs(state) WHERE state!='delivered';")?;
    Ok(())
}
pub(crate) fn enqueue(
    tx: &Transaction<'_>,
    id: &str,
    payload: &str,
    now: i64,
) -> Result<bool, EventError> {
    if id.trim().is_empty() || id.len() > 128 || payload.len() > 65_536 {
        return Err(EventError::InvalidInput(
            "Invalid delivery identity or payload size",
        ));
    }
    let old: Option<String> = tx
        .query_row("SELECT payload FROM delivery_jobs WHERE id=?1", [id], |r| {
            r.get(0)
        })
        .optional()?;
    if let Some(old) = old {
        if old != payload {
            return Err(EventError::EventIdConflict);
        }
        return Ok(false);
    }
    let count: i64 = tx.query_row(
        "SELECT count(*) FROM delivery_jobs WHERE state!='delivered'",
        [],
        |r| r.get(0),
    )?;
    if count >= CAPACITY {
        return Err(EventError::Busy);
    }
    tx.execute(
        "INSERT INTO delivery_jobs(id,payload,available_at,created_at) VALUES(?1,?2,?3,?3)",
        params![id, payload, now],
    )?;
    Ok(true)
}
#[cfg(feature = "client")]
#[derive(Debug)]
pub(crate) struct Claim {
    pub id: String,
    pub payload: String,
    pub token: String,
    pub attempts: i64,
}
#[cfg(feature = "client")]
pub(crate) fn claim(
    c: &mut Connection,
    now: i64,
    binding: (&str, &str),
) -> Result<Option<Claim>, EventError> {
    let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let sql = if binding.0 == "outbox" {
        "SELECT destination FROM outbox_binding WHERE singleton=1"
    } else {
        "SELECT endpoint FROM notification_settings WHERE singleton=1"
    };
    let destination: String = tx.query_row(sql, [], |r| r.get(0))?;
    if destination != binding.1 {
        return Err(EventError::InvalidInput(
            "Delivery destination changed; restart worker",
        ));
    }
    // A crash on the final attempt must still become visible as an exhausted job.
    tx.execute("UPDATE delivery_jobs SET state='exhausted',lease_token=NULL,last_error='Attempt limit reached; outcome may be unknown' WHERE state='pending' AND attempts>=?1 AND lease_until<=?2",params![MAX_ATTEMPTS,now])?;
    let job: Option<(String,String,i64)> = tx.query_row("SELECT id,payload,attempts FROM delivery_jobs WHERE state='pending' AND available_at<=?1 AND lease_until<=?1 AND attempts<?2 ORDER BY available_at,id LIMIT 1",params![now,MAX_ATTEMPTS],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let result = if let Some((id, payload, attempts)) = job {
        let token = uuid::Uuid::new_v4().to_string();
        // The HTTP client's total deadline is at most 120 seconds.
        tx.execute("UPDATE delivery_jobs SET attempts=attempts+1,lease_token=?2,lease_until=?3 WHERE id=?1",params![id,token,now.saturating_add(130_000)])?;
        Some(Claim {
            id,
            payload,
            token,
            attempts: attempts + 1,
        })
    } else {
        None
    };
    tx.commit()?;
    Ok(result)
}
#[cfg(feature = "client")]
pub(crate) fn finish(
    c: &Connection,
    job: &Claim,
    now: i64,
    success: bool,
    permanent: bool,
    error: &str,
    retry_after_ms: Option<i64>,
) -> Result<(), EventError> {
    let state = if success {
        "delivered"
    } else if permanent || job.attempts >= MAX_ATTEMPTS {
        "exhausted"
    } else {
        "pending"
    };
    let cap = (1000i64 * (1i64 << job.attempts.min(12))).min(300_000);
    let jitter = (uuid::Uuid::new_v4().as_u128() % cap as u128) as i64;
    let delay = retry_after_ms
        .unwrap_or(cap / 2 + jitter / 2)
        .clamp(0, 86_400_000);
    let changed=c.execute("UPDATE delivery_jobs SET state=?3,available_at=?4,lease_token=NULL,lease_until=0,last_error=?5,completed_at=?6 WHERE id=?1 AND lease_token=?2",params![job.id,job.token,state,now.saturating_add(delay),if success {None} else {Some(error)},if success {Some(now)} else {None}])?;
    if changed != 1 {
        return Err(EventError::RevisionConflict);
    }
    Ok(())
}
#[derive(Debug, Serialize)]
pub struct DeliverySummary {
    pub pending: i64,
    pub delivered: i64,
    pub exhausted: i64,
    pub oldest_pending_at: Option<i64>,
}
#[derive(Serialize)]
pub struct DeliveryRecord {
    pub id: String,
    pub state: String,
    pub attempts: i64,
    pub available_at_ms: i64,
    pub lease_until_ms: i64,
    pub last_error: Option<String>,
}
#[cfg(feature = "client")]
pub(crate) fn list(c: &Connection, after: &str) -> Result<Vec<DeliveryRecord>, EventError> {
    let mut q=c.prepare("SELECT id,state,attempts,available_at,lease_until,last_error FROM delivery_jobs WHERE id>?1 ORDER BY id LIMIT 100")?;
    Ok(q.query_map([after], |r| {
        Ok(DeliveryRecord {
            id: r.get(0)?,
            state: r.get(1)?,
            attempts: r.get(2)?,
            available_at_ms: r.get(3)?,
            lease_until_ms: r.get(4)?,
            last_error: r.get(5)?,
        })
    })?
    .collect::<Result<Vec<_>, _>>()?)
}
#[cfg(feature = "client")]
pub(crate) fn summary(c: &Connection) -> Result<DeliverySummary, EventError> {
    Ok(c.query_row("SELECT count(*) FILTER(WHERE state='pending'),count(*) FILTER(WHERE state='delivered'),count(*) FILTER(WHERE state='exhausted'),min(created_at) FILTER(WHERE state='pending') FROM delivery_jobs",[],|r|Ok(DeliverySummary{pending:r.get(0)?,delivered:r.get(1)?,exhausted:r.get(2)?,oldest_pending_at:r.get(3)?}))?)
}
#[cfg(feature = "client")]
pub(crate) fn redrive(c: &Connection, id: &str) -> Result<(), EventError> {
    if c.execute("UPDATE delivery_jobs SET state='pending',attempts=0,available_at=0,lease_token=NULL,lease_until=0,last_error=NULL WHERE id=?1 AND state='exhausted'",[id])?!=1 {return Err(EventError::InvalidInput("Only an exhausted delivery can be redriven"));}
    Ok(())
}

#[cfg(all(test, feature = "client"))]
mod tests {
    use super::*;
    fn queue() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        schema(&c).unwrap();
        c.execute_batch("CREATE TABLE outbox_binding(singleton INTEGER,destination TEXT); INSERT INTO outbox_binding VALUES(1,'target');").unwrap();
        c
    }
    #[test]
    fn active_capacity_is_bounded_but_delivered_receipts_do_not_block_enqueue() {
        let mut c = queue();
        c.execute_batch("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO delivery_jobs(id,payload,available_at,created_at) SELECT printf('id-%d',x),'{}',0,0 FROM n;").unwrap();
        {
            let tx = c.transaction().unwrap();
            assert!(matches!(
                enqueue(&tx, "new", "{}", 0),
                Err(EventError::Busy)
            ));
        }
        c.execute(
            "UPDATE delivery_jobs SET state='delivered' WHERE id='id-1'",
            [],
        )
        .unwrap();
        let tx = c.transaction().unwrap();
        assert!(enqueue(&tx, "new", "{}", 0).unwrap());
        tx.commit().unwrap();
        assert_eq!(summary(&c).unwrap().pending, CAPACITY);
    }
    #[test]
    fn expired_leases_are_fenced_and_payload_identity_survives_retry() {
        let mut c = queue();
        let tx = c.transaction().unwrap();
        assert!(enqueue(&tx, "id", "payload", 0).unwrap());
        tx.commit().unwrap();
        let first = claim(&mut c, 0, ("outbox", "target")).unwrap().unwrap();
        assert!(claim(&mut c, 1, ("outbox", "target")).unwrap().is_none());
        let second = claim(&mut c, 130001, ("outbox", "target"))
            .unwrap()
            .unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(first.payload, second.payload);
        assert!(matches!(
            finish(&c, &first, 130002, true, false, "", None),
            Err(EventError::RevisionConflict)
        ));
        finish(&c, &second, 130002, true, false, "", None).unwrap();
        assert_eq!(summary(&c).unwrap().delivered, 1);
        let tx = c.transaction().unwrap();
        assert!(!enqueue(&tx, "id", "payload", 140000).unwrap());
        assert!(enqueue(&tx, "id", "changed", 140000).is_err());
    }
    #[test]
    fn exhausted_jobs_require_explicit_redrive_and_destination_is_pinned() {
        let mut c = queue();
        let tx = c.transaction().unwrap();
        enqueue(&tx, "id", "payload", 0).unwrap();
        tx.commit().unwrap();
        assert!(claim(&mut c, 0, ("outbox", "wrong-target")).is_err());
        for attempt in 0..MAX_ATTEMPTS {
            let job = claim(&mut c, attempt * 200000, ("outbox", "target"))
                .unwrap()
                .unwrap();
            finish(&c, &job, attempt * 200000, false, false, "timeout", Some(0)).unwrap();
        }
        assert!(
            claim(&mut c, 9999999, ("outbox", "target"))
                .unwrap()
                .is_none()
        );
        assert_eq!(summary(&c).unwrap().exhausted, 1);
        redrive(&c, "id").unwrap();
        assert!(
            claim(&mut c, 9999999, ("outbox", "target"))
                .unwrap()
                .is_some()
        );
    }
    #[test]
    fn abandoned_final_attempt_becomes_exhausted() {
        let mut c = queue();
        let tx = c.transaction().unwrap();
        enqueue(&tx, "id", "payload", 0).unwrap();
        tx.commit().unwrap();
        c.execute("UPDATE delivery_jobs SET attempts=7", [])
            .unwrap();
        claim(&mut c, 0, ("outbox", "target")).unwrap().unwrap();
        assert!(
            claim(&mut c, 130001, ("outbox", "target"))
                .unwrap()
                .is_none()
        );
        assert_eq!(summary(&c).unwrap().exhausted, 1);
    }
}
