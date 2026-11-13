use super::*;

pub(super) fn migrate(tx: &rusqlite::Transaction<'_>) -> Result<(), EventError> {
    tx.execute_batch("CREATE TABLE case_state(payment_id TEXT PRIMARY KEY REFERENCES payments(payment_id), status TEXT NOT NULL, signature TEXT NOT NULL, revision INTEGER NOT NULL);
        CREATE TABLE payment_history(sequence INTEGER PRIMARY KEY AUTOINCREMENT, payment_id TEXT NOT NULL REFERENCES payments(payment_id), kind TEXT NOT NULL, recorded_at_ms INTEGER NOT NULL, effective_at_ms INTEGER NOT NULL, from_status TEXT, status TEXT NOT NULL, evidence TEXT NOT NULL, actor TEXT, note TEXT, revision INTEGER NOT NULL, note_id TEXT UNIQUE, request TEXT);
        CREATE INDEX history_payment_sequence ON payment_history(payment_id,sequence);
        PRAGMA user_version=2;")?;
    Ok(())
}
fn is_issue(status: &str) -> bool {
    !matches!(status, "Matched" | "AwaitingInternal" | "AwaitingProvider")
}
/// Record a transition in the same transaction as its triggering event. Deadline
/// transitions are observed lazily, but carry the actual waiting deadline.
pub(super) fn capture(
    tx: &rusqlite::Transaction<'_>,
    id: &str,
    now: i64,
) -> Result<LivePayment, EventError> {
    let current = read_payment(tx, id, now)?;
    let status = serde_json::to_value(&current.status)
        .map_err(|_| EventError::CorruptData)?
        .as_str()
        .ok_or(EventError::CorruptData)?
        .to_owned();
    let signature=serde_json::json!({"status":status,"internal":current.internal.as_ref().map(|v|&v.payment),"provider":current.provider.as_ref().map(|v|&v.payment)}).to_string();
    let old: Option<(String, String)> = tx
        .query_row(
            "SELECT status,signature FROM case_state WHERE payment_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if old.as_ref().is_none_or(|(_, s)| s != &signature) {
        let prior = old.as_ref().map(|(s, _)| s.as_str());
        let kind = match prior {
            None => "Baseline",
            Some(s) if !is_issue(s) && is_issue(&status) => "Opened",
            Some(s) if is_issue(s) && status == "Matched" => "Resolved",
            Some(s) if is_issue(s) && is_issue(&status) => "Changed",
            _ => "Observed",
        };
        let effective = if matches!(
            (prior, status.as_str()),
            (Some("AwaitingInternal"), "MissingFromInternal")
                | (Some("AwaitingProvider"), "MissingFromProvider")
        ) {
            current.waiting_until_ms
        } else {
            now
        };
        tx.execute("INSERT INTO payment_history(payment_id,kind,recorded_at_ms,effective_at_ms,from_status,status,evidence,revision) VALUES(?1,?2,?3,?4,?5,?6,?7,0)",params![id,kind,now,effective,prior,status,serde_json::to_string(&current).map_err(|_|EventError::CorruptData)?])?;
        let revision = tx.last_insert_rowid();
        tx.execute(
            "UPDATE payment_history SET revision=?1 WHERE sequence=?1",
            [revision],
        )?;
        tx.execute("INSERT INTO case_state(payment_id,status,signature,revision) VALUES(?1,?2,?3,?4) ON CONFLICT(payment_id) DO UPDATE SET status=excluded.status,signature=excluded.signature,revision=excluded.revision",params![id,status,signature,revision])?;
    }
    Ok(current)
}
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct HistoryEntry {
    pub sequence: i64,
    pub kind: String,
    pub recorded_at_ms: i64,
    pub effective_at_ms: i64,
    pub from_status: Option<String>,
    pub status: String,
    pub evidence: serde_json::Value,
    pub actor: Option<String>,
    pub note: Option<String>,
    pub revision: i64,
}
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct HistoryPage {
    pub entries: Vec<HistoryEntry>,
    pub next_after: Option<i64>,
    pub current_revision: i64,
    pub current_status: String,
    pub acknowledged: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub enum InvestigationAction {
    Note,
    Acknowledge,
}
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct Investigation {
    pub id: String,
    pub action: InvestigationAction,
    pub note: String,
    pub expected_revision: i64,
}
#[derive(Serialize, utoipa::ToSchema)]
pub struct InvestigationReceipt {
    pub sequence: i64,
    pub duplicate: bool,
    pub revision: i64,
}
impl EventStore {
    pub fn history(&self, id: &str, after: i64, limit: usize) -> Result<HistoryPage, EventError> {
        validate_id(id, 256)?;
        if after < 0 || !(1..=100).contains(&limit) {
            return Err(EventError::InvalidInput(
                "History cursor must be nonnegative and limit 1..100",
            ));
        }
        let mut c = self.lock()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        capture(&tx, id, chrono::Utc::now().timestamp_millis())?;
        let (revision, status): (i64, String) = tx.query_row(
            "SELECT revision,status FROM case_state WHERE payment_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let acknowledged:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM payment_history WHERE payment_id=?1 AND revision=?2 AND kind='Acknowledged')",params![id,revision],|r|r.get(0))?;
        let rows = {
            let mut q=tx.prepare("SELECT sequence,kind,recorded_at_ms,effective_at_ms,from_status,status,evidence,actor,note,revision FROM payment_history WHERE payment_id=?1 AND sequence>?2 ORDER BY sequence LIMIT ?3")?;
            q.query_map(params![id, after, limit as i64 + 1], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, Option<String>>(7)?,
                    r.get::<_, Option<String>>(8)?,
                    r.get::<_, i64>(9)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        let more = rows.len() > limit;
        let entries = rows
            .into_iter()
            .take(limit)
            .map(
                |(
                    sequence,
                    kind,
                    recorded_at_ms,
                    effective_at_ms,
                    from_status,
                    status,
                    evidence,
                    actor,
                    note,
                    revision,
                )| {
                    Ok(HistoryEntry {
                        sequence,
                        kind,
                        recorded_at_ms,
                        effective_at_ms,
                        from_status,
                        status,
                        evidence: serde_json::from_str(&evidence)
                            .map_err(|_| EventError::CorruptData)?,
                        actor,
                        note,
                        revision,
                    })
                },
            )
            .collect::<Result<Vec<_>, EventError>>()?;
        let next_after = if more {
            entries.last().map(|v| v.sequence)
        } else {
            None
        };
        tx.commit()?;
        Ok(HistoryPage {
            entries,
            next_after,
            current_revision: revision,
            current_status: status,
            acknowledged,
        })
    }
    pub fn investigate(
        &self,
        id: &str,
        actor: &str,
        input: Investigation,
    ) -> Result<InvestigationReceipt, EventError> {
        validate_id(id, 256)?;
        validate_id(actor, 64)?;
        validate_id(&input.id, 128)?;
        if input.note.trim().is_empty() || input.note.len() > 2000 || input.expected_revision <= 0 {
            return Err(EventError::InvalidInput(
                "Investigation requires a note of 1..2000 bytes and a positive revision",
            ));
        }
        let request = serde_json::json!({"payment_id":id,"actor":actor,"input":input}).to_string();
        let mut c = self.lock()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(i64, i64, String)> = tx
            .query_row(
                "SELECT sequence,revision,request FROM payment_history WHERE note_id=?1",
                [&input.id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((sequence, revision, old)) = existing {
            if old != request {
                return Err(EventError::RevisionConflict);
            }
            return Ok(InvestigationReceipt {
                sequence,
                revision,
                duplicate: true,
            });
        }
        let now = chrono::Utc::now().timestamp_millis();
        let payment = capture(&tx, id, now)?;
        let (revision, status): (i64, String) = tx.query_row(
            "SELECT revision,status FROM case_state WHERE payment_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if revision != input.expected_revision
            || matches!(input.action, InvestigationAction::Acknowledge) && !is_issue(&status)
        {
            return Err(EventError::RevisionConflict);
        }
        let kind = match input.action {
            InvestigationAction::Note => "Note",
            InvestigationAction::Acknowledge => "Acknowledged",
        };
        tx.execute("INSERT INTO payment_history(payment_id,kind,recorded_at_ms,effective_at_ms,status,evidence,actor,note,revision,note_id,request) VALUES(?1,?2,?3,?3,?4,?5,?6,?7,?8,?9,?10)",params![id,kind,now,status,serde_json::to_string(&payment).map_err(|_|EventError::CorruptData)?,actor,input.note,revision,input.id,request])?;
        let sequence = tx.last_insert_rowid();
        tx.commit()?;
        Ok(InvestigationReceipt {
            sequence,
            revision,
            duplicate: false,
        })
    }
}
