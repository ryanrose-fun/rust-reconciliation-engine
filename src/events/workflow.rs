use super::*;
pub(super) fn migrate(tx: &rusqlite::Transaction<'_>) -> Result<(), EventError> {
    tx.execute_batch("ALTER TABLE case_state ADD COLUMN opened_at INTEGER;
        UPDATE case_state SET opened_at=(SELECT recorded_at_ms FROM payment_history WHERE sequence=revision) WHERE status NOT IN ('Matched','AwaitingInternal','AwaitingProvider');
        CREATE INDEX due_payments ON payments(waiting_until_ms,payment_id);
        CREATE TABLE pending_deadlines(payment_id TEXT PRIMARY KEY REFERENCES payments(payment_id),deadline INTEGER NOT NULL);
        CREATE INDEX pending_due ON pending_deadlines(deadline,payment_id);
        INSERT INTO pending_deadlines SELECT p.payment_id,p.waiting_until_ms FROM payments p JOIN source_state s USING(payment_id) GROUP BY p.payment_id HAVING count(*)=1;
        CREATE INDEX case_status ON case_state(status,payment_id);
        CREATE TABLE case_workflow(payment_id TEXT PRIMARY KEY REFERENCES payments(payment_id),owner TEXT,workflow TEXT NOT NULL,version INTEGER NOT NULL);
        CREATE TABLE notification_settings(singleton INTEGER PRIMARY KEY CHECK(singleton=1),endpoint TEXT NOT NULL);
        PRAGMA user_version=3;")?;
    crate::delivery::schema(tx)?;
    Ok(())
}
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub enum WorkflowStatus {
    Open,
    InProgress,
    Waiting,
}
#[derive(Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CaseUpdate {
    pub id: String,
    pub expected_revision: i64,
    pub expected_case_version: i64,
    pub owner: Option<String>,
    pub workflow: WorkflowStatus,
    pub note: String,
}
#[derive(Default, Deserialize, utoipa::ToSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CaseQuery {
    pub after: Option<String>,
    pub limit: Option<usize>,
    pub owner: Option<String>,
    pub workflow: Option<WorkflowStatus>,
    pub acknowledged: Option<bool>,
    pub min_age_ms: Option<u64>,
}
#[derive(Serialize, utoipa::ToSchema)]
pub struct CaseEntry {
    pub payment_id: String,
    pub status: String,
    pub revision: i64,
    pub opened_at_ms: i64,
    pub owner: Option<String>,
    pub workflow: String,
    pub case_version: i64,
    pub acknowledged: bool,
}
#[derive(Serialize, utoipa::ToSchema)]
pub struct CasePage {
    pub entries: Vec<CaseEntry>,
    pub next_after: Option<String>,
}
impl EventStore {
    /// Bounded deadline work; no network calls or unbounded database scan per tick.
    pub fn process_deadlines(&self, limit: usize) -> Result<usize, EventError> {
        self.process_deadlines_at(chrono::Utc::now().timestamp_millis(), limit)
    }
    pub(super) fn process_deadlines_at(&self, now: i64, limit: usize) -> Result<usize, EventError> {
        if !(1..=100).contains(&limit) {
            return Err(EventError::InvalidInput("Deadline batch must be 1..100"));
        }
        let mut c = self.lock()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ids = {
            let mut q=tx.prepare("SELECT payment_id FROM pending_deadlines WHERE deadline<=?1 ORDER BY deadline,payment_id LIMIT ?2")?;
            q.query_map(params![now, limit as i64], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        for id in &ids {
            history::capture(&tx, id, now)?;
        }
        tx.commit()?;
        Ok(ids.len())
    }
    pub fn cases(&self, query: CaseQuery) -> Result<CasePage, EventError> {
        let limit = query.limit.unwrap_or(50);
        if !(1..=100).contains(&limit) || query.min_age_ms.is_some_and(|v| v > i64::MAX as u64) {
            return Err(EventError::InvalidInput("Invalid case page limits"));
        }
        let cutoff = chrono::Utc::now()
            .timestamp_millis()
            .saturating_sub(query.min_age_ms.unwrap_or(0) as i64);
        let workflow = query.workflow.map(|w| {
            serde_json::to_value(w)
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned()
        });
        let c = self.lock()?;
        let mut q=c.prepare("SELECT c.payment_id,c.status,c.revision,c.opened_at,w.owner,coalesce(w.workflow,'Open'),coalesce(w.version,0),EXISTS(SELECT 1 FROM payment_history h WHERE h.payment_id=c.payment_id AND h.revision=c.revision AND h.kind='Acknowledged') AS ack FROM case_state c LEFT JOIN case_workflow w USING(payment_id) WHERE c.payment_id>?1 AND c.opened_at IS NOT NULL AND c.opened_at<=?2 AND (?3 IS NULL OR w.owner=?3) AND (?4 IS NULL OR coalesce(w.workflow,'Open')=?4) AND (?5 IS NULL OR ack=?5) ORDER BY c.payment_id LIMIT ?6")?;
        let mut entries = q
            .query_map(
                params![
                    query.after.unwrap_or_default(),
                    cutoff,
                    query.owner,
                    workflow,
                    query.acknowledged,
                    limit as i64 + 1
                ],
                |r| {
                    Ok(CaseEntry {
                        payment_id: r.get(0)?,
                        status: r.get(1)?,
                        revision: r.get(2)?,
                        opened_at_ms: r.get(3)?,
                        owner: r.get(4)?,
                        workflow: r.get(5)?,
                        case_version: r.get(6)?,
                        acknowledged: r.get(7)?,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        let more = entries.len() > limit;
        entries.truncate(limit);
        let next_after = if more {
            entries.last().map(|e| e.payment_id.clone())
        } else {
            None
        };
        Ok(CasePage {
            entries,
            next_after,
        })
    }
    pub fn update_case(
        &self,
        id: &str,
        actor: &str,
        input: CaseUpdate,
    ) -> Result<history::InvestigationReceipt, EventError> {
        validate_id(id, 256)?;
        validate_id(actor, 64)?;
        validate_id(&input.id, 128)?;
        if let Some(owner) = &input.owner {
            validate_id(owner, 64)?;
        }
        if input.note.trim().is_empty()
            || input.note.len() > 2000
            || input.expected_case_version < 0
        {
            return Err(EventError::InvalidInput("Invalid case note or version"));
        }
        let canonical =
            serde_json::json!({"payment_id":id,"actor":actor,"case_update":input}).to_string();
        let mut c = self.lock()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old: Option<(i64, i64, String)> = tx
            .query_row(
                "SELECT sequence,revision,request FROM payment_history WHERE note_id=?1",
                [&input.id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((sequence, revision, request)) = old {
            if request != canonical {
                return Err(EventError::RevisionConflict);
            }
            return Ok(history::InvestigationReceipt {
                sequence,
                revision,
                duplicate: true,
            });
        }
        let now = chrono::Utc::now().timestamp_millis();
        let evidence = history::capture(&tx, id, now)?;
        let (revision,status,version):(i64,String,i64)=tx.query_row("SELECT c.revision,c.status,coalesce(w.version,0) FROM case_state c LEFT JOIN case_workflow w USING(payment_id) WHERE c.payment_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        if revision != input.expected_revision
            || version != input.expected_case_version
            || matches!(
                status.as_str(),
                "Matched" | "AwaitingInternal" | "AwaitingProvider"
            )
        {
            return Err(EventError::RevisionConflict);
        }
        let workflow = serde_json::to_value(&input.workflow)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        tx.execute("INSERT INTO case_workflow(payment_id,owner,workflow,version) VALUES(?1,?2,?3,?4) ON CONFLICT(payment_id) DO UPDATE SET owner=excluded.owner,workflow=excluded.workflow,version=excluded.version",params![id,input.owner,workflow,version+1])?;
        tx.execute("INSERT INTO payment_history(payment_id,kind,recorded_at_ms,effective_at_ms,status,evidence,actor,note,revision,note_id,request) VALUES(?1,'WorkflowChanged',?2,?2,?3,?4,?5,?6,?7,?8,?9)",params![id,now,status,serde_json::to_string(&serde_json::json!({"payment":evidence,"owner":input.owner,"workflow":input.workflow,"case_version":version+1})).unwrap(),actor,input.note,revision,input.id,canonical])?;
        let sequence = tx.last_insert_rowid();
        tx.commit()?;
        Ok(history::InvestigationReceipt {
            sequence,
            revision,
            duplicate: false,
        })
    }
}
