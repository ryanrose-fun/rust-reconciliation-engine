//! Explicit, fixed partition layout for embedded ingestion. Not an HTTP router.
use super::{EventError, EventStore};
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};

/// One tenant per directory. Partition count is immutable after creation.
/// Event IDs must be globally stable for each payment; cross-partition event-ID
/// uniqueness is not enforced. No atomic transaction spans partitions.
pub struct PartitionedStore {
    tenant: String,
    stores: Vec<EventStore>,
}
impl PartitionedStore {
    pub fn open(
        root: &Path,
        tenant: &str,
        count: usize,
        wait: Duration,
    ) -> Result<Self, EventError> {
        super::validate_id(tenant, 128)?;
        if !(1..=64).contains(&count) {
            return Err(EventError::InvalidInput(
                "Partition count must be 1 through 64",
            ));
        }
        std::fs::create_dir_all(root).map_err(EventError::Io)?;
        let mut layout = rusqlite::Connection::open(root.join("layout.sqlite3"))?;
        layout.busy_timeout(Duration::from_secs(1))?;
        layout.pragma_update(None, "synchronous", "FULL")?;
        let tx = layout.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS partition_layout(singleton INTEGER PRIMARY KEY CHECK(singleton=1),tenant TEXT NOT NULL,count INTEGER NOT NULL,algorithm TEXT NOT NULL)")?;
        tx.execute(
            "INSERT OR IGNORE INTO partition_layout VALUES(1,?1,?2,'sha256-length-prefix-v1')",
            rusqlite::params![tenant, count as i64],
        )?;
        let stored: (String, i64, String) = tx.query_row(
            "SELECT tenant,count,algorithm FROM partition_layout WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if stored
            != (
                tenant.into(),
                count as i64,
                "sha256-length-prefix-v1".into(),
            )
        {
            return Err(EventError::InvalidInput(
                "Partition layout differs; explicit migration required",
            ));
        }
        tx.commit()?;
        let stores = (0..count)
            .map(|i| EventStore::open(root.join(format!("partition-{i:02}.sqlite3")), wait))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            tenant: tenant.into(),
            stores,
        })
    }
    pub fn partition_index(&self, payment_id: &str) -> usize {
        partition_index(&self.tenant, payment_id, self.stores.len())
    }
    pub fn into_stores(self) -> Vec<EventStore> {
        self.stores
    }

    /// Callers must send batches containing only this partition's payments.
    /// Independent partitions may be processed concurrently by bounded workers.
    pub fn for_payment(&self, payment_id: &str) -> &EventStore {
        &self.stores[self.partition_index(payment_id)]
    }
}
/// Stable routing for an already validated layout (count must be nonzero).
pub fn partition_index(tenant: &str, payment_id: &str, count: usize) -> usize {
    let mut hash = Sha256::new();
    hash.update((tenant.len() as u64).to_be_bytes());
    hash.update(tenant.as_bytes());
    hash.update(payment_id.as_bytes());
    let bytes: [u8; 8] = hash.finalize()[..8].try_into().expect("SHA256 length");
    (u64::from_be_bytes(bytes) % count as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn layout_survives_restart_and_rejects_remapping() {
        let root = std::env::temp_dir().join(format!("partitions-{}", uuid::Uuid::new_v4()));
        let wait = Duration::from_secs(30);
        let store = PartitionedStore::open(&root, "tenant", 4, wait).unwrap();
        let indices: Vec<_> = (0..100)
            .map(|i| store.partition_index(&format!("p{i}")))
            .collect();
        drop(store);
        let store = PartitionedStore::open(&root, "tenant", 4, wait).unwrap();
        assert_eq!(
            indices,
            (0..100)
                .map(|i| store.partition_index(&format!("p{i}")))
                .collect::<Vec<_>>()
        );
        assert!(PartitionedStore::open(&root, "tenant", 8, wait).is_err());
        assert!(PartitionedStore::open(&root, "another", 4, wait).is_err());
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
