use crate::database::Database;
use crate::snapshot::get_atomic_snapshot;
use crate::types::Snapshot;
use std::io::Error;

const DELETED_MARKER: &[u8] = b"__deleted__";

pub struct Transaction {
    db: Database,
    // Snapshot timestamp used for MVCC.
    snapshot: Snapshot,
    // read_snapshot holds the oldest active snapshot when this transaction began.
    pub read_snapshot: Snapshot,
    ops: Vec<Vec<u8>>,
    // New: Combined counters for GC change tracking.
    pub new_counter: usize, // counts insertions and new version updates.
    pub old_counter: usize, // counts old version updates and deletes.
}

impl Transaction {
    /// Begins a new transaction from the given Database.
    pub async fn begin(db: crate::database::Database) -> Self {
        let snapshot = get_atomic_snapshot();
        // Optional: Await any async initialization if needed.
        let current_oldest = db.get_oldest_read_snapshot();
        let read_snapshot = if current_oldest == Snapshot::MAX { snapshot } else { current_oldest };
        db.register_transaction(snapshot);
        Self {
            db,
            snapshot,
            read_snapshot,
            ops: Vec::new(),
            new_counter: 0,
            old_counter: 0,
        }
    }

    /// Async: Buffers an insert operation.
    /// Uses self.select to check for an existing record.
    /// If the record already exists with the same value, the operation is ignored.
    pub async fn insert(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        // Validate key/value sizes.
        if key.len() > 1024 {
            return Err(Error::new(std::io::ErrorKind::InvalidInput, "Key length exceeds 1k"));
        }
        if value.len() > 256 * 1024 {
            return Err(Error::new(std::io::ErrorKind::InvalidInput, "Value length exceeds 256k"));
        }
        // Check for an existing record.
        if let Some(existing) = self.select(key).await {
            if existing == value {
                return Ok(());
            }
        }
        // Build mod_key: key:snapshot
        let mut mod_key = key.to_vec();
        mod_key.extend_from_slice(b":");
        mod_key.extend_from_slice(self.snapshot.to_string().as_bytes());
        self.db.engine.set(&mod_key, value).await?;
        self.ops.push(mod_key);
        // Count as new data.
        self.new_counter += 1;
        Ok(())
    }

    /// Buffers an update operation by first fetching the latest data.
    /// If no data exists, the update is ignored.
    pub async fn update(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        if self.select(key).await.is_some() {
            let mut mod_key = key.to_vec();
            mod_key.extend_from_slice(b":");
            mod_key.extend_from_slice(self.snapshot.to_string().as_bytes());
            self.db.engine.set(&mod_key, value).await?;
            self.ops.push(mod_key);
            // For update: count one new version and one old version.
            self.new_counter += 1;
            self.old_counter += 1;
        }
        Ok(())
    }

    /// Buffers a delete operation.
    /// If the key doesn't exist (no record in the snapshot), the delete is ignored.
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        if self.select(key).await.is_some() {
            let mut mod_key = key.to_vec();
            mod_key.extend_from_slice(b":");
            mod_key.extend_from_slice(self.snapshot.to_string().as_bytes());
            self.db.engine.set(&mod_key, DELETED_MARKER.to_vec()).await?;
            self.ops.push(mod_key);
            // Count delete as one old version replaced.
            self.old_counter += 1;
        }
        Ok(())
    }

    /// Reads a value by checking buffered operations first, then performing a reverse scan.
    /// For each candidate, if its corresponding txn_marker key (formed as "txn_marker:<snapshot>")
    /// shows "commit", the candidate is used. If the txn_marker is "rollback" or missing, the candidate is deleted and the scan continues.
    pub async fn select(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Build range: lower bound "key:0", upper bound "key:(read_snapshot+1)"
        let mut lower = key.to_vec();
        lower.extend_from_slice(b":0");
        let mut upper = key.to_vec();
        let upper_bound = self.read_snapshot + 1;
        upper.extend_from_slice(upper_bound.to_string().as_bytes());
        let mut rev_iter = self.db.engine.reverse_scan(lower..upper).await.ok()?;
        while let Some((candidate_key, candidate_value)) = rev_iter.next() {
            if let Some(pos) = candidate_key.iter().rposition(|&b| b == b':') {
                let snapshot_bytes = &candidate_key[pos + 1..];
                if let Ok(snapshot_str) = std::str::from_utf8(snapshot_bytes) {
                    if let Ok(snapshot) = snapshot_str.parse::<u64>() {
                        let txn_marker_key = format!("txn_marker:{}", snapshot);
                        if let Some(marker_value) = self.db.engine.get(txn_marker_key.as_bytes()).await {
                            if marker_value == b"commit" {
                                return if candidate_value == DELETED_MARKER {
                                    None
                                } else {
                                    Some(candidate_value)
                                };
                            } else {
                                let _ = self.db.engine.del(&candidate_key).await;
                                continue;
                            }
                        } else {
                            let _ = self.db.engine.del(&candidate_key).await;
                            continue;
                        }
                    }
                }
            }
            let _ = self.db.engine.del(&candidate_key).await;
        }
        None
    }

    /// Commits the buffered operations and writes a commit marker.
    pub async fn commit(self) -> Result<(), Error> {
        // New: Push the transaction's change counters to TransactionManager.
        self.db.push_counters(self.new_counter, self.old_counter);
        // Write the marker in the key; value is "commit".
        let marker_key = format!("txn_marker:{}", self.snapshot);
        self.db.engine.set(marker_key.as_bytes(), b"commit".to_vec()).await?;
        self.db.unregister_transaction(self.snapshot);
        Ok(())
    }

    /// Asynchronously rolls back the transaction by writing a rollback marker and clearing buffered operations.
    pub async fn rollback(&mut self) -> Result<(), Error> {
        for mk in &self.ops {
            self.db.engine.del(mk).await?;
        }
        // Write the rollback marker with snapshot in key; value is "rollback".
        let marker_key = format!("txn_marker:{}", self.snapshot);
        self.db.engine.set(marker_key.as_bytes(), b"rollback".to_vec()).await?;
        self.db.unregister_transaction(self.snapshot);
        Ok(())
    }
}
