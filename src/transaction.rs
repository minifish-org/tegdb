use crate::constants::TXN_MARKER_PREFIX;
use crate::database::Database;
use crate::snapshot::get_atomic_snapshot;
use crate::types::Snapshot;
use std::io::Error;

const TXN_MARKER_COMMIT: &[u8] = b"commit";
const TXN_MARKER_ROLLBACK: &[u8] = b"rollback";
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
    /// Helper function to create a snapshot key in the format "key:snapshot"
    fn make_snapshot_key(key: &[u8], snapshot: Snapshot) -> Vec<u8> {
        let mut snapshot_key = key.to_vec();
        snapshot_key.extend_from_slice(b":");
        snapshot_key.extend_from_slice(snapshot.to_string().as_bytes());
        snapshot_key
    }

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

    /// Buffers an insert operation after verifying key/value sizes.  
    /// If the key/value exceeds allowed limits (MAX_KEY_SIZE, MAX_VALUE_SIZE), returns an error.
    pub async fn insert(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        // Check for an existing record.
        if let Some(existing) = self.select(key).await {
            if existing == value {
                return Ok(());
            }
        }
        let mod_key = Self::make_snapshot_key(key, self.snapshot);
        self.db.engine.set(&mod_key, value).await?;
        self.ops.push(mod_key);
        // Count as new data.
        self.new_counter += 1;
        Ok(())
    }

    /// Buffers an update operation if the key exists.
    /// If no data exists, the update is ignored.
    pub async fn update(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        if self.select(key).await.is_some() {
            let mod_key = Self::make_snapshot_key(key, self.snapshot);
            self.db.engine.set(&mod_key, value).await?;
            self.ops.push(mod_key);
            // For update: count one new version and one old version.
            self.new_counter += 1;
            self.old_counter += 1;
        }
        Ok(())
    }

    /// Buffers a delete operation if the key exists.
    /// If the key doesn't exist (no record in the snapshot), the delete is ignored.
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        if self.select(key).await.is_some() {
            let mod_key = Self::make_snapshot_key(key, self.snapshot);
            self.db.engine.set(&mod_key, DELETED_MARKER.to_vec()).await?;
            self.ops.push(mod_key);
            // Count delete as one old version replaced.
            self.old_counter += 1;
        }
        Ok(())
    }

    /// Reads the latest value for a given key using buffered operations and a reverse scan.
    /// For each candidate, if its corresponding txn_marker key (formed as "txn_marker:<snapshot>")
    /// shows "commit", the candidate is used. If the txn_marker is "rollback" or missing, the candidate is deleted and the scan continues.
    pub async fn select(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Check current transaction changes first.
        let current_txn_key = Self::make_snapshot_key(key, self.snapshot);
        if let Some(value) = self.db.engine.get(&current_txn_key).await {
            if value == DELETED_MARKER {
                return None;
            } else {
                return Some(value);
            }
        }

        // Build range: lower bound "key:0", upper bound "key:(read_snapshot+1)"
        let lower = Self::make_snapshot_key(key, 0);
        let upper = Self::make_snapshot_key(key, self.read_snapshot + 1);
        let mut rev_iter = self.db.engine.reverse_scan(lower..upper).await.ok()?;
        while let Some((candidate_key, candidate_value)) = rev_iter.next() {
            if let Some(pos) = candidate_key.iter().rposition(|&b| b == b':') {
                let snapshot_bytes = &candidate_key[pos + 1..];
                if let Ok(snapshot_str) = std::str::from_utf8(snapshot_bytes) {
                    if let Ok(snapshot) = snapshot_str.parse::<u64>() {
                        let txn_marker_key = format!("{}{}", TXN_MARKER_PREFIX, snapshot);
                        if let Some(marker_value) = self.db.engine.get(txn_marker_key.as_bytes()).await {
                            if marker_value == TXN_MARKER_COMMIT {
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

    /// Commits the buffered operations and, if any ops are present, writes a commit marker.
    pub async fn commit(self) -> Result<(), Error> {
        if !self.ops.is_empty() {
            self.db.push_counters(self.new_counter, self.old_counter);
            let marker_key = format!("{}{}", TXN_MARKER_PREFIX, self.snapshot);
            self.db.engine.set(marker_key.as_bytes(), TXN_MARKER_COMMIT.to_vec()).await?;
        }
        self.db.unregister_transaction(self.snapshot);
        Ok(())
    }

    /// Rolls back the transaction. If ops exist, deletes them and writes a rollback marker.
    pub async fn rollback(&mut self) -> Result<(), Error> {
        if !self.ops.is_empty() {
            for mk in &self.ops {
                self.db.engine.del(mk).await?;
            }
            let marker_key = format!("{}{}", TXN_MARKER_PREFIX, self.snapshot);
            self.db.engine.set(marker_key.as_bytes(), TXN_MARKER_ROLLBACK.to_vec()).await?;
        }
        self.db.unregister_transaction(self.snapshot);
        Ok(())
    }
}
