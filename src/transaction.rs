use crate::database::Database;
use crate::snapshot::get_atomic_snapshot;
use crate::types::Snapshot;
use std::io::{Error, ErrorKind};
use crate::utils::make_marker_key;

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
    // New: List of acquired locks.
    locks: Vec<Vec<u8>>,
    // New: Transaction status flag to mark if it should be aborted.
    pub should_abort: bool,
}

impl Transaction {
    /// Creates a snapshot key by appending the separator and the snapshot value to the given key.
    fn make_snapshot_key(key: &[u8], snapshot: Snapshot) -> Vec<u8> {
        let mut snapshot_key = key.to_vec();
        snapshot_key.push(crate::constants::KEY_SEPARATOR);
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
            locks: Vec::new(),
            should_abort: false, // Initialize the abort status
        }
    }

    // Updated: Acquire a lock via the TransactionManager.
    pub async fn acquire_lock(&mut self, key: &[u8]) -> Result<(), Error> {
        // Skip reacquisition if already held.
        if self.locks.iter().any(|l| l == key) {
            return Ok(());
        }
        let key_vec = key.to_vec();
        if let Err(e) = self.db.transaction_manager.acquire_lock(key_vec.clone(), self.snapshot).await {
            self.mark_abort();
            return Err(e);
        }
        self.locks.push(key_vec);
        Ok(())
    }

    // New: Check write conflict: If the selected key's snapshot is greater than the txn snapshot, release the lock and return error.
    pub async fn check_write_conflict(&mut self, key: &[u8]) -> Result<(), Error> {
        let (_, candidate_snap) = self.select(key).await?;
        if candidate_snap > self.snapshot {
            let key_vec = key.to_vec();
            self.db.transaction_manager.release_lock(key_vec.clone(), self.snapshot).await;
            self.locks.retain(|l| l != &key_vec);
            return Err(Error::new(ErrorKind::Other, "Write conflict error"));
        }
        Ok(())
    }

    // Updated: Release all acquired locks.
    async fn release_locks(&mut self) {
        for lock in self.locks.drain(..) {
            self.db.transaction_manager.release_lock(lock, self.snapshot).await;
        }
    }

    /// Buffers an insert operation after verifying key/value sizes.  
    /// If the key/value exceeds allowed limits (MAX_KEY_SIZE, MAX_VALUE_SIZE), returns an error.
    pub async fn insert(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        if self.should_abort {
            return Err(Error::new(ErrorKind::Other, "Transaction aborted"));
        }
        // Acquire lock for the key.
        self.acquire_lock(key).await?;
        // Check for write conflict.
        self.check_write_conflict(key).await?;

        // Check for an existing record.
        if let Some(existing) = self.select(key).await?.0 {
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
        if self.should_abort {
            return Err(Error::new(ErrorKind::Other, "Transaction aborted"));
        }
        // Acquire lock for the key.
        self.acquire_lock(key).await?;
        // Check for write conflict.
        self.check_write_conflict(key).await?;

        if self.select(key).await?.0.is_some() {
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
        if self.should_abort {
            return Err(Error::new(ErrorKind::Other, "Transaction aborted"));
        }
        // Acquire lock for the key.
        self.acquire_lock(key).await?;
        // Check for write conflict.
        self.check_write_conflict(key).await?;

        if self.select(key).await?.0.is_some() {
            let mod_key = Self::make_snapshot_key(key, self.snapshot);
            self.db.engine.set(&mod_key, DELETED_MARKER.to_vec()).await?;
            self.ops.push(mod_key);
            // Count delete as one old version replaced.
            self.old_counter += 1;
        }
        Ok(())
    }

    /// Reads the latest value for a given key using buffered operations and a reverse scan.
    /// Returns a tuple with an optional value and a snapshot:
    /// - If a deleted record is found, returns (None, snapshot) using its snapshot.
    /// - Otherwise, if a value is found, returns (Some(value), 0).
    /// - If no valid candidate is found (line 180), returns (None, 0).
    pub async fn select(&self, key: &[u8]) -> Result<(Option<Vec<u8>>, u64), Error> {
        if self.should_abort {
            return Err(Error::new(ErrorKind::Other, "Transaction aborted"));
        }
        // Check current transaction changes first.
        let current_txn_key = Self::make_snapshot_key(key, self.snapshot);
        if let Some(value) = self.db.engine.get(&current_txn_key).await {
            if value == DELETED_MARKER {
                return Ok((None, self.snapshot));
            } else {
                // Updated: Return normal value with self.snapshot.
                return Ok((Some(value), self.snapshot));
            }
        }

        // Build range: lower bound "key:0", upper bound "key:(read_snapshot+1)"
        let lower = Self::make_snapshot_key(key, 0);
        let upper = Self::make_snapshot_key(key, self.read_snapshot + 1);
        let mut rev_iter = self.db.engine.reverse_scan(lower..upper).await?;
        while let Some((candidate_key, candidate_value)) = rev_iter.next() {
            if let Some(pos) = candidate_key.iter().rposition(|&b| b == crate::constants::KEY_SEPARATOR) {
                let snapshot_bytes = &candidate_key[pos + 1..];
                if let Ok(snapshot_str) = std::str::from_utf8(snapshot_bytes) {
                    if let Ok(candidate_snapshot) = snapshot_str.parse::<u64>() {
                        let txn_marker_key = make_marker_key(candidate_snapshot);
                        if let Some(marker_value) = self.db.engine.get(txn_marker_key.as_bytes()).await {
                            if marker_value == TXN_MARKER_COMMIT {
                                if candidate_value == DELETED_MARKER {
                                    return Ok((None, candidate_snapshot));
                                } else {
                                    // Updated: Return normal value with candidate_snapshot.
                                    return Ok((Some(candidate_value), candidate_snapshot));
                                }
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
        Ok((None, 0))
    }

    /// Commits the buffered operations and, if any ops are present, writes a commit marker.
    pub async fn commit(mut self) -> Result<(), Error> {
        if self.should_abort {
            // Rollback first, then return error.
            self.rollback().await?;
            return Err(Error::new(ErrorKind::Other, "Transaction aborted; commit not allowed, already rolled back"));
        }
        if !self.ops.is_empty() {
            self.db.push_counters(self.new_counter, self.old_counter);
            let marker_key = make_marker_key(self.snapshot);
            self.db.engine.set(marker_key.as_bytes(), TXN_MARKER_COMMIT.to_vec()).await?;
        }
        self.release_locks().await;
        self.db.unregister_transaction(self.snapshot);
        Ok(())
    }

    /// Rolls back the transaction. If ops exist, deletes them and writes a rollback marker.
    pub async fn rollback(&mut self) -> Result<(), Error> {
        if !self.ops.is_empty() {
            for mk in &self.ops {
                self.db.engine.del(mk).await?;
            }
            let marker_key = make_marker_key(self.snapshot);
            self.db.engine.set(marker_key.as_bytes(), TXN_MARKER_ROLLBACK.to_vec()).await?;
        }
        self.release_locks().await;
        self.db.unregister_transaction(self.snapshot);
        Ok(())
    }

    // New: Mark the transaction to be aborted.
    pub fn mark_abort(&mut self) {
        self.should_abort = true;
    }
}
