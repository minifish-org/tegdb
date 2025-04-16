use crate::database::Database;
use crate::snapshot::get_atomic_snapshot;
use crate::types::Snapshot;
use crate::utils::make_marker_key;
use std::io::{Error, ErrorKind};

const TXN_MARKER_COMMIT: &[u8] = b"commit";
const TXN_MARKER_ROLLBACK: &[u8] = b"rollback";
const DELETED_MARKER: &[u8] = b"__deleted__";

pub struct Transaction {
    db: Database,
    // Snapshot timestamp used for MVCC with Serializable isolation
    snapshot: Snapshot,
    // All active transaction snapshots for conflict detection
    pub active_transactions: Vec<Snapshot>,
    // List of operations performed in this transaction
    ops: Vec<Vec<u8>>,
    // Combined counters for GC change tracking
    pub new_counter: usize,
    pub old_counter: usize,
    // List of keys with write intents
    intent_keys: Vec<Vec<u8>>,
    // Transaction status flag
    pub should_abort: bool,
}

// This static assertion will fail to compile if Transaction doesn't implement Send
const _: () = {
    fn assert_send<T: Send>() {}
    fn check() {
        assert_send::<Transaction>();
    }
};

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
        let active_transactions = db.get_active_transactions();
        db.register_transaction(snapshot);
        Self {
            db,
            snapshot,
            active_transactions,
            ops: Vec::new(),
            new_counter: 0,
            old_counter: 0,
            intent_keys: Vec::new(),
            should_abort: false,
        }
    }

    /// Places a write intent on a key
    async fn place_intent(&mut self, key: &[u8]) -> Result<(), Error> {
        if self.should_abort {
            return Err(Error::new(ErrorKind::Other, "Transaction aborted"));
        }
        let key_vec = key.to_vec();
        if let Err(e) = self.db.transaction_manager.place_intent(key_vec.clone(), self.snapshot).await {
            self.mark_abort();
            return Err(e);
        }
        self.intent_keys.push(key_vec);
        Ok(())
    }

    /// Resolves all write intents for this transaction
    async fn resolve_intents(&mut self, commit: bool) {
        for key in self.intent_keys.drain(..) {
            self.db.transaction_manager.resolve_intent(&key, self.snapshot, commit);
        }
    }

    /// Inserts a key-value pair with write intent
    pub async fn insert(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        if self.should_abort {
            return Err(Error::new(ErrorKind::Other, "Transaction aborted"));
        }
        self.place_intent(key).await?;
        let (existing, _) = self.select(key).await?;
        if let Some(existing) = existing {
            if existing == value {
                return Ok(());
            }
        }
        let mod_key = Self::make_snapshot_key(key, self.snapshot);
        self.db.engine.set(&mod_key, value).await?;
        self.ops.push(mod_key);
        self.new_counter += 1;
        Ok(())
    }

    /// Updates a key-value pair with write intent
    pub async fn update(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        if self.should_abort {
            return Err(Error::new(ErrorKind::Other, "Transaction aborted"));
        }
        self.place_intent(key).await?;
        let (existing, _) = self.select(key).await?;
        if let Some(existing_value) = existing {
            if existing_value == value {
                return Ok(());
            }
            let mod_key = Self::make_snapshot_key(key, self.snapshot);
            self.db.engine.set(&mod_key, value).await?;
            self.ops.push(mod_key);
            self.new_counter += 1;
            self.old_counter += 1;
        }
        Ok(())
    }

    /// Deletes a key with write intent
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        if self.should_abort {
            return Err(Error::new(ErrorKind::Other, "Transaction aborted"));
        }
        self.place_intent(key).await?;
        let (existing, _) = self.select(key).await?;
        if existing.is_some() {
            let mod_key = Self::make_snapshot_key(key, self.snapshot);
            self.db.engine.set(&mod_key, DELETED_MARKER.to_vec()).await?;
            self.ops.push(mod_key);
            self.old_counter += 1;
        }
        Ok(())
    }

    /// Reads the latest value for a given key using buffered operations and a reverse scan.
    /// Returns a tuple with an optional value and a snapshot:
    /// - If a deleted record is found, returns (None, snapshot) using its snapshot.
    /// - Otherwise, if a value is found, returns (Some(value), 0).
    /// - If no valid candidate is found (line 180), returns (None, 0).
    pub async fn select(&mut self, key: &[u8]) -> Result<(Option<Vec<u8>>, u64), Error> {
        if self.should_abort {
            return Err(Error::new(ErrorKind::Other, "Transaction aborted"));
        }

        // Place an intent for read operation to ensure proper isolation
        self.place_intent(key).await?;

        // Reverse scan from snapshot 0 up to current transaction's snapshot.
        let lower = Self::make_snapshot_key(key, 0);
        let upper = Self::make_snapshot_key(key, self.snapshot + 1);
        // reverse_scan now returns a Vec
        let results = self.db.engine.reverse_scan(lower..upper).await?;
        for (candidate_key, candidate_value) in results { // Iterate over the Vec
            if let Some(pos) = candidate_key
                .iter()
                .rposition(|&b| b == crate::constants::KEY_SEPARATOR)
            {
                let snapshot_bytes = &candidate_key[pos + 1..];
                if let Ok(snapshot_str) = std::str::from_utf8(snapshot_bytes) {
                    if let Ok(candidate_snapshot) = snapshot_str.parse::<u64>() {
                        // Skip candidate keys with snapshots in active transactions.
                        if self.active_transactions.contains(&candidate_snapshot) {
                            continue;
                        }
                        // New: If candidate snapshot equals transaction snapshot, return its value directly.
                        if candidate_snapshot == self.snapshot {
                            if candidate_value == DELETED_MARKER {
                                return Ok((None, candidate_snapshot));
                            } else {
                                return Ok((Some(candidate_value), candidate_snapshot));
                            }
                        }
                        let txn_marker_key = make_marker_key(candidate_snapshot);
                        if let Some(marker_value) =
                            self.db.engine.get(txn_marker_key.as_bytes()).await
                        {
                            if marker_value == TXN_MARKER_COMMIT {
                                if candidate_value == DELETED_MARKER {
                                    return Ok((None, candidate_snapshot));
                                } else {
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

    /// Commits the transaction and resolves all write intents
    pub async fn commit(mut self) -> Result<(), Error> {
        if self.should_abort {
            self.rollback().await?;
            return Err(Error::new(ErrorKind::Other, "Transaction aborted"));
        }
        if !self.ops.is_empty() {
            self.db.push_counters(self.new_counter, self.old_counter);
            let marker_key = make_marker_key(self.snapshot);
            self.db.engine.set(marker_key.as_bytes(), TXN_MARKER_COMMIT.to_vec()).await?;
            self.db.engine.flush().expect("Failed to flush WAL");
        }
        self.resolve_intents(true).await;
        self.db.unregister_transaction(self.snapshot);
        Ok(())
    }

    /// Rolls back the transaction and resolves all write intents
    pub async fn rollback(&mut self) -> Result<(), Error> {
        if !self.ops.is_empty() {
            for mk in &self.ops {
                self.db.engine.del(mk).await?;
            }
            let marker_key = make_marker_key(self.snapshot);
            self.db.engine.set(marker_key.as_bytes(), TXN_MARKER_ROLLBACK.to_vec()).await?;
        }
        self.resolve_intents(false).await;
        self.db.unregister_transaction(self.snapshot);
        Ok(())
    }

    // New: Mark the transaction to be aborted.
    pub fn mark_abort(&mut self) {
        self.should_abort = true;
    }

}
