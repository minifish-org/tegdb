use crate::database::Database;
use std::io::Error;
use std::time::{SystemTime, UNIX_EPOCH};

const DELETED_MARKER: &[u8] = b"__deleted__";

pub struct Transaction {
    db: Database,
    // Snapshot timestamp in milliseconds used for MVCC.
    snapshot: u128,
    read_snapshot: u128,
    ops: Vec<Vec<u8>>,
}

impl Transaction {
    /// Begins a new transaction from the given Database.
    pub fn begin(db: Database) -> Self {
        let snapshot = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        db.register_transaction(snapshot);
        let oldest = db.active_transactions().get(0).cloned().unwrap_or(snapshot);
        Self {
            db,
            snapshot,
            read_snapshot: oldest,
            ops: Vec::new(),
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
        }
        Ok(())
    }

    /// Reads a value by checking buffered operations first,
    /// then performing a reverse scan over the range from key+":0" to key+":"+snapshot+1.
    /// Returns the first found record.
    pub async fn select(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Build range: lower bound "key:0", upper bound "key:snapshot+1"
        let mut lower = key.to_vec();
        lower.extend_from_slice(b":0");

        let mut upper = key.to_vec();
        upper.extend_from_slice(b":");
        // Use read_snapshot + 1
        let upper_bound = self.read_snapshot + 1;
        upper.extend_from_slice(upper_bound.to_string().as_bytes());

        // Perform a reverse scan using the Engine API and return the first record.
        let mut rev_iter = self.db.engine.reverse_scan(lower..upper).await.ok()?;
        rev_iter.next().and_then(|(_, v)| {
            if v == DELETED_MARKER { None } else { Some(v) }
        })
    }

    /// Commits the buffered operations and writes a commit marker.
    pub async fn commit(self) -> Result<(), Error> {
        let commit_marker = format!("txn:{}:commit", self.snapshot);
        self.db.engine.set(b"__txn_marker__", commit_marker.as_bytes().to_vec()).await?;
        self.db.unregister_transaction(self.snapshot);
        Ok(())
    }

    /// Asynchronously rolls back the transaction by writing a rollback marker and clearing buffered operations.
    pub async fn rollback(&mut self) -> Result<(), Error> {
        for mk in &self.ops {
            self.db.engine.del(mk).await?;
        }
        let rollback_marker = format!("txn:{}:rollback", self.snapshot);
        self.db.engine.set(b"__txn_marker__", rollback_marker.as_bytes().to_vec()).await?;
        self.db.unregister_transaction(self.snapshot);
        Ok(())
    }
}
