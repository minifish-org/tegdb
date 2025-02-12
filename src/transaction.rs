use crate::engine::Engine;
use std::io::Error;
use std::time::{SystemTime, UNIX_EPOCH};

const DELETED_MARKER: &[u8] = b"__deleted__";

enum Operation {
    Insert(Vec<u8>, Vec<u8>),
    Update(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

pub struct Transaction {
    engine: Engine,
    ops: Vec<Operation>,
    // Snapshot timestamp in milliseconds used for MVCC.
    snapshot: u128,
}

impl Transaction {
    /// Begins a new transaction from the given Engine.
    pub fn begin(engine: Engine) -> Self {
        let snapshot = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        Self {
            engine,
            ops: Vec::new(),
            snapshot,
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
        // Check pending operations.
        for op in self.ops.iter().rev() {
            match op {
                Operation::Insert(k, v) | Operation::Update(k, v) if k == key && *v == value => {
                    return Ok(())
                }
                _ => {}
            }
        }
        // Use self.select to check for an existing record.
        if let Some(existing) = self.select(key).await {
            if existing == value {
                return Ok(());
            }
        }
        self.ops.push(Operation::Insert(key.to_vec(), value));
        Ok(())
    }

    /// Buffers an update operation by first fetching the latest data.
    /// If no data exists, the update is ignored.
    pub async fn update(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        if self.select(key).await.is_some() {
            self.ops.push(Operation::Update(key.to_vec(), value));
        }
        Ok(())
    }

    /// Buffers a delete operation.
    /// If the key doesn't exist (no record in the snapshot), the delete is ignored.
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        if self.select(key).await.is_some() {
            self.ops.push(Operation::Delete(key.to_vec()));
        }
        Ok(())
    }

    /// Returns the lexicographically next key after the given key.
    fn next_key(key: &[u8]) -> Vec<u8> {
        let mut next = key.to_vec();
        if let Some(last) = next.last_mut() {
            *last = last.saturating_add(1);
        } else {
            next.push(0);
        }
        next
    }

    /// Reads a value by checking buffered operations first,
    /// then performing a reverse scan over the range from key+":0" to key+":"+snapshot.
    /// Returns the first found record.
    pub async fn select(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Check pending operations (last op wins).
        for op in self.ops.iter().rev() {
            match op {
                Operation::Insert(k, v) | Operation::Update(k, v) if k == key => return Some(v.clone()),
                Operation::Delete(k) if k == key => return None,
                _ => {}
            }
        }
        // Build range: lower bound "key:0", upper bound "key:snapshot"
        let mut lower = key.to_vec();
        lower.extend_from_slice(b":0");

        let mut upper = key.to_vec();
        upper.extend_from_slice(b":");
        upper.extend_from_slice(self.snapshot.to_string().as_bytes());

        // Perform a reverse scan using the Engine API and return the first record.
        let mut rev_iter = self.engine.reverse_scan(lower..upper).await.ok()?;
        rev_iter.next().and_then(|(_, v)| {
            if v == DELETED_MARKER { None } else { Some(v) }
        })
    }

    /// Commits the buffered operations and writes a commit marker.
    pub async fn commit(mut self) -> Result<(), Error> {
        for op in self.ops.drain(..) {
            match op {
                Operation::Insert(key, value) => {
                    let mut mod_key = key;
                    mod_key.extend_from_slice(b":");
                    mod_key.extend_from_slice(self.snapshot.to_string().as_bytes());
                    self.engine.set(&mod_key, value).await?;
                }
                Operation::Update(key, value) => {
                    let mut mod_key = key;
                    mod_key.extend_from_slice(b":");
                    mod_key.extend_from_slice(self.snapshot.to_string().as_bytes());
                    self.engine.set(&mod_key, value).await?;
                }
                Operation::Delete(key) => {
                    let mut mod_key = key;
                    mod_key.extend_from_slice(b":");
                    mod_key.extend_from_slice(self.snapshot.to_string().as_bytes());
                    self.engine.set(&mod_key, DELETED_MARKER.to_vec()).await?;
                }
            }
        }
        // Append the transaction snapshot to the txn_marker.
        let commit_marker = format!("txn:{}:commit", self.snapshot);
        self.engine.set(b"__txn_marker__", commit_marker.as_bytes().to_vec()).await?;
        Ok(())
    }

    /// Asynchronously rolls back the transaction by writing a rollback marker and clearing buffered operations.
    pub async fn rollback(&mut self) -> Result<(), Error> {
        let rollback_marker = format!("txn:{}:rollback", self.snapshot);
        self.engine.set(b"__txn_marker__", rollback_marker.as_bytes().to_vec()).await?;
        self.ops.clear();
        Ok(())
    }
}
