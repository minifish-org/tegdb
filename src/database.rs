use crate::engine::Engine;
use crate::transaction::Transaction;
use crossbeam_skiplist::SkipSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::io::Error;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
pub struct Database {
    pub engine: Engine,
    // record active transactions as snapshots
    active_transactions: Arc<SkipSet<u128>>,
    // changed: tracks oldest snapshots using a SkipSet for concurrent min lookup
    oldest_read_snapshot: Arc<SkipSet<u128>>,
    // New: flag to signal GC thread to stop.
    stop_gc: Arc<AtomicBool>,
}

impl Database {
    pub fn new(path: PathBuf) -> Self {
        let db = Self {
            engine: Engine::new(path),
            active_transactions: Arc::new(SkipSet::new()),
            oldest_read_snapshot: Arc::new(SkipSet::new()),
            stop_gc: Arc::new(AtomicBool::new(false)), // initialize flag
        };
        db.start_gc(); // start GC background task.
        db
    }

    // New: Public method to stop GC thread.
    pub fn stop_gc(&self) {
        self.stop_gc.store(true, Ordering::Relaxed);
    }

    // Update: register transaction by inserting snapshot to both skip sets.
    pub fn register_transaction(&self, snapshot: u128) {
        self.active_transactions.insert(snapshot);
        self.oldest_read_snapshot.insert(snapshot);
    }

    // Update: unregister transaction and remove snapshot from the SkipSet.
    pub fn unregister_transaction(&self, snapshot: u128) {
        self.active_transactions.remove(&snapshot);
        self.oldest_read_snapshot.remove(&snapshot);
    }

    // Update: returns the current oldest read snapshot from the SkipSet.
    pub fn get_oldest_read_snapshot(&self) -> u128 {
        self.oldest_read_snapshot.iter()
            .next()
            .map(|entry| *entry.value())
            .unwrap_or(u128::MAX)
    }

    // Update: Spawns a background task for garbage collection.
    fn start_gc(&self) {
        let db_clone = self.clone();
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                loop {
                    // Check if stop flag is set.
                    if db_clone.stop_gc.load(Ordering::Relaxed) {
                        break;
                    }
                    if let Err(e) = db_clone.garbage_collect().await {
                        eprintln!("GC error: {:?}", e);
                    }
                    sleep(Duration::from_secs(60)).await;
                }
            });
        });
    }

    // New: Garbage collection implementation.
    pub async fn garbage_collect(&self) -> Result<(), Error> {
        let oldest_read_snapshot = self.get_oldest_read_snapshot();
        const DELETED_MARKER: &[u8] = b"__deleted__";

        let lower_bound = vec![0];
        let upper_bound = vec![255];
        let mut iter = self.engine.reverse_scan(lower_bound.clone()..upper_bound.clone()).await?;
        // Modified: versions now stores (snapshot, bool) flag for deleted marker.
        let mut versions: HashMap<Vec<u8>, (u128, bool)> = HashMap::new();
        while let Some((key, value)) = iter.next() {
            if let Some(pos) = key.iter().rposition(|&b| b == b':') {
                let logical_key = key[..pos].to_vec();
                if let Ok(snap_str) = std::str::from_utf8(&key[pos+1..]) {
                    if let Ok(snapshot) = snap_str.parse::<u128>() {
                        if snapshot < oldest_read_snapshot {
                            // New: determine if the value contains the deleted marker.
                            let deleted_flag = value.windows(DELETED_MARKER.len()).any(|w| w == DELETED_MARKER);
                            versions.entry(logical_key.clone()).and_modify(|(existing_snapshot, flag)| {
                                if snapshot > *existing_snapshot { *existing_snapshot = snapshot; *flag = deleted_flag; }
                            }).or_insert((snapshot, deleted_flag));
                        }
                    }
                }
            }
        }
        let mut keys_to_delete = Vec::new();
        for (logical_key, (_max_snapshot, deleted_flag)) in versions.iter() {
            let mut lb = logical_key.clone();
            lb.extend_from_slice(b":0");
            let mut ub = logical_key.clone();
            ub.extend_from_slice(b":");
            ub.extend_from_slice(oldest_read_snapshot.to_string().as_bytes());
            let mut version_iter = self.engine.reverse_scan(lb..ub).await?;
            if *deleted_flag {
                // Delete all versions for keys with deleted marker.
                while let Some((k, _)) = version_iter.next() {
                    keys_to_delete.push(k);
                }
            } else {
                // Keep the latest version, delete the rest.
                let mut first = true;
                while let Some((k, _)) = version_iter.next() {
                    if first {
                        first = false; // skip latest version.
                        continue;
                    }
                    keys_to_delete.push(k);
                }
            }
        }
        for k in keys_to_delete {
            self.engine.del(&k).await?;
        }
        Ok(())
    }

    // Updated: API to begin a new transaction using Transaction::begin from transaction.rs.
    pub fn new_transaction(&self) -> crate::transaction::Transaction {
        Transaction::begin(self.clone())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.stop_gc();
    }
}
