use crate::engine::Engine;
use crossbeam_skiplist::SkipSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::io::Error;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

#[derive(Clone)]
pub struct Database {
    pub engine: Engine,
    // record active transactions as snapshots
    active_transactions: Arc<SkipSet<u128>>,
    // changed: tracks oldest snapshots using a SkipSet for concurrent min lookup
    oldest_read_snapshot: Arc<SkipSet<u128>>,
}

impl Database {
    pub fn new(path: PathBuf) -> Self {
        let db = Self {
            engine: Engine::new(path),
            active_transactions: Arc::new(SkipSet::new()),
            oldest_read_snapshot: Arc::new(SkipSet::new()),
        };
        db.start_gc(); // start GC background task.
        db
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

    // New: Spawns a background task for garbage collection.
    fn start_gc(&self) {
        let db_clone = self.clone();
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                loop {
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
        // Use oldest_read_snapshot field.
        let oldest_active = self.get_oldest_read_snapshot();
        const DELETED_MARKER: &[u8] = b"__deleted__";

        let lower_bound = vec![0];
        let upper_bound = vec![255];
        let mut iter = self.engine.reverse_scan(lower_bound.clone()..upper_bound.clone()).await?;
        let mut versions: HashMap<Vec<u8>, (u128, Vec<u8>)> = HashMap::new();
        while let Some((key, value)) = iter.next() {
            if let Some(pos) = key.iter().rposition(|&b| b == b':') {
                let logical_key = key[..pos].to_vec();
                if let Ok(snap_str) = std::str::from_utf8(&key[pos+1..]) {
                    if let Ok(snapshot) = snap_str.parse::<u128>() {
                        if snapshot < oldest_active {
                            versions.entry(logical_key.clone()).and_modify(|(existing, _)| {
                                if snapshot > *existing { *existing = snapshot; }
                            }).or_insert((snapshot, value.clone()));
                        }
                    }
                }
            }
        }
        let mut keys_to_delete = Vec::new();
        for (logical_key, (max_snapshot, _)) in versions.iter() {
            let mut lb = logical_key.clone();
            lb.extend_from_slice(b":0");
            let mut ub = logical_key.clone();
            ub.extend_from_slice(b":");
            ub.extend_from_slice(oldest_active.to_string().as_bytes());
            let mut check_iter = self.engine.reverse_scan(lb..ub).await?;
            if let Some((_found_key, found_val)) = check_iter.next() {
                if found_val == DELETED_MARKER {
                    let mut version_iter = self.engine.reverse_scan(
                        {
                            let mut l = logical_key.clone();
                            l.extend_from_slice(b":0");
                            l
                        }..
                        {
                            let mut u = logical_key.clone();
                            u.extend_from_slice(b":");
                            u.extend_from_slice(oldest_active.to_string().as_bytes());
                            u
                        }
                    ).await?;
                    while let Some((k, _)) = version_iter.next() {
                        keys_to_delete.push(k);
                    }
                }
            }
        }
        for k in keys_to_delete {
            self.engine.del(&k).await?;
        }
        Ok(())
    }
}
