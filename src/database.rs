use crate::engine::Engine;
use crate::transaction::Transaction;
use crossbeam_skiplist::SkipSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::io::Error;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use std::sync::atomic::{AtomicBool, Ordering};

// Updated: TransactionManager now holds transaction fields and GC logic.
#[derive(Clone)]
pub struct TransactionManager {
    pub active_transactions: Arc<SkipSet<u128>>,
    pub oldest_read_snapshot: Arc<SkipSet<u128>>,
    pub stop_gc: Arc<AtomicBool>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(SkipSet::new()),
            oldest_read_snapshot: Arc::new(SkipSet::new()),
            stop_gc: Arc::new(AtomicBool::new(false)),
        }
    }
    
    pub fn register_transaction(&self, snapshot: u128) {
        self.active_transactions.insert(snapshot);
        self.oldest_read_snapshot.insert(snapshot);
    }
    
    pub fn unregister_transaction(&self, snapshot: u128) {
        self.active_transactions.remove(&snapshot);
        self.oldest_read_snapshot.remove(&snapshot);
    }
    
    pub fn get_oldest_read_snapshot(&self) -> u128 {
        self.oldest_read_snapshot.iter()
            .next()
            .map(|entry| *entry.value())
            .unwrap_or(u128::MAX)
    }
    
    // New: Starts GC loop using the provided engine.
    pub fn start_gc(&self, engine: Engine) {
        let tm = self.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                loop {
                    println!("In GC loop, stop flag: {}", tm.stop_gc.load(Ordering::Relaxed));
                    if tm.stop_gc.load(Ordering::Relaxed) {
                        break;
                    }
                    if let Err(e) = tm.garbage_collect(&engine).await {
                        eprintln!("GC error: {:?}", e);
                    }
                    println!("Start GC sleep");
                    sleep(Duration::from_secs(60)).await;
                    println!("End GC sleep");
                }
            });
        });
    }
    
    // New: Garbage collection implementation.
    pub async fn garbage_collect(&self, engine: &Engine) -> Result<(), Error> {
        let oldest_read_snapshot = self.get_oldest_read_snapshot();
        println!("GC: Starting cycle. Oldest read snapshot: {}", oldest_read_snapshot);
        const DELETED_MARKER: &[u8] = b"__deleted__";
        let lower_bound = vec![0];
        let upper_bound = vec![255];
        println!("GC: Scanning keys between {:?} and {:?}", lower_bound, upper_bound);
        let mut iter = engine.reverse_scan(lower_bound.clone()..upper_bound.clone()).await?;
        let mut versions: HashMap<Vec<u8>, (u128, bool)> = HashMap::new();
        while let Some((key, value)) = iter.next() {
            if let Some(pos) = key.iter().rposition(|&b| b == b':') {
                let logical_key = key[..pos].to_vec();
                if let Ok(snap_str) = std::str::from_utf8(&key[pos+1..]) {
                    if let Ok(snapshot) = snap_str.parse::<u128>() {
                        if snapshot < oldest_read_snapshot {
                            let deleted_flag = value.windows(DELETED_MARKER.len()).any(|w| w == DELETED_MARKER);
                            versions.entry(logical_key.clone()).and_modify(|(existing_snapshot, flag)| {
                                if snapshot > *existing_snapshot {
                                    *existing_snapshot = snapshot;
                                    *flag = deleted_flag;
                                }
                            }).or_insert((snapshot, deleted_flag));
                        }
                    }
                }
            }
        }
        println!("GC: Collected {} keys for deletion", versions.len());
        let mut keys_to_delete = Vec::new();
        for (logical_key, (_max_snapshot, deleted_flag)) in versions.iter() {
            println!("GC: Processing key {:?} deleted_flag: {}", logical_key, deleted_flag);
            let mut lb = logical_key.clone();
            lb.extend_from_slice(b":0");
            let mut ub = logical_key.clone();
            ub.extend_from_slice(b":");
            ub.extend_from_slice(oldest_read_snapshot.to_string().as_bytes());
            let mut version_iter = engine.reverse_scan(lb..ub).await?;
            if *deleted_flag {
                while let Some((k, _)) = version_iter.next() {
                    keys_to_delete.push(k);
                }
            } else {
                let mut first = true;
                while let Some((k, _)) = version_iter.next() {
                    if first { first = false; continue; }
                    keys_to_delete.push(k);
                }
            }
        }
        println!("GC: Total keys to delete: {}", keys_to_delete.len());
        for k in keys_to_delete {
            println!("GC: Deleting key: {:?}", String::from_utf8_lossy(&k));
            engine.del(&k).await?;
        }
        Ok(())
    }
    
    // New: Stops the GC loop.
    pub fn stop_gc(&self) {
        self.stop_gc.store(true, Ordering::Relaxed);
    }
}

// Updated: Database no longer holds engine separately; GC is managed in TransactionManager.
#[derive(Clone)]
pub struct Database {
    pub engine: Engine,
    // Combined transaction fields and GC logic in TransactionManager.
    transaction_manager: TransactionManager,
}

impl Database {
    pub fn new(path: PathBuf) -> Self {
        let engine = Engine::new(path);
        let tm = TransactionManager::new();
        // Start GC using engine clone.
        tm.start_gc(engine.clone());
        Self {
            engine,
            transaction_manager: tm,
        }
    }

    // New: Public method to stop GC thread.
    pub fn stop_gc(&self) {
        self.transaction_manager.stop_gc();
    }

    // Updated: Delegate transaction registration.
    pub fn register_transaction(&self, snapshot: u128) {
        self.transaction_manager.register_transaction(snapshot);
    }

    // Updated: Delegate transaction unregistration.
    pub fn unregister_transaction(&self, snapshot: u128) {
        self.transaction_manager.unregister_transaction(snapshot);
    }

    // Updated: Delegate to TransactionManager.
    pub fn get_oldest_read_snapshot(&self) -> u128 {
        self.transaction_manager.get_oldest_read_snapshot()
    }

    // Updated: API to begin a new transaction.
    pub fn new_transaction(&self) -> crate::transaction::Transaction {
        Transaction::begin(self.clone())
    }

    // Updated: Shutdown GC via TransactionManager.
    pub fn shutdown(&self) {
        self.transaction_manager.stop_gc();
    }
}
