use crate::engine::Engine;
use crate::transaction::Transaction;
use crossbeam_skiplist::SkipSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::io::Error;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;
use crate::types::Snapshot;

// Updated: TransactionManager now holds transaction fields and GC logic.
pub struct TransactionManager {
    pub active_transactions: Arc<SkipSet<Snapshot>>,
    pub oldest_read_snapshot: Arc<SkipSet<Snapshot>>,
    pub stop_gc: Arc<AtomicBool>,
    // New: Global counters for committed changes.
    pub total_new: AtomicUsize,
    pub total_old: AtomicUsize,
    // New: Notification for waking the GC thread.
    pub gc_notify: Arc<Notify>,
}

impl Clone for TransactionManager {
    fn clone(&self) -> Self {
        Self {
            active_transactions: self.active_transactions.clone(),
            oldest_read_snapshot: self.oldest_read_snapshot.clone(),
            stop_gc: self.stop_gc.clone(),
            total_new: AtomicUsize::new(self.total_new.load(Ordering::Relaxed)),
            total_old: AtomicUsize::new(self.total_old.load(Ordering::Relaxed)),
            gc_notify: self.gc_notify.clone(),
        }
    }
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(SkipSet::new()),
            oldest_read_snapshot: Arc::new(SkipSet::new()),
            stop_gc: Arc::new(AtomicBool::new(false)),
            total_new: AtomicUsize::new(0),
            total_old: AtomicUsize::new(0),
            gc_notify: Arc::new(Notify::new()),
        }
    }
    
    // Update register_transaction/unregister_transaction to use Snapshot.
    pub fn register_transaction(&self, snapshot: Snapshot) {
        self.active_transactions.insert(snapshot);
        self.oldest_read_snapshot.insert(snapshot);
    }
    
    pub fn unregister_transaction(&self, snapshot: Snapshot) {
        self.active_transactions.remove(&snapshot);
        self.oldest_read_snapshot.remove(&snapshot);
    }
    
    pub fn get_oldest_read_snapshot(&self) -> Snapshot {
        self.oldest_read_snapshot.iter()
            .next()
            .map(|entry| *entry.value())
            .unwrap_or(Snapshot::MAX)
    }
    
    // Modified start_gc: Wait by default on Notify, then run GC when signaled.
    pub fn start_gc(&self, engine: Engine) {
        let tm = self.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            // Make engine mutable for compaction.
            let mut engine = engine;
            runtime.block_on(async move {
                loop {
                    // Wait indefinitely until notified or check stop flag.
                    tm.gc_notify.notified().await;
                    if tm.stop_gc.load(Ordering::Relaxed) { break; }
                    println!("GC thread awakened by notify");
                    // Persist the snapshot key once per GC cycle.
                    crate::snapshot::persist_snapshot(&engine).await;
                    if let Err(e) = tm.garbage_collect(&engine).await {
                        eprintln!("GC error: {:?}", e);
                    } else {
                        // Trigger compaction at the end of the GC cycle.
                        if let Err(e) = engine.compact() {
                            eprintln!("Compaction error: {:?}", e);
                        }
                    }
                    // Reset counters after GC: total_new = total_new - total_old, total_old = 0.
                    let tn = tm.total_new.load(Ordering::Relaxed);
                    let to = tm.total_old.load(Ordering::Relaxed);
                    tm.total_new.store(tn.saturating_sub(to), Ordering::Relaxed);
                    tm.total_old.store(0, Ordering::Relaxed);
                }
            });
        });
    }
    
    // Revert garbage_collect to its previous version.
    pub async fn garbage_collect(&self, engine: &Engine) -> Result<(), Error> {
        let oldest_read_snapshot = self.get_oldest_read_snapshot();
        println!("GC: Starting cycle. Oldest read snapshot: {}", oldest_read_snapshot);
        const DELETED_MARKER: &[u8] = b"__deleted__";
        let lower_bound = vec![0];
        let upper_bound = vec![255];
        println!("GC: Scanning keys between {:?} and {:?}", lower_bound, upper_bound);
        
        let mut iter = engine.reverse_scan(lower_bound.clone()..upper_bound.clone()).await?;
        let mut versions: HashMap<Vec<u8>, (Snapshot, bool)> = HashMap::new();
        while let Some((key, value)) = iter.next() {
            if let Some(pos) = key.iter().rposition(|&b| b == b':') {
                let logical_key = key[..pos].to_vec();
                if let Ok(snap_str) = std::str::from_utf8(&key[pos + 1..]) {
                    if let Ok(snapshot) = snap_str.parse::<Snapshot>() {
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
        
        for (logical_key, (_max_snapshot, deleted_flag)) in versions.iter() {
            let mut lb = logical_key.clone();
            lb.extend_from_slice(b":0");
            let mut ub = logical_key.clone();
            ub.extend_from_slice(b":");
            ub.extend_from_slice(oldest_read_snapshot.to_string().as_bytes());
            let mut version_iter = engine.reverse_scan(lb..ub).await?;
            if *deleted_flag {
                while let Some((k, _)) = version_iter.next() {
                    engine.del(&k).await?;
                }
            } else {
                let mut first = true;
                while let Some((k, _)) = version_iter.next() {
                    if first { first = false; continue; }
                    engine.del(&k).await?;
                }
            }
        }
        Ok(())
    }
    
    // New: Push counters and notify GC thread if threshold reached.
    pub fn push_counters(&self, new: usize, old: usize) {
        self.total_new.fetch_add(new, Ordering::Relaxed);
        self.total_old.fetch_add(old, Ordering::Relaxed);
        let tn = self.total_new.load(Ordering::Relaxed);
        let to = self.total_old.load(Ordering::Relaxed);
        if tn > 0 && (to as f64) / (tn as f64) >= 0.3 {
            self.gc_notify.notify_one();
        }
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
    pub async fn new(path: PathBuf) -> Self {
        let engine = Engine::new(path);
        // Directly await snapshot init.
        crate::snapshot::init_snapshot(&engine).await;
        let tm = TransactionManager::new();
        tm.start_gc(engine.clone());
        Self {
            engine,
            transaction_manager: tm,
        }
    }

    // New: Expose push_counters.
    pub fn push_counters(&self, new: usize, old: usize) {
        self.transaction_manager.push_counters(new, old);
    }

    // New: Public method to stop GC thread.
    pub fn stop_gc(&self) {
        self.transaction_manager.stop_gc();
    }

    // Updated: Delegate transaction registration.
    pub fn register_transaction(&self, snapshot: Snapshot) {
        self.transaction_manager.register_transaction(snapshot);
    }

    // Updated: Delegate transaction unregistration.
    pub fn unregister_transaction(&self, snapshot: Snapshot) {
        self.transaction_manager.unregister_transaction(snapshot);
    }

    // Updated: Delegate to TransactionManager.
    pub fn get_oldest_read_snapshot(&self) -> Snapshot {
        self.transaction_manager.get_oldest_read_snapshot()
    }

    // Updated: API to begin a new transaction.
    pub async fn new_transaction(&self) -> Transaction {
        Transaction::begin(self.clone()).await
    }

    // Updated: Shutdown GC and persist the snapshot key on shutdown.
    pub fn shutdown(&self) {
        self.transaction_manager.stop_gc();
        // Persist snapshot asynchronously on shutdown.
        let engine = self.engine.clone();
        tokio::spawn(async move {
            crate::snapshot::persist_snapshot(&engine).await;
        });
    }
}
