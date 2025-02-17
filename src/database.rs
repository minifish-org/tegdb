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
    // Global counters for committed modifications.
    pub total_new: AtomicUsize,
    pub total_old: AtomicUsize,
    // Notify GC thread when thresholds are met.
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
    /// Creates a new TransactionManager with empty state.
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
    
    /// Registers a transaction by inserting its snapshot.
    pub fn register_transaction(&self, snapshot: Snapshot) {
        self.active_transactions.insert(snapshot);
        self.oldest_read_snapshot.insert(snapshot);
    }
    
    /// Unregisters a transaction.
    pub fn unregister_transaction(&self, snapshot: Snapshot) {
        self.active_transactions.remove(&snapshot);
        self.oldest_read_snapshot.remove(&snapshot);
    }
    
    /// Returns the oldest active transaction snapshot or MAX if none.
    pub fn get_oldest_read_snapshot(&self) -> Snapshot {
        self.oldest_read_snapshot.iter()
            .next()
            .map(|entry| *entry.value())
            .unwrap_or(Snapshot::MAX)
    }
    
    /// Starts the GC thread using a dedicated runtime.
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
    
    /// Runs garbage collection based on the oldest snapshot using a single scan.
    pub async fn garbage_collect(&self, engine: &Engine) -> Result<(), Error> {
        let oldest_read_snapshot = self.get_oldest_read_snapshot();
        println!("GC: Starting cycle. Oldest read snapshot: {}", oldest_read_snapshot);
        const DELETED_MARKER: &[u8] = b"__deleted__";
        
        let mut current_key: Option<Vec<u8>> = None;
        
        let mut iter = engine.reverse_scan(vec![0]..vec![255]).await?;
        while let Some((key, value)) = iter.next() {
            if let Some(pos) = key.iter().rposition(|&b| b == b':') {
                let logical_key = key[..pos].to_vec();
                if let Ok(snap_str) = std::str::from_utf8(&key[pos + 1..]) {
                    if let Ok(snapshot) = snap_str.parse::<Snapshot>() {
                        if snapshot < oldest_read_snapshot {
                            match &current_key {
                                Some(current) if *current == logical_key => {
                                    // Not the first version, can be deleted
                                    engine.del(&key).await?;
                                }
                                _ => {
                                    // First version of a new key
                                    current_key = Some(logical_key);
                                    
                                    // Check transaction marker
                                    let txn_marker_key = format!("{}{}", crate::constants::TXN_MARKER_PREFIX, snapshot);
                                    match engine.get(txn_marker_key.as_bytes()).await {
                                        Some(marker) if marker == b"commit" => {
                                            // If committed but marked as deleted, remove it
                                            if value.windows(DELETED_MARKER.len()).any(|w| w == DELETED_MARKER) {
                                                engine.del(&key).await?;
                                            }
                                        }
                                        _ => {
                                            // Transaction not committed, delete the key
                                            engine.del(&key).await?;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    
    /// Pushes counters and notifies GC if the removal ratio threshold is reached.
    pub fn push_counters(&self, new: usize, old: usize) {
        self.total_new.fetch_add(new, Ordering::Relaxed);
        self.total_old.fetch_add(old, Ordering::Relaxed);
        let tn = self.total_new.load(Ordering::Relaxed);
        let to = self.total_old.load(Ordering::Relaxed);
        if tn > 0 && (to as f64) / (tn as f64) >= 0.3 {
            self.gc_notify.notify_one();
        }
    }

    /// Signals the GC thread to stop.
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
    /// Initializes a new Database with an Engine and starts GC.
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

    /// Exposes push_counters to update GC metrics.
    pub fn push_counters(&self, new: usize, old: usize) {
        self.transaction_manager.push_counters(new, old);
    }

    /// Stops the GC thread.
    pub fn stop_gc(&self) {
        self.transaction_manager.stop_gc();
    }

    /// Delegates transaction registration.
    pub fn register_transaction(&self, snapshot: Snapshot) {
        self.transaction_manager.register_transaction(snapshot);
    }

    /// Delegates transaction unregistration.
    pub fn unregister_transaction(&self, snapshot: Snapshot) {
        self.transaction_manager.unregister_transaction(snapshot);
    }

    /// Returns the current oldest snapshot.
    pub fn get_oldest_read_snapshot(&self) -> Snapshot {
        self.transaction_manager.get_oldest_read_snapshot()
    }

    /// Begins a new transaction.
    pub async fn new_transaction(&self) -> Transaction {
        Transaction::begin(self.clone()).await
    }

    /// Initiates shutdown by stopping GC and persisting snapshots.
    pub fn shutdown(&self) {
        self.transaction_manager.stop_gc();
        // Persist snapshot asynchronously on shutdown.
        let engine = self.engine.clone();
        tokio::spawn(async move {
            crate::snapshot::persist_snapshot(&engine).await;
        });
    }
}
